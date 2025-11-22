package migrator

import (
	"context"
	"fmt"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// DropColumnInHistoryMode removes a column from history-mode tables while maintaining historical records.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_column_in_history_mode
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Validate non-empty table and max(_fivetran_start) < operation_timestamp
// 2. INSERT new rows with NONE for dropped column where it was previously non-null
// 3. UPDATE prior active records: _fivetran_end = operation_timestamp - 1ms, _fivetran_active = FALSE
//
// This follows a similar pattern to ADD_COLUMN_IN_HISTORY_MODE but sets the column to NONE.
//
// Additionally, we remove the SurrealDB field definition to hide the dropped column from DescribeTable results,
// as Fivetran expects dropped columns to not be returned. All historical data values are preserved in existing records.
// To be precise, we observed the Fivetran sdktester 2.25.1105.001 bailed out if the dropped column was still present
// after this operation using schema_migrations_input_sync_modes.json.
func (m *Migrator) DropColumnInHistoryMode(ctx context.Context, schema, table, column string, operationTimestamp time.Time) error {
	// 1. Validate table has active records and get max(_fivetran_start)
	type MaxStartResult struct {
		Max *models.CustomDateTime `cbor:"max"`
	}
	maxQuery := fmt.Sprintf("SELECT time::max(_fivetran_start) AS max FROM %s WHERE _fivetran_active = true GROUP ALL", table)
	maxResults, err := surrealdb.Query[[]MaxStartResult](ctx, m.db, maxQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to get max _fivetran_start from table %s: %w", table, err)
	}

	// Check if we have active records with the column set
	hasActiveRecordsWithColumn := false
	if maxResults != nil && len(*maxResults) > 0 && len((*maxResults)[0].Result) > 0 {
		maxPtr := (*maxResults)[0].Result[0].Max
		if maxPtr != nil {
			hasActiveRecordsWithColumn = true
			// Validate max(_fivetran_start) < operation_timestamp
			if !maxPtr.Before(operationTimestamp) {
				return fmt.Errorf("operation_timestamp %v must be after max(_fivetran_start) %v", operationTimestamp, maxPtr.Time)
			}
		}
	}

	// If no active records, nothing to do
	if !hasActiveRecordsWithColumn {
		m.LogInfo("Dropped column in history mode (no active records)",
			"table", table,
			"column", column,
		)
		return nil
	}

	// 2. Calculate operation_timestamp - 1ms for the previous record's end time
	endTimePrev := operationTimestamp.Add(-time.Millisecond)

	// 3. Insert new rows for currently active records where column IS NOT NONE
	// Set the column to NONE in the new row
	// Aliased fields before * override fields from *; _fivetran_active comes from * (WHERE clause ensures true)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s SELECT
			array::add(array::slice(record::id(id), 0, array::len(record::id(id)) - 1), $operation_timestamp) AS id,
			NONE AS %s,
			$operation_timestamp AS _fivetran_start,
			d'9999-12-31T23:59:59Z' AS _fivetran_end,
			*
		FROM %s WHERE _fivetran_active = true AND %s IS NOT NONE
	`, table, column, table, column)
	_, err = surrealdb.Query[any](ctx, m.db, insertQuery, map[string]any{
		"operation_timestamp": operationTimestamp,
	})
	if err != nil {
		return fmt.Errorf("failed to insert new history rows: %w", err)
	}

	// 4. Deactivate original active records where column IS NOT NONE
	updateQuery := fmt.Sprintf(`
		UPDATE %s SET
			_fivetran_end = $end_time_prev,
			_fivetran_active = false
		WHERE _fivetran_active = true AND %s IS NOT NONE
	`, table, column)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
		"end_time_prev": models.CustomDateTime{Time: endTimePrev},
	})
	if err != nil {
		return fmt.Errorf("failed to deactivate previous active records: %w", err)
	}

	// 5. Remove the field definition from the table schema
	// This hides the column from DescribeTable results while preserving all historical values
	removeFieldQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", column, table)
	_, err = surrealdb.Query[any](ctx, m.db, removeFieldQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove field definition for column %s: %w", column, err)
	}

	m.LogInfo("Dropped column in history mode",
		"table", table,
		"column", column,
		"operation_timestamp", operationTimestamp,
	)

	return nil
}
