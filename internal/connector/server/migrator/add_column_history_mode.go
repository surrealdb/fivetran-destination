package migrator

import (
	"context"
	"fmt"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// AddColumnInHistoryMode adds a column to history-mode tables while preserving historical record integrity.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#add_column_in_history_mode
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Validate table is non-empty and max(_fivetran_start) < operation_timestamp
// 2. Execute: ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type>
// 3. INSERT new history rows with default value and operation_timestamp as _fivetran_start
// 4. UPDATE newly created rows to set column value
// 5. UPDATE previous active records: set _fivetran_end = operation_timestamp - 1ms, _fivetran_active = FALSE
//
// This maintains temporal integrity through timestamp-based state transitions.
func (m *Migrator) AddColumnInHistoryMode(ctx context.Context, schema, table string, column *pb.Column, defaultValue string, operationTimestamp time.Time) error {
	// 1. Get existing field count for column index
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	columnIndex := 0
	if infoResults != nil && len(*infoResults) > 0 {
		columnIndex = len((*infoResults)[0].Result.Fields)
	}

	// 2. Validate table has active records and get max(_fivetran_start)
	type MaxStartResult struct {
		Max *models.CustomDateTime `cbor:"max"`
	}
	maxQuery := fmt.Sprintf("SELECT time::max(_fivetran_start) AS max FROM %s WHERE _fivetran_active = true GROUP ALL", table)
	maxResults, err := surrealdb.Query[[]MaxStartResult](ctx, m.db, maxQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to get max _fivetran_start from table %s: %w", table, err)
	}

	// Check if we have active records
	hasActiveRecords := false
	if maxResults != nil && len(*maxResults) > 0 && len((*maxResults)[0].Result) > 0 {
		maxPtr := (*maxResults)[0].Result[0].Max
		if maxPtr != nil {
			hasActiveRecords = true
			// Validate max(_fivetran_start) < operation_timestamp
			if !maxPtr.Before(operationTimestamp) {
				return fmt.Errorf("operation_timestamp %v must be after max(_fivetran_start) %v", operationTimestamp, maxPtr.Time)
			}
		}
	}

	// 3. Add the new field using tablemapper
	defineFieldQuery, err := tablemapper.DefineFieldQueryFromFt(table, column, columnIndex)
	if err != nil {
		return fmt.Errorf("failed to generate field definition: %w", err)
	}
	_, err = surrealdb.Query[any](ctx, m.db, defineFieldQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to add column %s to table %s: %w", column.Name, table, err)
	}

	// If no active records, just add the column
	if !hasActiveRecords {
		m.LogInfo("Added column in history mode (no active records)",
			"table", table,
			"column", column.Name,
		)
		return nil
	}

	// 4. For active records: create new history row with the new column value
	// Calculate operation_timestamp - 1ms for the previous record's end time
	endTimePrev := operationTimestamp.Add(-time.Millisecond)

	// Get the type mapping for converting the default value
	typeMapping := tablemapper.FindTypeMappingByPbColumn(column)
	if typeMapping == nil {
		return fmt.Errorf("unsupported data type for column %s: %v", column.Name, column.Type)
	}

	// Convert default value to proper SurrealDB type
	defaultVal, err := typeMapping.SurrealType(defaultValue)
	if err != nil {
		return fmt.Errorf("failed to convert default value %q for column %s: %w", defaultValue, column.Name, err)
	}

	// Insert new rows for currently active records with the new column set
	// Generate new ID by replacing _fivetran_start in the composite key (pk1, pk2, ..., _fivetran_start)
	// Aliased fields before * override fields from *; _fivetran_active comes from * (WHERE clause ensures true)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s SELECT
			array::add(array::slice(record::id(id), 0, array::len(record::id(id)) - 1), $operation_timestamp) AS id,
			$default_value AS %s,
			$operation_timestamp AS _fivetran_start,
			d'9999-12-31T23:59:59Z' AS _fivetran_end,
			*
		FROM %s WHERE _fivetran_active = true
	`, table, column.Name, table)
	_, err = surrealdb.Query[any](ctx, m.db, insertQuery, map[string]any{
		"default_value":       defaultVal,
		"operation_timestamp": operationTimestamp,
	})
	if err != nil {
		return fmt.Errorf("failed to insert new history rows: %w", err)
	}

	// 5. Deactivate original active records (those without the new column set)
	updateQuery := fmt.Sprintf(`
		UPDATE %s SET
			_fivetran_end = $end_time_prev,
			_fivetran_active = false
		WHERE _fivetran_active = true AND %s IS NONE
	`, table, column.Name)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
		"end_time_prev": models.CustomDateTime{Time: endTimePrev},
	})
	if err != nil {
		return fmt.Errorf("failed to deactivate previous active records: %w", err)
	}

	m.LogInfo("Added column in history mode",
		"table", table,
		"column", column.Name,
		"operation_timestamp", operationTimestamp,
	)

	return nil
}
