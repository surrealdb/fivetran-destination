package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// ModeSoftDeleteToHistory converts a soft-delete mode table to history mode,
// preserving deleted record information.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#soft_delete_to_history
//
// According to the Fivetran Partner SDK documentation, this operation should:
//   - Restructure soft-delete markers into history mode temporal columns
//     (_fivetran_start, _fivetran_end, _fivetran_active) to track record lifecycle changes
//
// This is a complex data transformation maintaining deleted row information
// through temporal tracking.
func (m *Migrator) ModeSoftDeleteToHistory(ctx context.Context, schema, table, softDeletedColumn string) error {
	// 1. Obtain the max _fivetran_synced timestamp BEFORE adding new fields
	// Using a pointer to distinguish between no records (nil) and zero value
	type MaxResult struct {
		Max *models.CustomDateTime `cbor:"max"`
	}
	maxQuery := fmt.Sprintf("SELECT time::max(_fivetran_synced) AS max FROM %s GROUP ALL", table)
	maxResults, err := surrealdb.Query[[]MaxResult](ctx, m.db, maxQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to get max _fivetran_synced from table %s: %w", table, err)
	}

	// Check if we have records with valid _fivetran_synced values
	var maxFivetranSynced models.CustomDateTime
	hasRecordsToUpdate := false
	if maxResults != nil && len(*maxResults) > 0 && len((*maxResults)[0].Result) > 0 {
		maxPtr := (*maxResults)[0].Result[0].Max
		if maxPtr != nil {
			maxFivetranSynced = *maxPtr
			hasRecordsToUpdate = true
		}
	}

	// 2. Add history mode columns
	defineFieldsQuery := fmt.Sprintf(`
		DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON %s TYPE option<bool>;
	`, table, table, table)
	_, err = surrealdb.Query[any](ctx, m.db, defineFieldsQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to define history mode fields on table %s: %w", table, err)
	}

	// 3. For all records, set history mode fields based on soft delete status
	// Only run UPDATE if there are records to update
	if hasRecordsToUpdate {
		// Using parameterized query to prevent SQL injection
		// For deleted records: start=0001-01-01, end=0001-01-01, active=false
		// For active records: start=max_synced, end=9999-12-31, active=true
		updateQuery := `
			UPDATE type::table($tb) SET
				_fivetran_start = IF type::field($soft_deleted_column) THEN d'0001-01-01T00:00:00Z' ELSE $max_fivetran_synced END,
				_fivetran_end = IF type::field($soft_deleted_column) THEN d'0001-01-01T00:00:00Z' ELSE d'9999-12-31T23:59:59Z' END,
				_fivetran_active = not(type::field($soft_deleted_column))
		`
		_, err = surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
			"tb":                  table,
			"soft_deleted_column": softDeletedColumn,
			"max_fivetran_synced": maxFivetranSynced.Time,
		})
		if err != nil {
			return fmt.Errorf("failed to update records with history mode fields in table %s: %w", table, err)
		}
	}

	// 4. Remove the soft delete column definition
	// This must be done before unsetting the data to avoid schema validation errors
	removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", softDeletedColumn, table)
	_, err = surrealdb.Query[any](ctx, m.db, removeQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove soft delete column %s from table %s: %w", softDeletedColumn, table, err)
	}

	// 5. Update all records to unset the soft delete column data
	unsetQuery := fmt.Sprintf("UPDATE %s UNSET %s", table, softDeletedColumn)
	_, err = surrealdb.Query[any](ctx, m.db, unsetQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to unset soft delete column %s data: %w", softDeletedColumn, err)
	}

	m.LogInfo("Converted table from soft delete to history mode",
		"table", table,
		"soft_delete_column", softDeletedColumn,
	)

	return nil
}
