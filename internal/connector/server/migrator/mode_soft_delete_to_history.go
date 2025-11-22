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

	// 2. Add history mode field definitions
	defineFieldsQuery := fmt.Sprintf(`
		DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON %s TYPE option<bool>;
	`, table, table, table)
	_, err = surrealdb.Query[any](ctx, m.db, defineFieldsQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to define history mode fields on table %s: %w", table, err)
	}

	// 3. Remove the soft delete column definition before transforming records
	// This must be done before BatchUpdateIDs to avoid schema validation errors
	removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", softDeletedColumn, table)
	_, err = surrealdb.Query[any](ctx, m.db, removeQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove soft delete column %s from table %s: %w", softDeletedColumn, table, err)
	}

	// 4. Transform IDs and set history mode fields based on soft delete status
	// Only run if there are records to update
	if hasRecordsToUpdate {
		const batchSize = 1000
		// ID transformation: [pk...] -> [pk..., _fivetran_start]
		idExpression := "array::add(record::id(id), _fivetran_start)"

		// Set history mode fields based on soft delete status, omit soft delete column
		// For deleted records: start=0001-01-01, end=0001-01-01, active=false
		// For active records: start=max_synced, end=9999-12-31, active=true
		insertedFields := fmt.Sprintf(`
			IF type::field($soft_deleted_column) THEN d'0001-01-01T00:00:00Z' ELSE $max_fivetran_synced END AS _fivetran_start,
			IF type::field($soft_deleted_column) THEN d'0001-01-01T00:00:00Z' ELSE d'9999-12-31T23:59:59Z' END AS _fivetran_end,
			NOT(type::field($soft_deleted_column)) AS _fivetran_active,
			* OMIT %s
		`, softDeletedColumn)

		additionalVars := map[string]any{
			"soft_deleted_column": softDeletedColumn,
			"max_fivetran_synced": maxFivetranSynced.Time,
		}

		err = m.BatchUpdateIDs(ctx, table, "*", idExpression, insertedFields, batchSize, additionalVars)
		if err != nil {
			return fmt.Errorf("failed to update record IDs and history mode fields: %w", err)
		}
	}

	m.LogInfo("Converted table from soft delete to history mode",
		"table", table,
		"soft_delete_column", softDeletedColumn,
	)

	return nil
}
