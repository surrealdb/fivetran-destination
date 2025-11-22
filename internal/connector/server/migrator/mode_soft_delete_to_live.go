package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// ModeSoftDeleteToLive converts a soft-delete mode table to live mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#soft_delete_to_live
//
// This removes the soft delete column and optionally deletes soft-deleted records.
func (m *Migrator) ModeSoftDeleteToLive(ctx context.Context, schema, table, softDeletedColumn string) error {
	// 1. Delete all soft-deleted records (where softDeletedColumn = true)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = true", table, softDeletedColumn)
	_, err := surrealdb.Query[any](ctx, m.db, deleteQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to delete soft-deleted records from %s: %w", table, err)
	}

	// 2. Remove the soft delete column definition
	// This must be done before unsetting the data to avoid schema validation errors

	// Note that sdktester 2.25.1105.001 somehow does not provide us _fivetran_deleted column on CreateTable
	// even though soft_deleted_column = _fivetran_deleted is specified in schema_migrations_input_sync_modes.json.
	// So we skip this step if the column does not exist.
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	if _, ok := (*infoResults)[0].Result.Fields[softDeletedColumn]; softDeletedColumn == "_fivetran_deleted" && ok {
		removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", softDeletedColumn, table)
		_, err = surrealdb.Query[any](ctx, m.db, removeQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to remove soft delete column %s from table %s: %w", softDeletedColumn, table, err)
		}
	}

	// 3. Update all remaining records to unset the soft delete column data
	// Now that the field is not in the schema, we can safely unset it
	unsetQuery := fmt.Sprintf("UPDATE %s UNSET %s", table, softDeletedColumn)
	_, err = surrealdb.Query[any](ctx, m.db, unsetQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to unset soft delete column %s data: %w", softDeletedColumn, err)
	}

	m.LogInfo("Converted table from soft delete to live mode",
		"table", table,
		"soft_delete_column", softDeletedColumn,
	)

	return nil
}
