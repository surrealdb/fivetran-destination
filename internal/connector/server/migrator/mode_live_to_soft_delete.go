package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// ModeLiveToSoftDelete converts a live-mode table to soft-delete mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#live_to_soft_delete
//
// This adds a soft delete column to enable tracking of deleted records.
func (m *Migrator) ModeLiveToSoftDelete(ctx context.Context, schema, table, softDeletedColumn string) error {
	// 1. Add soft delete column with option<bool> for compatibility between modes
	defineFieldQuery := fmt.Sprintf("DEFINE FIELD %s ON %s TYPE option<bool>", softDeletedColumn, table)
	_, err := surrealdb.Query[any](ctx, m.db, defineFieldQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to add soft delete column %s to table %s: %w", softDeletedColumn, table, err)
	}

	// 2. Initialize all existing records as not deleted (false)
	// Using parameterized query for the table name to prevent SQL injection
	// Note: Field name in SET clause must be literal, not parameterized
	updateQuery := fmt.Sprintf("UPDATE type::table($tb) SET %s = false", softDeletedColumn)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
		"tb": table,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize soft delete column %s in table %s: %w", softDeletedColumn, table, err)
	}

	m.LogInfo("Converted table from live to soft delete mode",
		"table", table,
		"soft_delete_column", softDeletedColumn,
	)

	return nil
}
