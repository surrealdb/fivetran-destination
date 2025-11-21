package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// ModeHistoryToLive converts a history-mode table back to live mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#history_to_live
//
// This removes history tracking and keeps only the latest active version of each record.
func (m *Migrator) ModeHistoryToLive(ctx context.Context, schema, table string, keepDeletedRows bool) error {
	const batchSize = 1000

	// 1. If keepDeletedRows is false, delete all non-active records
	if !keepDeletedRows {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE _fivetran_active = false", table)
		_, err := surrealdb.Query[any](ctx, m.db, deleteQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to delete inactive records from %s: %w", table, err)
		}
	}

	// 2. Remove history mode field definitions from schema
	for _, field := range []string{"_fivetran_start", "_fivetran_end", "_fivetran_active"} {
		removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", field, table)
		_, err := surrealdb.Query[any](ctx, m.db, removeQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to remove field %s from %s: %w", field, table, err)
		}
	}

	// 3. Update IDs to remove _fivetran_start component and omit history fields from data
	// ID transformation: remove _fivetran_start from array-based ID [pk1, pk2, ..., _fivetran_start] -> [pk1, pk2, ...]
	idExpression := "array::slice(record::id(id), 0, array::len(record::id(id)) - 1)"
	insertedFields := "* OMIT _fivetran_start, _fivetran_end, _fivetran_active"

	err := m.BatchUpdateIDs(ctx, table, "*", idExpression, insertedFields, batchSize)
	if err != nil {
		return fmt.Errorf("failed to update record IDs: %w", err)
	}

	m.LogInfo("Converted table from history to live mode",
		"table", table,
		"keep_deleted_rows", keepDeletedRows,
	)

	return nil
}
