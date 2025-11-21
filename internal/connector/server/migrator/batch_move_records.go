package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// BatchMoveRecords moves records from oldTable to newTable in batches to prevent
// large transactions that could exhaust memory and crash RocksDB.
//
// The function executes batched DELETE-INSERT operations until all records are moved.
// Each batch is a separate transaction to keep WAL entries small.
//
// Parameters:
//   - oldTable: source table name
//   - newTable: destination table name
//   - selectedFields: fields to SELECT from old table (e.g., "id, name, value" or "*")
//   - insertedFields: fields to INSERT into new table (e.g., "id, name, value" or "*")
//   - batchSize: number of records per batch
//
func (m *Migrator) BatchMoveRecords(ctx context.Context, oldTable, newTable, selectedFields, insertedFields string, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Query template: DELETE records from old table, INSERT into new table
	// array::last returns NONE when no records remain
	query := fmt.Sprintf(`
		array::last(
			INSERT INTO %s SELECT %s FROM (
				DELETE (SELECT %s FROM %s LIMIT $batch_size) RETURN BEFORE
			)
		)
	`, newTable, insertedFields, selectedFields, oldTable)

	for {
		results, err := surrealdb.Query[any](ctx, m.db, query, map[string]any{
			"batch_size": batchSize,
		})
		if err != nil {
			return fmt.Errorf("batch move failed: %w", err)
		}

		// Check if result is NONE/nil (no more records)
		if results == nil || len(*results) == 0 {
			break
		}

		result := (*results)[0].Result
		if result == nil {
			break
		}
	}

	m.LogInfo("Batch move completed",
		"old_table", oldTable,
		"new_table", newTable,
	)

	return nil
}
