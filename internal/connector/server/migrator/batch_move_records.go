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
//   - additionalVars: additional query parameters to pass to the query (can be nil)
//
func (m *Migrator) BatchMoveRecords(ctx context.Context, oldTable, newTable, selectedFields, insertedFields string, batchSize int, additionalVars map[string]any) error {
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
		// Build query parameters by merging additionalVars with batch_size
		queryParams := map[string]any{
			"batch_size": batchSize,
		}
		// Merge additional variables if provided
		for k, v := range additionalVars {
			queryParams[k] = v
		}

		results, err := surrealdb.Query[any](ctx, m.db, query, queryParams)
		if err != nil {
			return fmt.Errorf("batch move failed: %w", err)
		}

		// Check results are valid - should always have exactly 1 result (record or NONE)
		if results == nil {
			return fmt.Errorf("unexpected nil results during batch move from %s to %s", oldTable, newTable)
		}
		if len(*results) != 1 {
			return fmt.Errorf("unexpected result count %d during batch move from %s to %s, expected 1", len(*results), oldTable, newTable)
		}

		// If result is NONE/nil, we've processed all records
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
