package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// BatchCopyRecords copies records from oldTable to newTable in batches.
// This is useful for copying data while applying field transformations.
//
// Parameters:
//   - oldTable: source table name
//   - newTable: destination table name
//   - fields: fields to SELECT and INSERT (e.g., "*", "* OMIT foo")
//   - batchSize: number of records per batch
//
// The fields parameter allows transformations like:
//   - "*" - copy all fields as-is (including the id field)
//   - "* OMIT field1, field2" - copy all fields except field1 and field2
//   - "id, field1, field2" - copy specific fields including id
//
// Important: The id field can be included in the fields parameter, but
// changing the ID content using expressions like "array::slice(...) AS id"
// will NOT work properly because pagination requires tracking the source
// table's IDs. Use BatchUpdateIDs or BatchMoveRecords for ID transformations.
func (m *Migrator) BatchCopyRecords(ctx context.Context, oldTable, newTable, fields string, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Query to copy records in batches
	// Returns the last inserted record to continue from
	copyQuery := fmt.Sprintf(`
		array::last(
			INSERT INTO %s SELECT %s FROM %s WHERE id > $start_id LIMIT $batch_size
		)
	`, newTable, fields, oldTable)

	// Start from the beginning
	startID := models.NewRecordID(oldTable, []any{})

	for {
		results, err := surrealdb.Query[any](ctx, m.db, copyQuery, map[string]any{
			"start_id":   startID,
			"batch_size": batchSize,
		})
		if err != nil {
			return fmt.Errorf("batch copy failed: %w", err)
		}

		// Check results are valid
		if results == nil {
			return fmt.Errorf("unexpected nil results during batch copy from %s to %s", oldTable, newTable)
		}
		if len(*results) != 1 {
			return fmt.Errorf("unexpected result count %d during batch copy from %s to %s, expected 1", len(*results), oldTable, newTable)
		}

		result := (*results)[0].Result

		// If result is nil/NONE, we've processed all records
		if result == nil {
			break
		}

		// Get the last inserted record (as a map)
		lastInsertedRecord, ok := result.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected result type during batch copy: %T", result)
		}

		// Extract the ID field from the record
		recordIDField, ok := lastInsertedRecord["id"]
		if !ok {
			return fmt.Errorf("last inserted record missing 'id' field during batch copy")
		}

		lastInsertedRecordID, ok := recordIDField.(models.RecordID)
		if !ok {
			return fmt.Errorf("unexpected id field type during batch copy: %T", recordIDField)
		}

		// Update start_id for next iteration
		// Use the oldTable name since we're querying FROM oldTable
		// The ID part is the same, just the table name changes
		startID = models.NewRecordID(oldTable, lastInsertedRecordID.ID)
	}

	m.LogInfo("Batch copy completed",
		"old_table", oldTable,
		"new_table", newTable,
	)

	return nil
}
