package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// BatchCopyRecordsWithNewIDs copies records from fromTable to toTable in batches
// while transforming their IDs.
//
// Parameters:
//   - fromTable: source table name
//   - selectedFields: fields to SELECT from source (e.g., "*", "* OMIT foo")
//   - toTable: destination table name
//   - idExpression: SurrealQL expression to compute new ID (e.g., "array::slice(record::id(id), 0, 2)")
//   - insertedFields: fields to INSERT into destination (e.g., "*", "field1, field2")
//   - batchSize: number of records per batch
//   - additionalVars: additional query parameters to pass to the query (can be nil)
//
// This function is useful when you need to copy records while changing their ID structure.
// Unlike BatchCopyRecords, this tracks the original source IDs for pagination rather than
// the new inserted IDs.
//
// Example use cases:
//   - Copying with simplified IDs: idExpression = "array::slice(record::id(id), 0, 1)"
//   - Copying with extended IDs: idExpression = "array::add(record::id(id), $timestamp)"
//   - Copying with field transformations: insertedFields = "*, computed_field"
//   - Using additional variables: additionalVars = map[string]any{"now": time.Now(), "max": 100}
func (m *Migrator) BatchCopyRecordsWithNewIDs(ctx context.Context, fromTable, selectedFields, toTable, idExpression, insertedFields string, batchSize int, additionalVars map[string]any) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Query to copy records in batches with ID transformation
	// Use temp variable $selected to track original IDs for pagination
	copyQuery := fmt.Sprintf(`
		BEGIN;
		LET $selected = SELECT %s FROM %s WHERE id > $start_id LIMIT $batch_size;
		LET $inserted = INSERT INTO %s (SELECT %s AS id, %s FROM $selected);
		RETURN {
			last_source_record: array::last($selected),
			inserted_count: array::len($inserted)
		};
		COMMIT;
	`, selectedFields, fromTable, toTable, idExpression, insertedFields)

	// Start from the beginning
	startID := models.NewRecordID(fromTable, []any{})

	for {
		// Build query parameters by merging additionalVars with pagination params
		queryParams := map[string]any{
			"start_id":   startID,
			"batch_size": batchSize,
		}
		// Merge additional variables if provided
		for k, v := range additionalVars {
			queryParams[k] = v
		}

		results, err := surrealdb.Query[map[string]any](ctx, m.db, copyQuery, queryParams)
		if err != nil {
			return fmt.Errorf("batch copy with new IDs failed: %w", err)
		}

		// Check results are valid
		if results == nil {
			return fmt.Errorf("unexpected nil results during batch copy from %s to %s", fromTable, toTable)
		}
		if len(*results) != 1 {
			return fmt.Errorf("unexpected result count %d during batch copy from %s to %s, expected 1", len(*results), fromTable, toTable)
		}

		result := (*results)[0].Result

		// Extract last_source_record from result
		lastSourceRecord, ok := result["last_source_record"]
		if !ok {
			// No last_source_record means no more records to process
			break
		}

		// If last_source_record is nil/NONE, we've processed all records
		if lastSourceRecord == nil {
			break
		}

		// Get the last source record (as a map)
		lastSourceRecordMap, ok := lastSourceRecord.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected last_source_record type during batch copy: %T", lastSourceRecord)
		}

		// Extract the ID field from the source record
		recordIDField, ok := lastSourceRecordMap["id"]
		if !ok {
			return fmt.Errorf("last source record missing 'id' field during batch copy")
		}

		lastSourceRecordID, ok := recordIDField.(models.RecordID)
		if !ok {
			return fmt.Errorf("unexpected id field type during batch copy: %T", recordIDField)
		}

		// Update start_id for next iteration using the original source ID
		startID = models.NewRecordID(fromTable, lastSourceRecordID.ID)
	}

	m.LogInfo("Batch copy with new IDs completed",
		"from_table", fromTable,
		"to_table", toTable,
	)

	return nil
}
