package migrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// CopyTableToHistoryMode copies an existing table from non-history mode to a new table
// configured for history mode, allowing customers to transition tables while preserving data.
//
// According to the Fivetran Partner SDK documentation, this operation should:
//  1. Create the destination table (toTable) with history mode columns in the schema
//     (_fivetran_start, _fivetran_end, _fivetran_active, _fivetran_synced)
//  2. Copy data from source (fromTable) to destination using INSERT-SELECT
//  3. Apply sync mode migration:
//     - If softDeletedColumn is specified: follow SOFT_DELETE_TO_HISTORY procedure
//     - Otherwise: follow LIVE_TO_HISTORY procedure
//
// This properly configures the destination table's history mode columns.
//
// Parameters:
//   - schema: database namespace (unused for SurrealDB)
//   - table: unused parameter for compatibility
//   - fromTable: source table name (can be live-mode or soft-delete-mode)
//   - toTable: destination table name to create
//   - softDeletedColumn: name of the soft delete column if source is in soft-delete mode (empty string if live mode)
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC.
func (m *Migrator) CopyTableToHistoryMode(ctx context.Context, schema, table, fromTable, toTable, softDeletedColumn string) error {
	const batchSize = 1000

	now := models.CustomDateTime{Time: time.Now().UTC()}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	// 1. Get source table schema to replicate field definitions
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", fromTable), nil)
	if err != nil {
		return fmt.Errorf("failed to get source table info: %w", err)
	}
	if infoResults == nil || len(*infoResults) == 0 {
		return fmt.Errorf("failed to get source table info: empty results")
	}
	sourceFields := (*infoResults)[0].Result.Fields

	// 2. Create destination table with same fields plus history fields
	_, err = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("DEFINE TABLE %s SCHEMAFULL", toTable), nil)
	if err != nil {
		return fmt.Errorf("failed to create destination table: %w", err)
	}

	// Copy field definitions from source table, replacing table name
	for fieldName, fieldDef := range sourceFields {
		// Skip soft delete column if present - we'll omit it during copy
		if softDeletedColumn != "" && fieldName == softDeletedColumn {
			continue
		}
		// Replace old table name with new table name in the field definition
		// fieldDef is like "DEFINE FIELD fieldName ON oldTable TYPE ..."
		newFieldDef := strings.Replace(fieldDef, " ON "+fromTable+" ", " ON "+toTable+" ", 1)
		_, err = surrealdb.Query[any](ctx, m.db, newFieldDef, nil)
		if err != nil {
			return fmt.Errorf("failed to define field %s on %s: %w", fieldName, toTable, err)
		}
	}

	// Add history mode fields to destination table
	historyFields := []string{
		fmt.Sprintf("DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>", toTable),
		fmt.Sprintf("DEFINE FIELD _fivetran_end ON %s TYPE option<datetime>", toTable),
		fmt.Sprintf("DEFINE FIELD _fivetran_active ON %s TYPE option<bool>", toTable),
	}
	for _, fieldDef := range historyFields {
		_, err := surrealdb.Query[any](ctx, m.db, fieldDef, nil)
		if err != nil {
			return fmt.Errorf("failed to add history field: %w", err)
		}
	}

	// 3. Copy records with history mode transformation
	var insertedFields string
	if softDeletedColumn != "" {
		// Source is soft-delete mode
		// _fivetran_active = NOT(softDeletedColumn), omit softDeletedColumn
		insertedFields = fmt.Sprintf(
			"$now AS _fivetran_start, $end_max AS _fivetran_end, NOT(%s) AS _fivetran_active, * OMIT %s",
			softDeletedColumn,
			softDeletedColumn,
		)
	} else {
		// Source is live mode
		// _fivetran_active = true
		insertedFields = "$now AS _fivetran_start, $end_max AS _fivetran_end, true AS _fivetran_active, *"
	}

	// ID transformation: [pk...] -> [pk..., _fivetran_start]
	idExpression := "array::add(record::id(id), _fivetran_start)"

	// Use a modified version of BatchCopyRecordsWithNewIDs that supports parameters
	// We need to pass $now and $end_max as parameters
	copyQuery := fmt.Sprintf(`
		BEGIN;
		LET $selected = SELECT * FROM %s WHERE id > $start_id LIMIT $batch_size;
		LET $inserted = INSERT INTO %s (SELECT %s AS id, %s FROM $selected);
		RETURN {
			last_source_record: array::last($selected),
			inserted_count: array::len($inserted)
		};
		COMMIT;
	`, fromTable, toTable, idExpression, insertedFields)

	// Start from the beginning
	startID := models.NewRecordID(fromTable, []any{})

	for {
		results, err := surrealdb.Query[map[string]any](ctx, m.db, copyQuery, map[string]any{
			"start_id":   startID,
			"batch_size": batchSize,
			"now":        now,
			"end_max":    endTimeMax,
		})
		if err != nil {
			return fmt.Errorf("batch copy to history mode failed: %w", err)
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

	m.LogInfo("Copied table to history mode",
		"source_table", fromTable,
		"dest_table", toTable,
		"soft_delete_mode", softDeletedColumn != "",
	)

	return nil
}
