package migrator

import (
	"context"
)

// CopyTableToHistoryMode copies an existing table from non-history mode to a new table
// configured for history mode, allowing customers to transition tables while preserving data.
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Create the destination table (toTable) with history mode columns in the schema
//    (_fivetran_start, _fivetran_end, _fivetran_active, _fivetran_synced)
// 2. Copy data from source (fromTable) to destination using INSERT-SELECT
// 3. Apply sync mode migration:
//   - If softDeletedColumn is specified: follow SOFT_DELETE_TO_HISTORY procedure
//   - Otherwise: follow LIVE_TO_HISTORY procedure
//
// This properly configures the destination table's history mode columns.
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC.
func (m *Migrator) CopyTableToHistoryMode(ctx context.Context, schema, table, fromTable, toTable, softDeletedColumn string) error {
	// TODO: Implement copy table to history mode logic
	// 1. Create destination table with history mode schema:
	//    DEFINE TABLE toTable SCHEMAFULL
	//    - Copy all fields from fromTable
	//    - Add _fivetran_start (datetime)
	//    - Add _fivetran_end (datetime)
	//    - Add _fivetran_active (bool)
	//    - Add _fivetran_synced (datetime)
	// 2. Copy data from source to destination:
	//    INSERT INTO toTable SELECT * FROM fromTable
	// 3. Initialize history mode columns:
	//    - Set _fivetran_start to current timestamp or _fivetran_synced
	//    - Set _fivetran_end to max datetime (9999-12-31 23:59:59)
	//    - Set _fivetran_active to true
	// 4. If softDeletedColumn is specified:
	//    - For soft-deleted records (_fivetran_deleted = true):
	//      Set _fivetran_active = false and _fivetran_end = current timestamp
	// 5. Return any errors encountered

	return nil
}
