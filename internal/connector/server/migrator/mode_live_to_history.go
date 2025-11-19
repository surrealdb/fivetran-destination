package migrator

import (
	"context"
)

// ModeLiveToHistory converts a live-mode table to history mode for temporal tracking.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#live_to_history
//
// According to the Fivetran Partner SDK documentation, this operation should:
//   - Add history columns (_fivetran_start, _fivetran_end, _fivetran_active)
//     and populate with appropriate timestamps for all existing records
//
// This enables transition from simple current-state tracking to full
// historical record maintenance.
func (m *Migrator) ModeLiveToHistory(ctx context.Context, schema, table string) error {
	// TODO: Implement live to history mode migration
	// 1. Add history mode columns:
	//    DEFINE FIELD _fivetran_start ON table TYPE datetime
	//    DEFINE FIELD _fivetran_end ON table TYPE datetime
	//    DEFINE FIELD _fivetran_active ON table TYPE bool
	// 2. Decide the current timestamp to use for initialization
	//    now := time.Now().UTC()
	// 3. Create composite index on Fivetran primary keys (See DefineFivetranPKIndex)
	// 4. Initialize all existing records as active:
	//    UPDATE table SET
	//      _fivetran_start = $now,
	//      _fivetran_end = 9999-12-31T23:59:59Z,
	//      _fivetran_active = true
	// 5. Return any errors encountered

	return nil
}
