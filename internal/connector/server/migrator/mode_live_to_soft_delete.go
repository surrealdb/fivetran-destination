package migrator

import (
	"context"
)

// ModeLiveToSoftDelete converts a live-mode table to soft-delete mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#live_to_soft_delete
//
// This adds a soft delete column to enable tracking of deleted records.
func (m *Migrator) ModeLiveToSoftDelete(ctx context.Context, schema, table, softDeletedColumn string) error {
	// TODO: Implement live to soft delete mode migration
	// 1. Add soft delete column:
	//    DEFINE FIELD softDeletedColumn ON table TYPE bool
	// 2. Initialize all existing records as not deleted:
	//    UPDATE table SET softDeletedColumn = false
	// 3. Return any errors encountered

	return nil
}
