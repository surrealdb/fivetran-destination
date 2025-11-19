package migrator

import (
	"context"
)

// ModeSoftDeleteToLive converts a soft-delete mode table to live mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#soft_delete_to_live
//
// This removes the soft delete column and optionally deletes soft-deleted records.
func (m *Migrator) ModeSoftDeleteToLive(ctx context.Context, schema, table, softDeletedColumn string) error {
	// TODO: Implement soft delete to live mode migration
	// 1. DELETE FROM table WHERE softDeletedColumn = true
	// 2. Remove the soft delete column:
	//    REMOVE FIELD softDeletedColumn ON table
	// 3. Update all records to ensure no soft delete markers remain:
	//    UPDATE table UNSET softDeletedColumn
	// 4. Return any errors encountered

	return nil
}
