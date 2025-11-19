package migrator

import (
	"context"
)

// ModeHistoryToLive converts a history-mode table back to live mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#history_to_live
//
// This removes history tracking and keeps only the latest active version of each record.
func (m *Migrator) ModeHistoryToLive(ctx context.Context, schema, table string, keepDeletedRows bool) error {
	// TODO: Implement history to live mode migration
	// 1. If keepDeletedRows is false, delete all non-active records:
	//    array::last(DELETE (SELECT id FROM table WHERE _fivetran_active = false AND id > $last_deleted_id LIMIT $batch_size) RETURN BEFORE)
	//    Repeat until no more records are deleted.
	//    You cannot remove records anymore once array::last returns None,
	//    because array::last returns an object containing `id` which is the record id of the last deleted record if any were deleted)
	// 2. Remove the compound index on Fivetran primary key columns (See DefineFivetranPKIndex)
	// 3. Remove history mode columns:
	//    REMOVE FIELD _fivetran_start ON table
	//    REMOVE FIELD _fivetran_end ON table
	//    REMOVE FIELD _fivetran_active ON table
	// 4. Update each record's ID to not contain _fivetran_start.
	//    Note that you cannot `SET id = array::slice(record::id(id), 0, array::len(record::id(id) - 1))`` because SurrealDB does not allow updating the record IDs.
	//    Instead, you need to create another table with the desired record IDs, remove the old table, and create a new table with the original name.
	//    In addition to that, you must omit _fivetran_start, _fivetran_end, and _fivetran_active fields when inserting into the new table,
	//    as those fields will no longer exist in live mode, and REMOVE FIELD does not remove the data from existing records.
	//    array::last(INSERT INTO new_table SELECT *, array::slice(record::id(id), 0, array::len(record::id(id)) - 1) as id FROM DELETE (SELECT * OMIT _fivetran_start, _fivetran_end, _fivetran_active FROM old_table LIMIT $batch_size) RETURN BEFORE) // Repeat until all records are migrated to new temp table
	//    array::last(INSERT INTO old_table DELETE SELECT * FROM new_table LIMIT $batch_size RETURN BEFORE) // Move records back to original table
	//    REMOVE TABLE new_table // Clean up the emptied temporary table
	// 5. Return any errors encountered

	return nil
}
