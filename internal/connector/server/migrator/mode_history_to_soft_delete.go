package migrator

import (
	"context"
)

// This query identifies and deletes non-latest versions of records in batches.
// It groups records by their primary key (excluding the _fivetran_start part of the ID)
// and deletes all but the latest version for each primary key.
// Parameters:
//   - $table: The name of the table to operate on.
//   - $start_id: The starting record ID to consider for deletion (for pagination).
//   - $batch_size: The maximum number of records to process in this batch.
//
// The caller should repeat calling this query, updating $start_id based on the last seen ID,
// until no more records can be deleted (i.e. first_seen_pk and last_seen_pk are the same and to_delete is empty).
//
// The example parameters for the executing this by hand could be:
// let $table = "foo";
// let $batch_size = 10;
// let $start_id = type::thing($table, []);
const _ = `
BEGIN;
LET $res = SELECT
  array::first(pk) AS first_seen_pk,
  array::last(pk) AS last_seen_pk,
  array::group(to_delete_per_pk) AS to_delete
FROM (
  SELECT
    pk,
    array::slice(group, 0, array::len(group)-1) as to_delete_per_pk
  FROM (
    SELECT
        pk,
        array::group(id) AS group
    FROM (
        SELECT
            id,
            array::slice(record::id(id), 0, array::len(record::id(id))-1) AS pk
        FROM
            type::table($table)
        WHERE
            id >= $start_id
        LIMIT $batch_size // We need to limit before grouping to group up to $batch_size records only
    ) GROUP BY pk
  )
) GROUP ALL;
LET $deleted = DELETE $res[0].to_delete RETURN BEFORE;
RETURN {
    first_seen_pk: $res[0].first_seen_pk,
    last_seen_pk: $res[0].last_seen_pk,
    to_delete: $res[0].to_delete,
    deleted: $deleted,
};
COMMIT;
`

// ModeHistoryToSoftDelete converts a history-mode table back to soft-delete mode.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#history_to_soft_delete
//
// This reverses the history mode conversion, keeping only the latest version
// of each record.
func (m *Migrator) ModeHistoryToSoftDelete(ctx context.Context, schema, table, softDeletedColumn string) error {
	// TODO: Implement history to soft delete mode migration
	// 1. Add soft delete column if not present:
	//    DEFINE FIELD softDeletedColumn ON table TYPE bool
	// 2. Delete historical versions (non-latest):
	//    Conceptually `DELETE SELECT * FROM table WHERE is_not_latest_version` in batches.
	//    See the const above for an example query to identify and delete non-latest versions in batches.
	// 3. Remove Fivetran primary key index
	//    (See DefineFivetranPKIndex)
	// 4. Remove history mode columns:
	//    REMOVE FIELD _fivetran_start ON table
	//    REMOVE FIELD _fivetran_end ON table
	//    REMOVE FIELD _fivetran_active ON table
	// 5. Update each record's ID to not contain _fivetran_start.
	//    Note that you cannot `SET id = array::slice(record::id(id), 0, array::len(record::id(id) - 1))`` because SurrealDB does not allow updating the record IDs.
	//    Instead, you need to create another table with the desired record IDs, remove the old table, and create a new table with the original name.
	//    array::last(INSERT INTO new_table SELECT array::slice(record::id(id), 0, array::len(record::id(id)) - 1) as id, not(_fivetran_active) as softDeletedColumn, * OMIT _fivetran_active FROM DELETE (SELECT * OMIT _fivetran_start, _fivetran_end FROM old_table LIMIT $batch_size) RETURN BEFORE) // Repeat until all records are migrated to new temp table
	//    array::last(INSERT INTO old_table DELETE SELECT * FROM new_table LIMIT $batch_size RETURN BEFORE) // Move records back to original table
	//    REMOVE TABLE new_table // Clean up the emptied temporary table
	// 6. Return any errors encountered

	return nil
}
