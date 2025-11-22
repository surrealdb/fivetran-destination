package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
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
	const batchSize = 1000
	const maxIterations = 100

	// 1. Add soft delete column
	defineFieldQuery := fmt.Sprintf("DEFINE FIELD %s ON %s TYPE option<bool>", softDeletedColumn, table)
	_, err := surrealdb.Query[any](ctx, m.db, defineFieldQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to add soft delete column %s: %w", softDeletedColumn, err)
	}

	// 2. Delete historical versions (non-latest) in batches
	// For each primary key, keep only the record with the highest _fivetran_start
	deleteQuery := fmt.Sprintf(`
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
					FROM %s
					WHERE id >= $start_id
					LIMIT $batch_size
				) GROUP BY pk
			)
		) GROUP ALL;
		LET $to_delete = $res[0].to_delete;
		LET $deleted = IF array::len($to_delete) > 0 { DELETE $to_delete RETURN BEFORE } ELSE { [] };
		RETURN {
			first_seen_pk: $res[0].first_seen_pk,
			last_seen_pk: $res[0].last_seen_pk,
			to_delete: $to_delete,
			deleted: $deleted
		};
		COMMIT;
	`, table)

	// Check if table is empty first
	type CountResult struct {
		Count int `cbor:"count"`
	}
	countResults, err := surrealdb.Query[[]CountResult](ctx, m.db, fmt.Sprintf("SELECT count() AS count FROM %s GROUP ALL", table), nil)
	if err != nil {
		return fmt.Errorf("failed to count records in %s: %w", table, err)
	}

	tableIsEmpty := countResults == nil || len(*countResults) == 0 || len((*countResults)[0].Result) == 0 || (*countResults)[0].Result[0].Count == 0

	// Start from the beginning
	startID := models.NewRecordID(table, []any{})
	completed := tableIsEmpty // Skip deletion loop if table is empty
	for iteration := 0; iteration < maxIterations && !completed; iteration++ {
		type DeleteResult struct {
			FirstSeenPK []any            `cbor:"first_seen_pk"`
			LastSeenPK  []any            `cbor:"last_seen_pk"`
			ToDelete    []map[string]any `cbor:"to_delete"`
			Deleted     []map[string]any `cbor:"deleted"`
		}
		results, err := surrealdb.Query[DeleteResult](ctx, m.db, deleteQuery, map[string]any{
			"start_id":   startID,
			"batch_size": batchSize,
		})
		if err != nil {
			return fmt.Errorf("failed to delete historical versions: %w", err)
		}

		// Check results are valid
		if results == nil {
			return fmt.Errorf("unexpected nil results when deleting historical versions from %s", table)
		}
		if len(*results) == 0 {
			return fmt.Errorf("unexpected empty results when deleting historical versions from %s", table)
		}

		// Get the result from the RETURN statement (first result contains the returned value)
		res := (*results)[0].Result

		// Stop if first_seen_pk == last_seen_pk and to_delete/deleted are empty
		// This means we've processed all records
		if len(res.ToDelete) == 0 && len(res.Deleted) == 0 {
			if len(res.FirstSeenPK) == len(res.LastSeenPK) {
				allEqual := true
				for i := range res.FirstSeenPK {
					if res.FirstSeenPK[i] != res.LastSeenPK[i] {
						allEqual = false
						break
					}
				}
				if allEqual {
					completed = true
					break
				}
			}
		}

		// Update start_id for next iteration based on last_seen_pk
		if len(res.LastSeenPK) == 0 {
			completed = true
			break
		}
		startID = models.NewRecordID(table, res.LastSeenPK)
	}

	if !completed {
		return fmt.Errorf("exceeded maximum iterations (%d) while deleting historical versions from %s", maxIterations, table)
	}

	// 3. Remove history mode field definitions from schema
	for _, field := range []string{"_fivetran_start", "_fivetran_end", "_fivetran_active"} {
		removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", field, table)
		_, err := surrealdb.Query[any](ctx, m.db, removeQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to remove field %s from %s: %w", field, table, err)
		}
	}

	// 4. Update IDs to remove _fivetran_start component and convert _fivetran_active to soft delete column
	// ID transformation: [pk1, pk2, ..., _fivetran_start] -> [pk1, pk2, ...]
	// Also: softDeletedColumn = NOT _fivetran_active
	idExpression := "array::slice(record::id(id), 0, array::len(record::id(id)) - 1)"
	insertedFields := fmt.Sprintf("NOT(_fivetran_active) AS %s, * OMIT _fivetran_start, _fivetran_end, _fivetran_active", softDeletedColumn)

	err = m.BatchUpdateIDs(ctx, table, "*", idExpression, insertedFields, batchSize, nil)
	if err != nil {
		return fmt.Errorf("failed to update record IDs: %w", err)
	}

	m.LogInfo("Converted table from history to soft delete mode",
		"table", table,
		"soft_delete_column", softDeletedColumn,
	)

	return nil
}
