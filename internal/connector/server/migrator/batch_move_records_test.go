package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestBatchMoveRecords_BasicMove(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source and destination tables
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source_table SCHEMAFULL;
		DEFINE FIELD value ON source_table TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON source_table TYPE option<datetime>;

		DEFINE TABLE dest_table SCHEMAFULL;
		DEFINE FIELD value ON dest_table TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON dest_table TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create tables")

	// Insert 10 records into source
	for i := 1; i <= 10; i++ {
		_, err = surrealdb.Query[any](ctx, db, `CREATE type::thing("source_table", $id) SET value = $value, _fivetran_synced = d'2024-01-01T00:00:00Z'`, map[string]any{
			"id":    i,
			"value": i * 10,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Move records in batches of 3
	err = migrator.BatchMoveRecords(ctx, "source_table", "dest_table", "*", "*", 3, nil)
	require.NoError(t, err, "BatchMoveRecords failed")

	// Verify source is empty
	srcResults, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM source_table", nil)
	require.NoError(t, err)
	require.NotNil(t, srcResults)
	require.NotEmpty(t, *srcResults)
	assert.Empty(t, (*srcResults)[0].Result, "Source table should be empty")

	// Verify destination has all records
	dstResults, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest_table", nil)
	require.NoError(t, err)
	require.NotNil(t, dstResults)
	require.NotEmpty(t, *dstResults)
	records := (*dstResults)[0].Result
	assert.Len(t, records, 10, "Destination should have 10 records")
}

func TestBatchMoveRecords_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty source and destination tables
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_source SCHEMAFULL;
		DEFINE FIELD value ON empty_source TYPE option<int>;

		DEFINE TABLE empty_dest SCHEMAFULL;
		DEFINE FIELD value ON empty_dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create tables")

	// Move from empty table
	err = migrator.BatchMoveRecords(ctx, "empty_source", "empty_dest", "*", "*", 10, nil)
	require.NoError(t, err, "BatchMoveRecords on empty table should not fail")
}

func TestBatchMoveRecords_DefaultBatchSize(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create tables
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE src SCHEMAFULL;
		DEFINE FIELD value ON src TYPE option<int>;

		DEFINE TABLE dst SCHEMAFULL;
		DEFINE FIELD value ON dst TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create tables")

	// Insert 5 records
	for i := 1; i <= 5; i++ {
		_, err = surrealdb.Query[any](ctx, db, `CREATE type::thing("src", $id) SET value = $value`, map[string]any{
			"id":    i,
			"value": i,
		})
		require.NoError(t, err)
	}

	// Move with batchSize=0 (should use default 1000)
	err = migrator.BatchMoveRecords(ctx, "src", "dst", "*", "*", 0, nil)
	require.NoError(t, err, "BatchMoveRecords with default batch size should not fail")
}
