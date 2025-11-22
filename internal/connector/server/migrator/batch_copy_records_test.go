package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestBatchCopyRecords_BasicCopy(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert test data with array IDs
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:[1] SET name = 'Alice', value = 100;
		CREATE source:[2] SET name = 'Bob', value = 200;
		CREATE source:[3] SET name = 'Charlie', value = 300;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy all records
	err = migrator.BatchCopyRecords(ctx, "source", "dest", "*", 1000)
	require.NoError(t, err, "BatchCopyRecords failed")

	// Verify all records were copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in dest")

	// Verify data
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, uint64(100), records[0]["value"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, uint64(200), records[1]["value"])
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.Equal(t, uint64(300), records[2]["value"])
}

func TestBatchCopyRecords_WithOmit(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD secret ON source TYPE option<string>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert test data with array IDs
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:[1] SET name = 'Alice', secret = 'password123', value = 100;
		CREATE source:[2] SET name = 'Bob', secret = 'qwerty', value = 200;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Create destination table (without secret field)
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy records, omitting secret field
	err = migrator.BatchCopyRecords(ctx, "source", "dest", "* OMIT secret", 1000)
	require.NoError(t, err, "BatchCopyRecords failed")

	// Verify records were copied without secret field
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records in dest")

	// Verify secret field is not present
	assert.Equal(t, "Alice", records[0]["name"])
	assert.NotContains(t, records[0], "secret", "secret field should be omitted")
	assert.Equal(t, uint64(100), records[0]["value"])
}

func TestBatchCopyRecords_SmallBatchSize(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert 10 records with array IDs
	for i := 1; i <= 10; i++ {
		_, err = surrealdb.Query[any](ctx, db, "CREATE type::thing('source', [$id]) SET value = $value", map[string]any{
			"id":    i,
			"value": i * 10,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy with small batch size (2 records per batch)
	err = migrator.BatchCopyRecords(ctx, "source", "dest", "*", 2)
	require.NoError(t, err, "BatchCopyRecords failed")

	// Verify all 10 records were copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 10, "Expected 10 records in dest")

	// Verify all values
	for i := range 10 {
		assert.Equal(t, uint64((i+1)*10), records[i]["value"], "Record %d has wrong value", i)
	}
}

func TestBatchCopyRecords_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy from empty table
	err = migrator.BatchCopyRecords(ctx, "source", "dest", "*", 1000)
	require.NoError(t, err, "BatchCopyRecords on empty table should not fail")

	// Verify dest table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Dest table should be empty")
}

