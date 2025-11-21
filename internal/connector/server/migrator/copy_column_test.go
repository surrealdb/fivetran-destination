package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestCopyColumn_BasicCopy(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com';
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy column
	err = migrator.CopyColumn(ctx, namespace, "users", "name", "display_name")
	require.NoError(t, err, "CopyColumn failed")

	// Verify data was copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify both columns have the same data
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "Alice", records[0]["display_name"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "Bob", records[1]["display_name"])

	// Verify new field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasDisplayName := fields["display_name"]
	assert.True(t, hasDisplayName, "display_name field should exist in schema")
}

func TestCopyColumn_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_users SCHEMAFULL;
		DEFINE FIELD name ON empty_users TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Copy column on empty table
	err = migrator.CopyColumn(ctx, namespace, "empty_users", "name", "display_name")
	require.NoError(t, err, "CopyColumn on empty table should not fail")

	// Verify new field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasDisplayName := fields["display_name"]
	assert.True(t, hasDisplayName, "display_name field should exist in schema")
}

func TestCopyColumn_IntColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with int column
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD stock ON products TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', stock = 100;
		CREATE products:2 SET name = 'Product B', stock = 50;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy int column
	err = migrator.CopyColumn(ctx, namespace, "products", "stock", "quantity")
	require.NoError(t, err, "CopyColumn failed")

	// Verify data was copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	assert.Equal(t, uint64(100), records[0]["stock"])
	assert.Equal(t, uint64(100), records[0]["quantity"])
	assert.Equal(t, uint64(50), records[1]["stock"])
	assert.Equal(t, uint64(50), records[1]["quantity"])
}

func TestCopyColumn_FloatColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with float column
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD price ON items TYPE option<float>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item A', price = 9.99;
		CREATE items:2 SET name = 'Item B', price = 19.99;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy float column
	err = migrator.CopyColumn(ctx, namespace, "items", "price", "cost")
	require.NoError(t, err, "CopyColumn failed")

	// Verify data was copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	assert.Equal(t, float64(9.99), records[0]["price"])
	assert.Equal(t, float64(9.99), records[0]["cost"])
	assert.Equal(t, float64(19.99), records[1]["price"])
	assert.Equal(t, float64(19.99), records[1]["cost"])
}

func TestCopyColumn_NonexistentSourceColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE test_table SCHEMAFULL;
		DEFINE FIELD name ON test_table TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Try to copy non-existent column
	err = migrator.CopyColumn(ctx, namespace, "test_table", "nonexistent", "new_column")
	require.Error(t, err, "CopyColumn should fail for non-existent source column")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestCopyColumn_WithNoneValues(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with optional column
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD notes ON orders TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with some NONE values
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:1 SET customer = 'Alice', notes = 'Priority order';
		CREATE orders:2 SET customer = 'Bob';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy column with NONE values
	err = migrator.CopyColumn(ctx, namespace, "orders", "notes", "comments")
	require.NoError(t, err, "CopyColumn failed")

	// Verify data was copied (including NONE values)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// First record has notes
	assert.Equal(t, "Priority order", records[0]["notes"])
	assert.Equal(t, "Priority order", records[0]["comments"])

	// Second record has NONE for notes (field not present)
	_, hasNotes := records[1]["notes"]
	assert.False(t, hasNotes, "Second record should not have notes field")
	_, hasComments := records[1]["comments"]
	assert.False(t, hasComments, "Second record should not have comments field (copied from NONE)")
}
