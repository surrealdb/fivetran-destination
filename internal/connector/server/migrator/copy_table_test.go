package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestCopyTable_BasicCopy(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with data
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
		CREATE users:3 SET name = 'Charlie', email = 'charlie@example.com';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy table
	err = migrator.CopyTable(ctx, namespace, "users", "users_backup")
	require.NoError(t, err, "CopyTable failed")

	// Verify destination table has same data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users_backup ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in backup")

	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.Equal(t, "charlie@example.com", records[2]["email"])

	// Verify schema was copied
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users_backup", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasName := fields["name"]
	_, hasEmail := fields["email"]
	assert.True(t, hasName, "name field should exist")
	assert.True(t, hasEmail, "email field should exist")
}

func TestCopyTable_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_source SCHEMAFULL;
		DEFINE FIELD name ON empty_source TYPE option<string>;
		DEFINE FIELD amount ON empty_source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Copy empty table
	err = migrator.CopyTable(ctx, namespace, "empty_source", "empty_dest")
	require.NoError(t, err, "CopyTable on empty table should not fail")

	// Verify destination table exists and is empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_dest", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Destination table should be empty")

	// Verify schema was copied
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_dest", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasName := fields["name"]
	_, hasAmount := fields["amount"]
	assert.True(t, hasName, "name field should exist")
	assert.True(t, hasAmount, "amount field should exist")
}

func TestCopyTable_MultipleDataTypes(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with multiple data types
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD stock ON products TYPE option<int>;
		DEFINE FIELD active ON products TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', price = 9.99, stock = 100, active = true;
		CREATE products:2 SET name = 'Product B', price = 19.99, stock = 50, active = false;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy table
	err = migrator.CopyTable(ctx, namespace, "products", "products_copy")
	require.NoError(t, err, "CopyTable failed")

	// Verify data was copied with correct types
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products_copy ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify first record
	assert.Equal(t, "Product A", records[0]["name"])
	assert.Equal(t, float64(9.99), records[0]["price"])
	assert.Equal(t, uint64(100), records[0]["stock"])
	assert.Equal(t, true, records[0]["active"])

	// Verify second record
	assert.Equal(t, "Product B", records[1]["name"])
	assert.Equal(t, float64(19.99), records[1]["price"])
	assert.Equal(t, uint64(50), records[1]["stock"])
	assert.Equal(t, false, records[1]["active"])
}

func TestCopyTable_WithNoneValues(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with optional fields
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD notes ON orders TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with some NONE values
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:1 SET customer = 'Alice', notes = 'Priority';
		CREATE orders:2 SET customer = 'Bob';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy table
	err = migrator.CopyTable(ctx, namespace, "orders", "orders_copy")
	require.NoError(t, err, "CopyTable failed")

	// Verify NONE values are preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders_copy ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// First record has notes
	assert.Equal(t, "Alice", records[0]["customer"])
	assert.Equal(t, "Priority", records[0]["notes"])

	// Second record has NONE for notes
	assert.Equal(t, "Bob", records[1]["customer"])
	_, hasNotes := records[1]["notes"]
	assert.False(t, hasNotes, "Second record should not have notes field")
}

func TestCopyTable_SourceTableNotModified(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item 1';
		CREATE items:2 SET name = 'Item 2';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy table
	err = migrator.CopyTable(ctx, namespace, "items", "items_copy")
	require.NoError(t, err, "CopyTable failed")

	// Verify source table is unchanged
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Source table should still have 2 records")

	assert.Equal(t, "Item 1", records[0]["name"])
	assert.Equal(t, "Item 2", records[1]["name"])
}
