package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestDropColumn_BasicDrop(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD age ON users TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com', age = 30;
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com', age = 25;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Drop the age column
	err = migrator.DropColumn(ctx, namespace, "users", "age")
	require.NoError(t, err, "DropColumn failed")

	// Verify column was removed from schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasAge := fields["age"]
	assert.False(t, hasAge, "age field should not exist in schema")
	_, hasName := fields["name"]
	assert.True(t, hasName, "name field should still exist")
	_, hasEmail := fields["email"]
	assert.True(t, hasEmail, "email field should still exist")

	// Verify data no longer has age column
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		_, hasAgeValue := record["age"]
		assert.False(t, hasAgeValue, "Record %d should not have age value", i)
		assert.Contains(t, record, "name", "Record %d should still have name", i)
		assert.Contains(t, record, "email", "Record %d should still have email", i)
	}
}

func TestDropColumn_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_users SCHEMAFULL;
		DEFINE FIELD name ON empty_users TYPE option<string>;
		DEFINE FIELD status ON empty_users TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Drop column on empty table
	err = migrator.DropColumn(ctx, namespace, "empty_users", "status")
	require.NoError(t, err, "DropColumn on empty table should not fail")

	// Verify column was removed from schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasStatus := fields["status"]
	assert.False(t, hasStatus, "status field should not exist in schema")
	_, hasName := fields["name"]
	assert.True(t, hasName, "name field should still exist")
}

func TestDropColumn_PreservesOtherData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD stock ON products TYPE option<int>;
		DEFINE FIELD category ON products TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', price = 9.99, stock = 100, category = 'electronics';
		CREATE products:2 SET name = 'Product B', price = 19.99, stock = 50, category = 'clothing';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Drop the category column
	err = migrator.DropColumn(ctx, namespace, "products", "category")
	require.NoError(t, err, "DropColumn failed")

	// Verify other data is preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify first record
	assert.Equal(t, "Product A", records[0]["name"])
	assert.Equal(t, float64(9.99), records[0]["price"])
	assert.Equal(t, uint64(100), records[0]["stock"])
	_, hasCategory := records[0]["category"]
	assert.False(t, hasCategory, "category should be removed")

	// Verify second record
	assert.Equal(t, "Product B", records[1]["name"])
	assert.Equal(t, float64(19.99), records[1]["price"])
	assert.Equal(t, uint64(50), records[1]["stock"])
	_, hasCategory2 := records[1]["category"]
	assert.False(t, hasCategory2, "category should be removed")
}

func TestDropColumn_WithNoneValues(t *testing.T) {
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
		CREATE orders:1 SET customer = 'Alice', notes = 'Priority';
		CREATE orders:2 SET customer = 'Bob';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Drop the notes column
	err = migrator.DropColumn(ctx, namespace, "orders", "notes")
	require.NoError(t, err, "DropColumn failed")

	// Verify notes column is removed
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		_, hasNotes := record["notes"]
		assert.False(t, hasNotes, "Record %d should not have notes", i)
		assert.Contains(t, record, "customer", "Record %d should still have customer", i)
	}
}
