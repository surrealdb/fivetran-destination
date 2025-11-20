package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestRenameTable_BasicRename(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE old_users SCHEMAFULL;
		DEFINE FIELD name ON old_users TYPE string;
		DEFINE FIELD email ON old_users TYPE string;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE old_users:1 SET name = 'Alice', email = 'alice@example.com';
		CREATE old_users:2 SET name = 'Bob', email = 'bob@example.com';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Rename table
	err = migrator.RenameTable(ctx, namespace, "", "old_users", "new_users")
	require.NoError(t, err, "RenameTable failed")

	// Verify old table no longer exists
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM old_users", nil)
	require.NoError(t, err, "Failed to query new table")
	require.NotEmpty(t, *results, "Query results should not be empty")
	require.Empty(t, (*results)[0].Result, "Select query should be empty")

	// Verify new table exists with data
	results, err = surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM new_users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query new table")
	require.NotNil(t, results, "Query results is nil")
	require.NotEmpty(t, *results, "Query results is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records in new table")

	// Verify data integrity
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])
}

func TestRenameTable_WithIndexes(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table with indexes
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD sku ON products TYPE string;
		DEFINE FIELD name ON products TYPE string;
		DEFINE FIELD price ON products TYPE number;
		DEFINE INDEX products_sku ON products FIELDS sku;
		DEFINE INDEX products_name ON products FIELDS name;
	`, nil)
	require.NoError(t, err, "Failed to create table with indexes")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET sku = 'SKU001', name = 'Widget', price = 9.99;
		CREATE products:2 SET sku = 'SKU002', name = 'Gadget', price = 19.99;
		CREATE products:3 SET sku = 'SKU003', name = 'Doodad', price = 29.99;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename table
	err = migrator.RenameTable(ctx, namespace, "", "products", "items")
	require.NoError(t, err, "RenameTable with indexes failed")

	// Verify new table has data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err, "Failed to query new table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in new table")

	// Verify data
	assert.Equal(t, "SKU001", records[0]["sku"])
	assert.Equal(t, "Widget", records[0]["name"])

	// Verify indexes exist on new table by checking INFO FOR TABLE
	type InfoForTableResult struct {
		Indexes map[string]string `cbor:"indexes"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE items", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	indexes := (*infoResults)[0].Result.Indexes
	require.NotNil(t, indexes, "Indexes should not be nil")

	// Check that indexes were recreated with new table name
	_, hasSku := indexes["products_sku"]
	_, hasName := indexes["products_name"]
	assert.True(t, hasSku, "Index products_sku should exist on new table")
	assert.True(t, hasName, "Index products_name should exist on new table")
}

func TestRenameTable_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_old SCHEMAFULL;
		DEFINE FIELD value ON empty_old TYPE string;
	`, nil)
	require.NoError(t, err, "Failed to create empty table")

	// Rename empty table
	err = migrator.RenameTable(ctx, namespace, "", "empty_old", "empty_new")
	require.NoError(t, err, "RenameTable on empty table failed")

	// Verify new table exists using INFO FOR TABLE
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_new", nil)
	require.NoError(t, err, "Failed to get table info for new table")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	// Verify field was copied
	fields := (*infoResults)[0].Result.Fields
	require.NotEmpty(t, fields, "Fields should not be empty")
	// The table should have the 'value' field defined
	assert.Len(t, fields, 1, "Should have exactly 1 field")
}

func TestRenameTable_WithMultipleDataTypes(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with various data types
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE old_mixed SCHEMAFULL;
		DEFINE FIELD name ON old_mixed TYPE string;
		DEFINE FIELD count ON old_mixed TYPE int;
		DEFINE FIELD price ON old_mixed TYPE float;
		DEFINE FIELD active ON old_mixed TYPE bool;
		DEFINE FIELD description ON old_mixed TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table with multiple types")

	// Insert data with various types
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE old_mixed:1 SET name = 'Item1', count = 10, price = 19.99, active = true, description = 'A description';
		CREATE old_mixed:2 SET name = 'Item2', count = 20, price = 29.99, active = false, description = NONE;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename table
	err = migrator.RenameTable(ctx, namespace, "", "old_mixed", "new_mixed")
	require.NoError(t, err, "RenameTable with multiple types failed")

	// Verify data in new table
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM new_mixed ORDER BY id", nil)
	require.NoError(t, err, "Failed to query new table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify first record
	assert.Equal(t, "Item1", records[0]["name"])
	assert.Equal(t, uint64(10), records[0]["count"])
	assert.Equal(t, true, records[0]["active"])
	assert.Equal(t, "A description", records[0]["description"])

	// Verify second record with nil description
	assert.Equal(t, "Item2", records[1]["name"])
	assert.Equal(t, uint64(20), records[1]["count"])
	assert.Equal(t, false, records[1]["active"])
	assert.Nil(t, records[1]["description"])
}

func TestRenameTable_WithFieldDefaults(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with default values
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE old_defaults SCHEMAFULL;
		DEFINE FIELD name ON old_defaults TYPE string;
		DEFINE FIELD status ON old_defaults TYPE string DEFAULT 'pending';
	`, nil)
	require.NoError(t, err, "Failed to create table with defaults")

	// Insert data (status will use default)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE old_defaults:1 SET name = 'Task1';
		CREATE old_defaults:2 SET name = 'Task2', status = 'completed';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename table
	err = migrator.RenameTable(ctx, namespace, "", "old_defaults", "new_defaults")
	require.NoError(t, err, "RenameTable with defaults failed")

	// Verify data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM new_defaults ORDER BY id", nil)
	require.NoError(t, err, "Failed to query new table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	assert.Equal(t, "Task1", records[0]["name"])
	assert.Equal(t, "pending", records[0]["status"])
	assert.Equal(t, "Task2", records[1]["name"])
	assert.Equal(t, "completed", records[1]["status"])
}

func TestRenameTable_LargeDataset(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE old_large SCHEMAFULL;
		DEFINE FIELD value ON old_large TYPE int;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert multiple records
	for i := 1; i <= 100; i++ {
		_, err = surrealdb.Query[any](ctx, db, "CREATE old_large SET value = $value", map[string]any{
			"value": i,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Rename table
	err = migrator.RenameTable(ctx, namespace, "", "old_large", "new_large")
	require.NoError(t, err, "RenameTable with large dataset failed")

	// Verify count by selecting all records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM new_large", nil)
	require.NoError(t, err, "Failed to query records")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	assert.Len(t, records, 100, "Expected 100 records in new table")
}
