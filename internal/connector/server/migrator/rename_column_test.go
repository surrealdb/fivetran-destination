package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestRenameColumn_BasicRename(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD old_name ON users TYPE string;
		DEFINE FIELD email ON users TYPE string;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET old_name = 'Alice', email = 'alice@example.com';
		CREATE users:2 SET old_name = 'Bob', email = 'bob@example.com';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Rename column
	err = migrator.RenameColumn(ctx, namespace, "users", "old_name", "new_name")
	require.NoError(t, err, "RenameColumn failed")

	// Verify old column no longer exists and new column has data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify new column has data
	assert.Equal(t, "Alice", records[0]["new_name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["new_name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])

	// Verify old column doesn't exist
	assert.NotContains(t, records[0], "old_name")
	assert.NotContains(t, records[1], "old_name")
}

func TestRenameColumn_WithOptionalType(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with optional field
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE string;
		DEFINE FIELD old_description ON products TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with some null values
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Widget', old_description = 'A widget';
		CREATE products:2 SET name = 'Gadget', old_description = NONE;
		CREATE products:3 SET name = 'Doodad', old_description = 'A doodad';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename column
	err = migrator.RenameColumn(ctx, namespace, "products", "old_description", "new_description")
	require.NoError(t, err, "RenameColumn with optional type failed")

	// Verify data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	// Verify values including null
	assert.Equal(t, "A widget", records[0]["new_description"])
	assert.Nil(t, records[1]["new_description"])
	assert.Equal(t, "A doodad", records[2]["new_description"])

	// Verify old column doesn't exist
	assert.NotContains(t, records[0], "old_description")
}

func TestRenameColumn_WithDefaultValue(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with field that has a default value
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE tasks SCHEMAFULL;
		DEFINE FIELD title ON tasks TYPE string;
		DEFINE FIELD old_status ON tasks TYPE string DEFAULT 'pending';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE tasks:1 SET title = 'Task 1';
		CREATE tasks:2 SET title = 'Task 2', old_status = 'completed';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename column
	err = migrator.RenameColumn(ctx, namespace, "tasks", "old_status", "new_status")
	require.NoError(t, err, "RenameColumn with default value failed")

	// Verify data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM tasks ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify the default value was preserved
	assert.Equal(t, "pending", records[0]["new_status"])
	assert.Equal(t, "completed", records[1]["new_status"])
}

func TestRenameColumn_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD old_field ON empty_table TYPE string;
	`, nil)
	require.NoError(t, err, "Failed to create empty table")

	// Rename column on empty table
	err = migrator.RenameColumn(ctx, namespace, "empty_table", "old_field", "new_field")
	require.NoError(t, err, "RenameColumn on empty table failed")

	// Verify field was renamed using INFO FOR TABLE
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_table", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	require.NotEmpty(t, fields, "Fields should not be empty")

	// Verify new field exists and old field doesn't
	_, hasNewField := fields["new_field"]
	_, hasOldField := fields["old_field"]
	assert.True(t, hasNewField, "New field should exist")
	assert.False(t, hasOldField, "Old field should not exist")
}

func TestRenameColumn_WithMultipleDataTypes(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple columns of different types
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE mixed SCHEMAFULL;
		DEFINE FIELD old_count ON mixed TYPE int;
		DEFINE FIELD price ON mixed TYPE float;
		DEFINE FIELD active ON mixed TYPE bool;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE mixed:1 SET old_count = 10, price = 19.99, active = true;
		CREATE mixed:2 SET old_count = 20, price = 29.99, active = false;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename the int column
	err = migrator.RenameColumn(ctx, namespace, "mixed", "old_count", "new_count")
	require.NoError(t, err, "RenameColumn with int type failed")

	// Verify data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM mixed ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify int values were preserved
	assert.Equal(t, uint64(10), records[0]["new_count"])
	assert.Equal(t, uint64(20), records[1]["new_count"])

	// Verify other columns remain intact
	assert.Equal(t, true, records[0]["active"])
	assert.Equal(t, false, records[1]["active"])
}

func TestRenameColumn_NonExistentColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE test_table SCHEMAFULL;
		DEFINE FIELD existing_field ON test_table TYPE string;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Try to rename non-existent column
	err = migrator.RenameColumn(ctx, namespace, "test_table", "non_existent", "new_name")
	require.Error(t, err, "Should fail when renaming non-existent column")
	assert.Contains(t, err.Error(), "not found", "Error should mention column not found")
}

func TestRenameColumn_WithComments(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with field that has a COMMENT (used for metadata)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD old_sku ON items TYPE string COMMENT '{"metadata": "test"}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET old_sku = 'SKU001';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Rename column
	err = migrator.RenameColumn(ctx, namespace, "items", "old_sku", "new_sku")
	require.NoError(t, err, "RenameColumn with COMMENT failed")

	// Verify data
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 1, "Expected 1 record")

	assert.Equal(t, "SKU001", records[0]["new_sku"])

	// Verify the COMMENT was preserved
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE items", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	newFieldDef, hasNewField := fields["new_sku"]
	require.True(t, hasNewField, "New field should exist")
	assert.Contains(t, newFieldDef, "COMMENT", "COMMENT should be preserved")
}
