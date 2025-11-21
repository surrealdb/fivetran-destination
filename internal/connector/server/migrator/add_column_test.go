package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

func TestAddColumnWithDefaultValue_BasicAdd(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with existing data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert existing records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:2 SET name = 'Bob', _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Add new column with default value
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "users", "age", pb.DataType_INT, "25")
	require.NoError(t, err, "AddColumnWithDefaultValue failed")

	// Verify all records have the new column with default value
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.Equal(t, uint64(25), record["age"], "Record %d should have age = 25", i)
	}

	// Verify field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasAgeField := fields["age"]
	assert.True(t, hasAgeField, "age field should exist in schema")
}

func TestAddColumnWithDefaultValue_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_users SCHEMAFULL;
		DEFINE FIELD name ON empty_users TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Add column to empty table
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "empty_users", "status", pb.DataType_STRING, "active")
	require.NoError(t, err, "AddColumnWithDefaultValue on empty table should not fail")

	// Verify field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasStatusField := fields["status"]
	assert.True(t, hasStatusField, "status field should exist in schema")
}

func TestAddColumnWithDefaultValue_StringColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with existing data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert existing records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item 1';
		CREATE items:2 SET name = 'Item 2';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Add string column with default value
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "items", "category", pb.DataType_STRING, "uncategorized")
	require.NoError(t, err, "AddColumnWithDefaultValue failed")

	// Verify all records have the new column with default value
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.Equal(t, "uncategorized", record["category"], "Record %d should have category = uncategorized", i)
	}
}

func TestAddColumnWithDefaultValue_FloatColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with existing data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert existing records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A';
		CREATE products:2 SET name = 'Product B';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Add float column with default value
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "products", "price", pb.DataType_FLOAT, "9.99")
	require.NoError(t, err, "AddColumnWithDefaultValue failed")

	// Verify all records have the new column with default value
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.Equal(t, float64(9.99), record["price"], "Record %d should have price = 9.99", i)
	}
}

func TestAddColumnWithDefaultValue_BooleanColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with existing data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE flags SCHEMAFULL;
		DEFINE FIELD name ON flags TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert existing records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE flags:1 SET name = 'Flag 1';
		CREATE flags:2 SET name = 'Flag 2';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Add boolean column with default value
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "flags", "enabled", pb.DataType_BOOLEAN, "true")
	require.NoError(t, err, "AddColumnWithDefaultValue failed")

	// Verify all records have the new column with default value
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM flags ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.Equal(t, true, record["enabled"], "Record %d should have enabled = true", i)
	}
}

func TestAddColumnWithDefaultValue_PreservesExistingData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD amount ON orders TYPE option<float>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert existing records with different values
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:1 SET customer = 'Alice', amount = 100.50;
		CREATE orders:2 SET customer = 'Bob', amount = 200.75;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Add new column with default value
	err = migrator.AddColumnWithDefaultValue(ctx, namespace, "orders", "status", pb.DataType_STRING, "pending")
	require.NoError(t, err, "AddColumnWithDefaultValue failed")

	// Verify existing data is preserved and new column has default value
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify first record
	assert.Equal(t, "Alice", records[0]["customer"])
	assert.Equal(t, float32(100.50), records[0]["amount"])
	assert.Equal(t, "pending", records[0]["status"])

	// Verify second record
	assert.Equal(t, "Bob", records[1]["customer"])
	assert.Equal(t, float32(200.75), records[1]["amount"])
	assert.Equal(t, "pending", records[1]["status"])
}
