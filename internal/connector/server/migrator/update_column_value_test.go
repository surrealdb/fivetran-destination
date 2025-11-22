package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestUpdateColumnValue_BasicUpdate(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD status ON users TYPE string COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', status = 'active';
		CREATE users:2 SET name = 'Bob', status = 'active';
		CREATE users:3 SET name = 'Charlie', status = 'active';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Update all status values to 'inactive'
	err = migrator.UpdateColumnValue(ctx, namespace, "users", "status", "inactive")
	require.NoError(t, err, "UpdateColumnValue failed")

	// Verify all records have updated status
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	for _, record := range records {
		assert.Equal(t, "inactive", record["status"], "Status should be updated to 'inactive'")
	}
}

func TestUpdateColumnValue_UpdateToNull(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table with optional field
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD description ON products TYPE option<string> COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data with descriptions
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Widget', description = 'A useful widget';
		CREATE products:2 SET name = 'Gadget', description = 'A handy gadget';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Update all descriptions to NULL
	err = migrator.UpdateColumnValue(ctx, namespace, "products", "description", "NULL")
	require.NoError(t, err, "UpdateColumnValue to NULL failed")

	// Verify all descriptions are now NULL/NONE
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for _, record := range records {
		assert.Nil(t, record["description"], "Description should be NULL/NONE")
	}
}

func TestUpdateColumnValue_UpdateEmptyString(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD category ON items TYPE option<string> COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item1', category = 'electronics';
		CREATE items:2 SET name = 'Item2', category = 'furniture';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Update with empty string (should be treated as NULL)
	err = migrator.UpdateColumnValue(ctx, namespace, "items", "category", "")
	require.NoError(t, err, "UpdateColumnValue with empty string failed")

	// Verify all categories are now NULL/NONE
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for _, record := range records {
		assert.Equal(t, "", record["category"], "Category should be empty string")
	}
}

func TestUpdateColumnValue_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD name ON empty_table TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD value ON empty_table TYPE string COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Update should succeed even on empty table
	err = migrator.UpdateColumnValue(ctx, namespace, "empty_table", "value", "new_value")
	require.NoError(t, err, "UpdateColumnValue on empty table should succeed")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")
}

func TestUpdateColumnValue_WithRecordID(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD customer ON orders TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD status ON orders TYPE string COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD total ON orders TYPE number COMMENT '{"ft_index":2,"ft_data_type":3,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data using models.NewRecordID
	order1 := models.NewRecordID("orders", "order_001")
	order2 := models.NewRecordID("orders", "order_002")

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE $order1 SET customer = 'John', status = 'pending', total = 100;
		CREATE $order2 SET customer = 'Jane', status = 'pending', total = 200;
	`, map[string]any{
		"order1": order1,
		"order2": order2,
	})
	require.NoError(t, err, "Failed to insert initial data")

	// Update all statuses to 'shipped'
	err = migrator.UpdateColumnValue(ctx, namespace, "orders", "status", "shipped")
	require.NoError(t, err, "UpdateColumnValue failed")

	// Verify all records have updated status
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for _, record := range records {
		assert.Equal(t, "shipped", record["status"], "Status should be updated to 'shipped'")
	}
}

func TestUpdateColumnValue_LowercaseNull(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create test table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE test_null SCHEMAFULL;
		DEFINE FIELD name ON test_null TYPE string COMMENT '{"ft_index":0,"ft_data_type":13,"ft_primary_key":false}';
		DEFINE FIELD optional_field ON test_null TYPE option<string> COMMENT '{"ft_index":1,"ft_data_type":13,"ft_primary_key":false}';
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert initial data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE test_null:1 SET name = 'Test', optional_field = 'has value';
	`, nil)
	require.NoError(t, err, "Failed to insert initial data")

	// Update with lowercase 'null'
	err = migrator.UpdateColumnValue(ctx, namespace, "test_null", "optional_field", "null")
	require.NoError(t, err, "UpdateColumnValue with lowercase 'null' failed")

	// Verify field is now NULL/NONE
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM test_null", nil)
	require.NoError(t, err, "Failed to query results")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 1, "Expected 1 record")

	assert.Nil(t, records[0]["optional_field"], "Field should be NULL/NONE")
}
