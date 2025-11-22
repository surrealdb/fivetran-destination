package server

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// setupDescribeTableTest creates a test environment for DescribeTable tests
func setupDescribeTableTest(t *testing.T) (*Server, map[string]string, string, func()) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	schema := "test_describetable"

	cleanup := func() {
		// Test-specific cleanup if needed
	}

	return srv, config, schema, cleanup
}

// buildBasicTable creates a simple table definition for testing
func buildBasicTable() *pb.Table {
	return testframework.NewTableDefinition("users", map[string]pb.DataType{
		"id":     pb.DataType_STRING,
		"name":   pb.DataType_STRING,
		"age":    pb.DataType_INT,
		"active": pb.DataType_BOOLEAN,
	}, []string{"id"})
}

// buildAllTypesTable creates a table with all supported data types
func buildAllTypesTable() *pb.Table {
	return &pb.Table{
		Name: "all_types",
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_STRING, PrimaryKey: true},
			{Name: "mybool", Type: pb.DataType_BOOLEAN},
			{Name: "myshort", Type: pb.DataType_SHORT},
			{Name: "myint", Type: pb.DataType_INT},
			{Name: "mylong", Type: pb.DataType_LONG},
			{
				Name: "mydecimal",
				Type: pb.DataType_DECIMAL,
				Params: &pb.DataTypeParams{
					Params: &pb.DataTypeParams_Decimal{
						Decimal: &pb.DecimalParams{
							Precision: 10,
							Scale:     2,
						},
					},
				},
			},
			{Name: "myfloat", Type: pb.DataType_FLOAT},
			{Name: "mydouble", Type: pb.DataType_DOUBLE},
			{Name: "mynaivedate", Type: pb.DataType_NAIVE_DATE},
			{Name: "mynaivedatetime", Type: pb.DataType_NAIVE_DATETIME},
			{Name: "myutcdatetime", Type: pb.DataType_UTC_DATETIME},
			{Name: "mybinary", Type: pb.DataType_BINARY},
			{Name: "myxml", Type: pb.DataType_XML},
			{Name: "mystring", Type: pb.DataType_STRING},
			{Name: "myjson", Type: pb.DataType_JSON},
			{Name: "mynaivetime", Type: pb.DataType_NAIVE_TIME},
		},
	}
}

// assertTableEquals compares two table definitions
func assertTableEquals(t *testing.T, expected, actual *pb.Table) {
	require.Equal(t, expected.Name, actual.Name, "Table name mismatch")
	require.Len(t, actual.Columns, len(expected.Columns), "Column count mismatch")

	// Create a map of expected columns for easy lookup
	expectedCols := make(map[string]*pb.Column)
	for _, col := range expected.Columns {
		expectedCols[col.Name] = col
	}

	// Verify each actual column matches expected
	for _, actualCol := range actual.Columns {
		expectedCol, exists := expectedCols[actualCol.Name]
		require.True(t, exists, "Unexpected column: %s", actualCol.Name)
		require.Equal(t, expectedCol.Type, actualCol.Type, "Type mismatch for column %s", actualCol.Name)
		require.Equal(t, expectedCol.PrimaryKey, actualCol.PrimaryKey, "Primary key mismatch for column %s", actualCol.Name)

		// For decimal types, verify parameters match
		if expectedCol.Type == pb.DataType_DECIMAL && expectedCol.Params != nil {
			require.NotNil(t, actualCol.Params, "Missing Params for decimal column %s", actualCol.Name)
			expectedDecimal := expectedCol.Params.GetDecimal()
			actualDecimal := actualCol.Params.GetDecimal()
			require.NotNil(t, expectedDecimal, "Expected decimal params for column %s", actualCol.Name)
			require.NotNil(t, actualDecimal, "Missing decimal params for column %s", actualCol.Name)
			require.Equal(t, expectedDecimal.Precision, actualDecimal.Precision, "Decimal precision mismatch for column %s", actualCol.Name)
			require.Equal(t, expectedDecimal.Scale, actualDecimal.Scale, "Decimal scale mismatch for column %s", actualCol.Name)
		}
	}
}

// Success test cases

func TestDescribeTable_SuccessBasicTable(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	table := buildBasicTable()

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Describe table
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     table.Name,
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	tableResp, ok := describeResp.Response.(*pb.DescribeTableResponse_Table)
	require.True(t, ok, "Expected DescribeTable table response")
	require.NotNil(t, tableResp.Table)

	// Verify table structure matches
	assertTableEquals(t, table, tableResp.Table)
}

func TestDescribeTable_SuccessAllDataTypes(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	table := buildAllTypesTable()

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Describe table
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     table.Name,
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	tableResp, ok := describeResp.Response.(*pb.DescribeTableResponse_Table)
	require.True(t, ok, "Expected DescribeTable table response")
	require.NotNil(t, tableResp.Table)

	// Verify all data types are correctly described
	assertTableEquals(t, table, tableResp.Table)
}

func TestDescribeTable_SuccessCompositePrimaryKey(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	// Create table with composite primary key
	table := &pb.Table{
		Name: "orders",
		Columns: []*pb.Column{
			{Name: "order_id", Type: pb.DataType_STRING, PrimaryKey: true},
			{Name: "line_item", Type: pb.DataType_INT, PrimaryKey: true},
			{Name: "product", Type: pb.DataType_STRING},
			{Name: "quantity", Type: pb.DataType_INT},
		},
	}

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Describe table
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     table.Name,
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	tableResp, ok := describeResp.Response.(*pb.DescribeTableResponse_Table)
	require.True(t, ok, "Expected DescribeTable table response")
	require.NotNil(t, tableResp.Table)

	// Verify composite primary key structure
	assertTableEquals(t, table, tableResp.Table)

	// Verify primary key columns
	pkCount := 0
	for _, col := range tableResp.Table.Columns {
		if col.PrimaryKey {
			pkCount++
		}
	}
	require.Equal(t, 2, pkCount, "Expected 2 primary key columns")
}

func TestDescribeTable_SuccessHistoryModeTable(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	// Create history mode table (with _fivetran_* columns)
	table := testframework.NewTableDefinition("history_users", map[string]pb.DataType{
		"_fivetran_id":     pb.DataType_STRING,
		"_fivetran_start":  pb.DataType_UTC_DATETIME,
		"_fivetran_end":    pb.DataType_UTC_DATETIME,
		"_fivetran_active": pb.DataType_BOOLEAN,
		"_fivetran_synced": pb.DataType_UTC_DATETIME,
		"name":             pb.DataType_STRING,
		"age":              pb.DataType_INT,
	}, []string{"_fivetran_id", "_fivetran_start"})

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Describe table
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     table.Name,
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	tableResp, ok := describeResp.Response.(*pb.DescribeTableResponse_Table)
	require.True(t, ok, "Expected DescribeTable table response")
	require.NotNil(t, tableResp.Table)

	// Verify history mode columns are present
	assertTableEquals(t, table, tableResp.Table)
}

// Failure test cases

func TestDescribeTable_FailureTableNotExists(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	// Describe non-existent table
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     "nonexistent_table",
	})

	// Should return NotFound
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	notFound, ok := describeResp.Response.(*pb.DescribeTableResponse_NotFound)
	require.True(t, ok, "Expected DescribeTable NotFound response")
	require.True(t, notFound.NotFound, "Expected NotFound to be true")
}

func TestDescribeTable_FailureInvalidConfig(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	// Use invalid configuration (bad credentials)
	invalidConfig := map[string]string{
		"url":  testframework.GetSurrealDBEndpoint(),
		"ns":   "test",
		"user": "invalid_user",
		"pass": "invalid_password",
	}

	// Describe table with invalid config
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: invalidConfig,
		SchemaName:    "test_describetable",
		TableName:     "users",
	})

	// Should fail with error response
	require.Error(t, err)
	require.NotNil(t, describeResp)

	// Check for warning response
	warning, ok := describeResp.Response.(*pb.DescribeTableResponse_Warning)
	if ok {
		require.NotEmpty(t, warning.Warning)
	} else {
		// Or NotFound response if connection fails before table lookup
		_, notFoundOk := describeResp.Response.(*pb.DescribeTableResponse_NotFound)
		require.True(t, notFoundOk, "Expected either Warning or NotFound response")
	}
}

func TestDescribeTable_FailureEmptyTableName(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	// Describe with empty table name
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     "",
	})

	// Should return error with warning response (table name validation fails)
	require.Error(t, err)
	require.NotNil(t, describeResp)
	warning, ok := describeResp.Response.(*pb.DescribeTableResponse_Warning)
	require.True(t, ok, "Expected DescribeTable Warning response for empty table name")
	require.NotEmpty(t, warning.Warning.Message)
	require.Contains(t, warning.Warning.Message, "table name is required")
}

func TestDescribeTable_FailureInvalidSchemaName(t *testing.T) {
	srv, config, _, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	// Create table in valid schema
	table := buildBasicTable()
	validSchema := "test_describetable"
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    validSchema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", validSchema, table.Name)

	// Try to describe from different/invalid schema
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    "nonexistent_schema",
		TableName:     table.Name,
	})

	// Should return NotFound (table doesn't exist in that schema)
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	notFound, ok := describeResp.Response.(*pb.DescribeTableResponse_NotFound)
	require.True(t, ok, "Expected DescribeTable NotFound response for invalid schema")
	require.True(t, notFound.NotFound, "Expected NotFound to be true")
}

func TestDescribeTable_SuccessEmptyTableWithFields(t *testing.T) {
	srv, config, schema, cleanup := setupDescribeTableTest(t)
	defer cleanup()

	table := buildBasicTable()

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Describe table that has fields but no data
	describeResp, err := srv.DescribeTable(t.Context(), &pb.DescribeTableRequest{
		Configuration: config,
		SchemaName:    schema,
		TableName:     table.Name,
	})

	// Should succeed and return table structure
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	tableResp, ok := describeResp.Response.(*pb.DescribeTableResponse_Table)
	require.True(t, ok, "Expected DescribeTable table response")
	require.NotNil(t, tableResp.Table)

	// Verify the table has the expected columns even with no data
	assert.Equal(t, table.Name, tableResp.Table.Name)
	assert.Len(t, tableResp.Table.Columns, len(table.Columns))
}
