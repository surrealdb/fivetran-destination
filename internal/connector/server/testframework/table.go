package testframework

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// TruncateTable removes all records from a table
func TruncateTable(t *testing.T, config map[string]string, namespace, database, tableName string) {
	ctx := t.Context()
	db, err := ConnectAndUse(ctx, config["url"], namespace, database, config["user"], config["pass"])
	require.NoError(t, err, "Failed to connect to database for truncate")
	defer db.Close(ctx)

	_, err = surrealdb.Query[any](ctx, db, fmt.Sprintf("DELETE FROM %s;", tableName), nil)
	require.NoError(t, err, "Failed to truncate table")
}

// DropTable removes a table from the database
func DropTable(t *testing.T, config map[string]string, namespace, database, tableName string) {
	ctx := t.Context()
	db, err := ConnectAndUse(ctx, config["url"], namespace, database, config["user"], config["pass"])
	require.NoError(t, err, "Failed to connect to database for drop")
	defer db.Close(ctx)

	_, err = surrealdb.Query[any](ctx, db, fmt.Sprintf("REMOVE TABLE IF EXISTS %s;", tableName), nil)
	require.NoError(t, err, "Failed to drop table")
}

// NewTableDefinition creates a pb.Table definition from column specifications
// columns is a map of column name to DataType
// primaryKeys is a slice of column names that form the primary key
func NewTableDefinition(name string, columns map[string]pb.DataType, primaryKeys []string) *pb.Table {
	table := &pb.Table{
		Name:    name,
		Columns: make([]*pb.Column, 0, len(columns)),
	}

	// Create a set of primary key column names for quick lookup
	for _, pk := range primaryKeys {
		colType, ok := columns[pk]
		if !ok {
			panic(fmt.Sprintf("primary key column %s not found in columns map", pk))
		}
		delete(columns, pk)
		col := &pb.Column{
			Name:       pk,
			Type:       colType,
			PrimaryKey: true,
		}
		table.Columns = append(table.Columns, col)
	}

	// Add columns
	for colName, colType := range columns {
		col := &pb.Column{
			Name:       colName,
			Type:       colType,
			PrimaryKey: false,
		}
		table.Columns = append(table.Columns, col)
	}

	return table
}

// NewTableDefinitionWithParams creates a pb.Table with columns that have params (e.g., DECIMAL)
type ColumnDef struct {
	Name       string
	Type       pb.DataType
	PrimaryKey bool
	Params     *pb.DataTypeParams
}

func NewTableDefinitionWithParams(name string, columns []ColumnDef) *pb.Table {
	table := &pb.Table{
		Name:    name,
		Columns: make([]*pb.Column, 0, len(columns)),
	}

	for _, colDef := range columns {
		col := &pb.Column{
			Name:       colDef.Name,
			Type:       colDef.Type,
			PrimaryKey: colDef.PrimaryKey,
		}
		if colDef.Params != nil {
			col.Params = colDef.Params
		}
		table.Columns = append(table.Columns, col)
	}

	return table
}
