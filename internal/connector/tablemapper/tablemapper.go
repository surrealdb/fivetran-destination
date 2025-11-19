package tablemapper

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/surrealdb/fivetran-destination/internal/connector/log"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
)

// TableMapper handles table definition and reading operations for SurrealDB tables.
type TableMapper struct {
	db      *surrealdb.DB
	logging *log.Logging
}

// New creates a new TableMapper with the given database connection and logging.
func New(db *surrealdb.DB, logging *log.Logging) *TableMapper {
	return &TableMapper{
		db:      db,
		logging: logging,
	}
}

// LogInfo logs an informational message.
func (tm *TableMapper) LogInfo(msg string, fields ...interface{}) {
	tm.logging.LogInfo(msg, fields...)
}

// LogDebug logs a debug message.
func (tm *TableMapper) LogDebug(msg string, fields ...interface{}) {
	tm.logging.LogDebug(msg, fields...)
}

// LogWarning logs a warning message.
func (tm *TableMapper) LogWarning(msg string, err error) {
	tm.logging.LogWarning(msg, err)
}

// Debugging returns true if debug logging is enabled.
func (tm *TableMapper) Debugging() bool {
	return tm.logging.Debugging()
}

// TableInfo contains information about a table's structure.
type TableInfo struct {
	Columns []ColumnInfo
}

// ColumnInfo contains information about a column in a table.
type ColumnInfo struct {
	Name       string
	SDBType    string
	PrimaryKey bool
	Optional   bool
	ColumnMeta
}

// StrToSurrealType converts a string value to the appropriate SurrealDB type.
func (c *ColumnInfo) StrToSurrealType(v string) (interface{}, error) {
	tpe := FindTypeMappingByColumnInfo(c)
	if tpe == nil {
		return nil, fmt.Errorf("converting value: unsupported data type for column %s: surrealdb type %s, fivetran type %s", c.Name, c.SDBType, c.FtType)
	}
	return tpe.SurrealType(v)
}

// ColumnMeta is the metadata for a field in a table.
// It is used to store information like the Fivetran column index and type,
// that can not be represented directly in the SurrealDB schema.
type ColumnMeta struct {
	// The column index in the Fivetran schema.
	// This is used to map the SurrealDB field to the correct index in the Fivetran schema.
	FtIndex int `json:"ft_index"`
	// The data type of the column in the Fivetran schema.
	// This is used to map the SurrealDB field to the correct data type in the Fivetran schema,
	// even in case the type cannot be directly represented in the SurrealDB schema.
	FtType pb.DataType `json:"ft_data_type"`

	// DecimalPrecision is the precision for decimal types.
	// It is only set when the FtType is pb.DataType_DECIMAL.
	// This is used for deciding which SurrealDB type to use for the decimal column.
	// SurrealDB's decimal is decimal128 powered by rust_decimal,
	// whose max value is:
	//   79_228_162_514_264_337_593_543_950_335
	// which has 29 decimal digits.
	// As any 29 decimal digits that presents larger value than that
	// will be an error in SurrealDB side, we fall back to
	// using float type in case the precision is larger than 28 (to be safe).
	DecimalPrecision uint32 `json:"decimal_precision,omitempty"`
}

// ErrTableNotFound is returned when a table is not found.
var ErrTableNotFound = fmt.Errorf("table not found")

// InfoForTable retrieves information about a table's structure.
func (tm *TableMapper) InfoForTable(ctx context.Context, tableName string) (TableInfo, error) {
	// the result is formatted like:
	// {
	// 	"events": {},
	// 	"fields": {
	// 		"name": "DEFINE FIELD name ON user TYPE string PERMISSIONS FULL"
	// 	},
	// 	"indexes": {},
	// 	"lives": {},
	// 	"tables": {}
	// }
	type InfoForTableResult struct {
		Fields map[string]string `json:"fields"`
	}

	if err := ValidateTableName(tableName); err != nil {
		return TableInfo{}, err
	}

	query := fmt.Sprintf(`INFO FOR TABLE %s;`, tableName)

	info, err := surrealdb.Query[InfoForTableResult](ctx, tm.db, query, nil)
	if err != nil {
		return TableInfo{}, err
	}

	if len(*info) == 0 {
		return TableInfo{}, ErrTableNotFound
	}

	first := (*info)[0]

	fields := first.Result.Fields

	if tm.Debugging() {
		tm.LogDebug("INFO FOR TABLE", "table", tableName, "fields", fields)
	}

	columns := []ColumnInfo{}

	for _, field := range fields {
		field = strings.TrimPrefix(field, "DEFINE FIELD ")
		s := strings.Split(field, " ")
		name := s[0]
		rr := strings.Split(field, " TYPE ")
		tpe := strings.Split(rr[1], " ")[0]

		var meta ColumnMeta
		if strings.Contains(field, "COMMENT '") {
			comment := strings.Split(field, "COMMENT '")[1]
			comment = strings.Split(comment, "'")[0]
			err := json.Unmarshal([]byte(comment), &meta)
			if err != nil {
				return TableInfo{}, fmt.Errorf("failed to unmarshal comment %s for field %s: %v", comment, name, err)
			}
		}

		// `DEFINE FIELD upper.* ON table TYPE any;`
		// ends up with a field name like `upper[*]`
		// upper[*] are for any nested fields in the `upper` object field
		// and does not need to be mapped to a Fivetran column.
		// What's why we skip them here.
		if strings.HasSuffix(name, "[*]") {
			if tpe != "any" {
				return TableInfo{}, fmt.Errorf("unexpected type for field %s: %s", name, tpe)
			}
			continue
		}

		var optional bool
		if strings.HasPrefix(tpe, "option<") {
			tpe = strings.TrimPrefix(tpe, "option<")
			tpe = strings.TrimSuffix(tpe, ">")
			optional = true
		}

		columns = append(columns, ColumnInfo{
			Name:       strings.ReplaceAll(name, "`", ""),
			SDBType:    tpe,
			Optional:   optional,
			ColumnMeta: meta,
		})
	}

	if tm.Debugging() {
		tm.LogDebug("Ran info for table", "table", tableName, "columns", columns)
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].FtIndex < columns[j].FtIndex
	})

	return TableInfo{
		Columns: columns,
	}, nil
}

// ColumnsFromSurrealToFivetran converts SurrealDB column info to Fivetran columns.
func ColumnsFromSurrealToFivetran(sColumns []ColumnInfo) ([]*pb.Column, error) {
	var ftColumns []*pb.Column

	for _, c := range sColumns {
		ftColumns = append(ftColumns, &pb.Column{
			Name: c.Name,
			Type: c.FtType,
		})
	}

	return ftColumns, nil
}

// DefineFivetranStartFieldIndex generates the query to define an index on _fivetran_start.
func DefineFivetranStartFieldIndex(tb string) (string, error) {
	return fmt.Sprintf(`DEFINE INDEX %s ON %s FIELDS _fivetran_start;`, tb, tb), nil
}

// DefineFivetranPKIndex generates the query to define an index on primary key columns.
func DefineFivetranPKIndex(table *pb.Table) (string, error) {
	var pkFields []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkFields = append(pkFields, c.Name)
		}
	}
	if len(pkFields) == 0 {
		return "", fmt.Errorf("no primary key columns found for table %s", table.Name)
	}
	return fmt.Sprintf(`DEFINE INDEX %s_pkcol ON %s FIELDS %s;`, table.Name, table.Name, strings.Join(pkFields, ", ")), nil
}

// DefineTable defines a table and its fields in SurrealDB.
func (tm *TableMapper) DefineTable(ctx context.Context, table *pb.Table) error {
	var rpcRes connection.RPCResponse[any]
	if err := ValidateTableName(table.Name); err != nil {
		return err
	}
	tb := table.Name
	query := fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMAFULL;`, tb)
	if err := surrealdb.Send(ctx, tm.db, &rpcRes, "query", query); err != nil {
		return err
	}

	tm.LogInfo("Defined table", "table", tb, "query", query)

	var (
		historyMode bool
	)

	// We assume the table will be written using WriteHistoryBatch
	// if the table has a _fivetran_start column.
	for _, c := range table.Columns {
		if c.Name == "_fivetran_start" {
			historyMode = true
			break
		}
	}

	for i, c := range table.Columns {
		if c.Name == "id" {
			if tm.Debugging() {
				tm.LogDebug("Skipping id")
			}
			// We treat it specially since it's the primary key column in SurrealDB,
			// which needs to be the primary id type itself when soft-delete(non-history) mode,
			// while in history mode it can be a composite key of (the id type assuming its pk, _fivetran_start).
			q, err := DefineFieldQueryForHistoryModeIDFromFt(tb, c, i)
			if err != nil {
				return err
			}
			if err := surrealdb.Send(ctx, tm.db, &rpcRes, "query", q); err != nil {
				return err
			}
			if tm.Debugging() {
				tm.LogDebug("Defined field", "field", c.Name, "table", tb, "query", q)
			}
			continue
		}

		if err := ValidateColumnName(c.Name); err != nil {
			return err
		}
		q, err := DefineFieldQueryFromFt(tb, c, i)
		if err != nil {
			return err
		}
		if err := surrealdb.Send(ctx, tm.db, &rpcRes, "query", q); err != nil {
			return err
		}
		if tm.Debugging() {
			tm.LogDebug("Defined field", "field", c.Name, "table", tb, "query", q)
		}
	}

	if historyMode {
		q, err := DefineFivetranStartFieldIndex(tb)
		if err != nil {
			return err
		}
		if err := surrealdb.Send(ctx, tm.db, &rpcRes, "query", q); err != nil {
			return err
		}
		if tm.Debugging() {
			tm.LogDebug("Defined fivetran_start field index", "table", tb, "query", q)
		}

		q, err = DefineFivetranPKIndex(table)
		if err != nil {
			return err
		}
		if err := surrealdb.Send(ctx, tm.db, &rpcRes, "query", q); err != nil {
			return err
		}
		if tm.Debugging() {
			tm.LogDebug("Defined pkcol index", "table", tb, "query", q)
		}
	}
	return nil
}
