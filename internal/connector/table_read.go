package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

type tableInfo struct {
	columns []columnInfo
}

type columnInfo struct {
	Name       string
	SDBType    string
	PrimaryKey bool
	Optional   bool
	ColumnMeta
}

func (c *columnInfo) strToSurrealType(v string) (interface{}, error) {
	for _, t := range typeMappings {
		if t.ft == c.FtType {
			return t.surrealType(v)
		}
	}
	return nil, fmt.Errorf("unsupported data type for column %s: surrealdb type %s, fivetran type %s", c.Name, c.SDBType, c.FtType)
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
}

var ErrTableNotFound = fmt.Errorf("table not found")

func (s *Server) infoForTable(ctx context.Context, schemaName string, tableName string, configuration map[string]string) (tableInfo, error) {
	cfg, err := s.parseConfig(configuration)
	if err != nil {
		return tableInfo{}, fmt.Errorf("failed parsing info for table config: %v", err.Error())
	}

	db, err := s.connect(ctx, cfg, schemaName)
	if err != nil {
		return tableInfo{}, err
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.LogWarning("failed to close db", err)
		}
	}()

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

	if err := validateTableName(tableName); err != nil {
		return tableInfo{}, err
	}

	query := fmt.Sprintf(`INFO FOR TABLE %s;`, tableName)

	info, err := surrealdb.Query[InfoForTableResult](ctx, db, query, nil)
	if err != nil {
		return tableInfo{}, err
	}

	if len(*info) == 0 {
		return tableInfo{}, ErrTableNotFound
	}

	first := (*info)[0]

	fields := first.Result.Fields

	if s.Debugging() {
		s.LogDebug("INFO FOR TABLE", "table", tableName, "fields", fields)
	}

	columns := []columnInfo{}

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
				return tableInfo{}, fmt.Errorf("failed to unmarshal comment %s for field %s: %v", comment, name, err)
			}
		}

		// `DEFINE FIELD upper.* ON table TYPE any;`
		// ends up with a field name like `upper[*]`
		// upper[*] are for any nested fields in the `upper` object field
		// and does not need to be mapped to a Fivetran column.
		// What's why we skip them here.
		if strings.HasSuffix(name, "[*]") {
			if tpe != "any" {
				return tableInfo{}, fmt.Errorf("unexpected type for field %s: %s", name, tpe)
			}
			continue
		}

		var optional bool
		if strings.HasPrefix(tpe, "option<") {
			tpe = strings.TrimPrefix(tpe, "option<")
			tpe = strings.TrimSuffix(tpe, ">")
			optional = true
		}

		columns = append(columns, columnInfo{
			Name:       name,
			SDBType:    tpe,
			Optional:   optional,
			ColumnMeta: meta,
		})
	}

	// columns = append(columns, columnInfo{
	// 	Name:       "id",
	// 	Type:       "string",
	// 	PrimaryKey: true,
	// })

	if s.Debugging() {
		s.LogDebug("Ran info for table", "table", tableName, "columns", columns)
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].FtIndex < columns[j].FtIndex
	})

	return tableInfo{
		columns: columns,
	}, nil
}

func (s *Server) columnsFromSurrealToFivetran(sColumns []columnInfo) ([]*pb.Column, error) {
	var ftColumns []*pb.Column

	for _, c := range sColumns {
		ftColumns = append(ftColumns, &pb.Column{
			Name: c.Name,
			Type: c.FtType,
		})
	}

	return ftColumns, nil
}
