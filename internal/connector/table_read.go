package connector

import (
	"fmt"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

type tableInfo struct {
	columns []columnInfo
}

type columnInfo struct {
	Name       string
	Type       string
	PrimaryKey bool
	Optional   bool
}

func (c *columnInfo) strToSurrealType(v string) (interface{}, error) {
	for _, t := range typeMappings {
		if t.sdb == c.Type {
			return t.surrealType(v)
		}
	}
	return nil, fmt.Errorf("unsupported data type for column %s: %s", c.Name, c.Type)
}

var ErrTableNotFound = fmt.Errorf("table not found")

func (s *Server) infoForTable(schemaName string, tableName string, configuration map[string]string) (tableInfo, error) {
	cfg, err := s.parseConfig(configuration)
	if err != nil {
		return tableInfo{}, fmt.Errorf("failed parsing info for table config: %v", err.Error())
	}

	db, err := s.connect(cfg, schemaName)
	if err != nil {
		return tableInfo{}, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
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

	info, err := surrealdb.Query[InfoForTableResult](db, query, nil)
	if err != nil {
		return tableInfo{}, err
	}

	if len(*info) == 0 {
		return tableInfo{}, ErrTableNotFound
	}

	first := (*info)[0]

	fields := first.Result.Fields

	if s.debugging() {
		s.logDebug("INFO FOR TABLE", "table", tableName, "fields", fields)
	}

	columns := []columnInfo{}

	for _, field := range fields {
		field = strings.TrimPrefix(field, "DEFINE FIELD ")
		s := strings.Split(field, " ")
		l := s[0]
		rr := strings.Split(field, " TYPE ")
		r := strings.Split(rr[1], " ")[0]

		name := l
		tpe := r

		var optional bool
		if strings.HasPrefix(tpe, "option<") {
			tpe = strings.TrimPrefix(tpe, "option<")
			tpe = strings.TrimSuffix(tpe, ">")
			optional = true
		}

		columns = append(columns, columnInfo{
			Name:     name,
			Type:     tpe,
			Optional: optional,
		})
	}

	// columns = append(columns, columnInfo{
	// 	Name:       "id",
	// 	Type:       "string",
	// 	PrimaryKey: true,
	// })

	if s.debugging() {
		s.logDebug("Ran info for table", "table", tableName, "columns", columns)
	}

	return tableInfo{
		columns: columns,
	}, nil
}

func (s *Server) columnsFromSurrealToFivetran(sColumns []columnInfo) ([]*pb.Column, error) {
	var ftColumns []*pb.Column

	for _, c := range sColumns {
		var pbDataType pb.DataType
		switch c.Type {
		case "string":
			pbDataType = pb.DataType_STRING
		case "int":
			pbDataType = pb.DataType_INT
		case "float":
			pbDataType = pb.DataType_FLOAT
		case "double":
			pbDataType = pb.DataType_DOUBLE
		case "bool":
			pbDataType = pb.DataType_BOOLEAN
		case "datetime":
			pbDataType = pb.DataType_UTC_DATETIME
		case "object":
			pbDataType = pb.DataType_JSON
		default:
			return nil, fmt.Errorf("columnsFromSurrealToFivetran: unsupported data type: %s", c.Type)
		}
		ftColumns = append(ftColumns, &pb.Column{
			Name: c.Name,
			Type: pbDataType,
		})
	}

	return ftColumns, nil
}
