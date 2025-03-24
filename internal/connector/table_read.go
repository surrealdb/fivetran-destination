package connector

import (
	"fmt"
	"log"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

type tableInfo struct {
	columns []columnInfo
}

type columnInfo struct {
	Name       string
	Type       string
	PrimaryKey bool
}

func (c *columnInfo) strToSurrealType(v string) (interface{}, error) {
	for _, t := range typeMappings {
		if t.sdb == c.Type {
			return t.surrealType(v)
		}
	}
	return nil, fmt.Errorf("unsupported data type for column %s: %s", c.Name, c.Type)
}

func (s *Server) infoForTable(schemaName string, tableName string, configuration map[string]string) (tableInfo, error) {
	cfg, err := s.parseConfig(configuration)
	if err != nil {
		return tableInfo{}, err
	}

	db, err := s.connect(cfg, schemaName)
	if err != nil {
		return tableInfo{}, err
	}
	defer db.Close()

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

	m := map[string]interface{}{}
	if err := db.Send(&m, "query", query); err != nil {
		return tableInfo{}, err
	}

	log.Printf("INFO FOR TABLE %s: %v", tableName, m)

	fields, ok := m["fields"].([]string)
	if !ok {
		// New table without any fields
		return tableInfo{}, nil
	}

	columns := []columnInfo{}
	for _, field := range fields {
		i := strings.Index(field, "TYPE")
		if i == -1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		// the right part is the column name
		tpeStart := strings.Index(field[i:], " ") + i + 1
		if tpeStart < i+1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		tpeEnd := strings.Index(field[tpeStart:], " ") + tpeStart
		if tpeEnd < tpeStart {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		tpe := field[tpeStart:tpeEnd]

		// the left part is the column name
		colEnd := strings.Index(field[:i], " ")
		if colEnd == -1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		colStart := strings.LastIndex(field[:colEnd], " ") + 1
		if colStart > colEnd {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		col := field[colStart:colEnd]

		columns = append(columns, columnInfo{
			Name:       col,
			Type:       tpe,
			PrimaryKey: col == "id",
		})
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
		case "boolean":
			pbDataType = pb.DataType_BOOLEAN
		default:
			return nil, fmt.Errorf("unsupported data type: %s", c.Type)
		}
		ftColumns = append(ftColumns, &pb.Column{
			Name: c.Name,
			Type: pbDataType,
		})
	}

	return ftColumns, nil
}
