package connector

import (
	"fmt"
	"log"
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

	info, err := surrealdb.Query[InfoForTableResult](db, query, nil)
	if err != nil {
		return tableInfo{}, err
	}

	first := (*info)[0]

	fields := first.Result.Fields

	log.Printf("INFO FOR TABLE %s: %v", tableName, fields)

	columns := []columnInfo{}

	for _, field := range fields {
		field = strings.TrimPrefix(field, "DEFINE FIELD ")
		s := strings.Split(field, " ")
		l := s[0]
		rr := strings.Split(field, " TYPE ")
		r := strings.Split(rr[1], " ")[0]

		name := l
		tpe := r

		columns = append(columns, columnInfo{
			Name: name,
			Type: tpe,
		})
	}

	columns = append(columns, columnInfo{
		Name:       "id",
		Type:       "string",
		PrimaryKey: true,
	})

	log.Printf("COLUMNS: %v", columns)

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
