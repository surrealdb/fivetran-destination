package connector

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// See https://surrealdb.com/docs/surrealql/datamodel#data-types for
// available SurrealDB data types.
type typeMapping struct {
	sdb         string
	ft          pb.DataType
	surrealType func(string) (interface{}, error)
}

var typeMappings = []typeMapping{
	{
		sdb: "string",
		ft:  pb.DataType_STRING,
		surrealType: func(v string) (interface{}, error) {
			return v, nil
		},
	},
	{
		sdb: "int",
		ft:  pb.DataType_INT,
		surrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		sdb: "int",
		ft:  pb.DataType_SHORT,
		surrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		sdb: "int",
		ft:  pb.DataType_LONG,
		surrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		sdb: "bytes",
		ft:  pb.DataType_BINARY,
		surrealType: func(v string) (interface{}, error) {
			return []byte(v), nil
		},
	},
	{
		sdb: "float",
		ft:  pb.DataType_FLOAT,
		surrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		sdb: "float",
		ft:  pb.DataType_DOUBLE,
		surrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		sdb: "bool",
		ft:  pb.DataType_BOOLEAN,
		surrealType: func(v string) (interface{}, error) {
			return strconv.ParseBool(v)
		},
	},
	{
		sdb: "decimal",
		ft:  pb.DataType_DECIMAL,
		surrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_NAIVE_DATE,
		surrealType: func(v string) (interface{}, error) {
			return time.Parse(time.RFC3339, v)
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_NAIVE_DATETIME,
		surrealType: func(v string) (interface{}, error) {
			return time.Parse(time.RFC3339, v)
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_UTC_DATETIME,
		surrealType: func(v string) (interface{}, error) {
			return time.Parse(time.RFC3339, v)
		},
	},
	{
		sdb: "object",
		ft:  pb.DataType_JSON,
		surrealType: func(v string) (interface{}, error) {
			m := map[string]interface{}{}
			if err := json.Unmarshal([]byte(v), &m); err != nil {
				return nil, fmt.Errorf("surrealType(object): %w", err)
			}
			return m, nil
		},
	},
	{
		sdb: "string",
		ft:  pb.DataType_XML,
		surrealType: func(v string) (interface{}, error) {
			return v, nil
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_NAIVE_TIME,
		surrealType: func(v string) (interface{}, error) {
			return time.Parse(time.RFC3339, v)
		},
	},
}

func (s *Server) defineFieldQueryFromFt(tb string, c *pb.Column) (string, error) {
	t := `DEFINE FIELD %s on %s TYPE option<%s>;`

	var sdb string
	for _, m := range typeMappings {
		if m.ft == c.Type {
			sdb = m.sdb
			break
		}
	}

	if sdb == "" {
		return "", fmt.Errorf("unsupported data type: %s (name=%v, type=%v, params=%v)", c.Type, c.Name, c.Type, c.Params)
	}

	defineField := fmt.Sprintf(t, c.Name, tb, sdb)

	if c.Type == pb.DataType_JSON {
		defineField += fmt.Sprintf("DEFINE FIELD %s.* ON %s TYPE any;", c.Name, tb)
	}

	return defineField, nil
}
