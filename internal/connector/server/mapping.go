package server

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// See https://surrealdb.com/docs/surrealql/datamodel#data-types for
// available SurrealDB data types.
type typeMapping struct {
	sdb string
	ft  pb.DataType
	// max decimal precision supported for this mapping
	maxDecimalPrecision uint32
	surrealType         func(string) (interface{}, error)
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
		sdb:                 "decimal",
		ft:                  pb.DataType_DECIMAL,
		maxDecimalPrecision: 28,
		surrealType: func(v string) (interface{}, error) {
			return models.DecimalString(v), nil
		},
	},
	{
		sdb:                 "float",
		ft:                  pb.DataType_DECIMAL,
		maxDecimalPrecision: math.MaxUint32,
		surrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_NAIVE_DATE,
		surrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse(time.DateOnly, v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_NAIVE_DATETIME,
		surrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse("2006-01-02T15:04:05", v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
		},
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_UTC_DATETIME,
		surrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
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
		sdb: "duration",
		ft:  pb.DataType_NAIVE_TIME,
		surrealType: func(v string) (interface{}, error) {
			ref, err := time.Parse(time.TimeOnly, "00:00:00")
			if err != nil {
				return nil, err
			}
			parsed, err := time.Parse(time.TimeOnly, v)
			if err != nil {
				return nil, err
			}
			return models.CustomDuration{Duration: parsed.Sub(ref)}, nil
		},
	},
}

func findTypeMappingByPbColumn(col *pb.Column) *typeMapping {
	for _, m := range typeMappings {
		if m.ft == col.Type {
			if m.maxDecimalPrecision < pbColumnDecimalPrecision(col) {
				continue
			}
			return &m
		}
	}
	return nil
}

func findTypeMappingByColumnInfo(col *columnInfo) *typeMapping {
	for _, m := range typeMappings {
		if m.ft == col.FtType {
			if m.maxDecimalPrecision < col.DecimalPrecision {
				continue
			}
			return &m
		}
	}
	return nil
}

func (s *Server) defineFieldQueryForHistoryModeIDFromFt(tb string, c *pb.Column, columnIndex int) (string, error) {
	t := `DEFINE FIELD OVERWRITE %s on %s TYPE array<any> COMMENT '%s';`

	meta := newColumnMeta(c, columnIndex)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("failed to marshal column meta: %w", err)
	}

	defineField := fmt.Sprintf(t, c.Name, tb, string(metaJSON))

	return defineField, nil
}

func (s *Server) defineFieldQueryFromFt(tb string, c *pb.Column, columnIndex int) (string, error) {
	t := `DEFINE FIELD OVERWRITE %s on %s TYPE option<%s> COMMENT '%s';`

	tpe := findTypeMappingByPbColumn(c)
	if tpe == nil {
		return "", fmt.Errorf("defining field: unsupported data type: %s (name=%v, type=%v, params=%v)", c.Type, c.Name, c.Type, c.Params)
	}

	sdb := tpe.sdb

	meta := newColumnMeta(c, columnIndex)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("failed to marshal column meta: %w", err)
	}

	defineField := fmt.Sprintf(t, c.Name, tb, sdb, string(metaJSON))

	if c.Type == pb.DataType_JSON {
		defineField += fmt.Sprintf("DEFINE FIELD %s.* ON %s TYPE any;", c.Name, tb)
	}

	return defineField, nil
}

func pbColumnDecimalPrecision(c *pb.Column) uint32 {
	if c.Type != pb.DataType_DECIMAL {
		return 0
	}
	params, ok := c.Params.Params.(*pb.DataTypeParams_Decimal)
	if !ok {
		return 0
	}
	return params.Decimal.Precision
}

func newColumnMeta(c *pb.Column, columnIndex int) ColumnMeta {
	return ColumnMeta{
		FtIndex:          columnIndex,
		FtType:           c.Type,
		DecimalPrecision: pbColumnDecimalPrecision(c),
	}
}
