package tablemapper

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// TypeMapping maps between SurrealDB and Fivetran data types.
type TypeMapping struct {
	SDB string
	FT  pb.DataType
	// max decimal precision supported for this mapping
	MaxDecimalPrecision uint32
	SurrealType         func(string) (interface{}, error)
}

// TypeMappings contains all available type mappings.
var TypeMappings = []TypeMapping{
	{
		SDB: "string",
		FT:  pb.DataType_STRING,
		SurrealType: func(v string) (interface{}, error) {
			return v, nil
		},
	},
	{
		SDB: "int",
		FT:  pb.DataType_INT,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		SDB: "int",
		FT:  pb.DataType_SHORT,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		SDB: "int",
		FT:  pb.DataType_LONG,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.Atoi(v)
		},
	},
	{
		SDB: "bytes",
		FT:  pb.DataType_BINARY,
		SurrealType: func(v string) (interface{}, error) {
			return []byte(v), nil
		},
	},
	{
		SDB: "float",
		FT:  pb.DataType_FLOAT,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		SDB: "float",
		FT:  pb.DataType_DOUBLE,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		SDB: "bool",
		FT:  pb.DataType_BOOLEAN,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.ParseBool(v)
		},
	},
	{
		SDB:                 "decimal",
		FT:                  pb.DataType_DECIMAL,
		MaxDecimalPrecision: 28,
		SurrealType: func(v string) (interface{}, error) {
			return models.DecimalString(v), nil
		},
	},
	{
		SDB:                 "float",
		FT:                  pb.DataType_DECIMAL,
		MaxDecimalPrecision: math.MaxUint32,
		SurrealType: func(v string) (interface{}, error) {
			return strconv.ParseFloat(v, 64)
		},
	},
	{
		SDB: "datetime",
		FT:  pb.DataType_NAIVE_DATE,
		SurrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse(time.DateOnly, v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
		},
	},
	{
		SDB: "datetime",
		FT:  pb.DataType_NAIVE_DATETIME,
		SurrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse("2006-01-02T15:04:05", v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
		},
	},
	{
		SDB: "datetime",
		FT:  pb.DataType_UTC_DATETIME,
		SurrealType: func(v string) (interface{}, error) {
			dt, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			return models.CustomDateTime{Time: dt}, nil
		},
	},
	{
		SDB: "object",
		FT:  pb.DataType_JSON,
		SurrealType: func(v string) (interface{}, error) {
			m := map[string]interface{}{}
			if err := json.Unmarshal([]byte(v), &m); err != nil {
				return nil, fmt.Errorf("surrealType(object): %w", err)
			}
			return m, nil
		},
	},
	{
		SDB: "string",
		FT:  pb.DataType_XML,
		SurrealType: func(v string) (interface{}, error) {
			return v, nil
		},
	},
	{
		SDB: "duration",
		FT:  pb.DataType_NAIVE_TIME,
		SurrealType: func(v string) (interface{}, error) {
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

// FindTypeMappingByPbColumn finds the type mapping for a protobuf column.
func FindTypeMappingByPbColumn(col *pb.Column) *TypeMapping {
	for _, m := range TypeMappings {
		if m.FT == col.Type {
			if m.MaxDecimalPrecision < PbColumnDecimalPrecision(col) {
				continue
			}
			return &m
		}
	}
	return nil
}

// FindTypeMappingByColumnInfo finds the type mapping for a column info.
func FindTypeMappingByColumnInfo(col *ColumnInfo) *TypeMapping {
	for _, m := range TypeMappings {
		if m.FT == col.FtType {
			if m.MaxDecimalPrecision < col.DecimalPrecision {
				continue
			}
			return &m
		}
	}
	return nil
}

// DefineFieldQueryForHistoryModeIDFromFt generates a DEFINE FIELD query for history mode ID.
func DefineFieldQueryForHistoryModeIDFromFt(tb string, c *pb.Column, columnIndex int) (string, error) {
	t := `DEFINE FIELD OVERWRITE %s on %s TYPE array<any> COMMENT '%s';`

	meta := NewColumnMeta(c, columnIndex)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("failed to marshal column meta: %w", err)
	}

	defineField := fmt.Sprintf(t, c.Name, tb, string(metaJSON))

	return defineField, nil
}

// DefineFieldQueryFromFt generates a DEFINE FIELD query from a Fivetran column.
func DefineFieldQueryFromFt(tb string, c *pb.Column, columnIndex int) (string, error) {
	t := `DEFINE FIELD OVERWRITE %s on %s TYPE option<%s> COMMENT '%s';`

	tpe := FindTypeMappingByPbColumn(c)
	if tpe == nil {
		return "", fmt.Errorf("defining field: unsupported data type: %s (name=%v, type=%v, params=%v)", c.Type, c.Name, c.Type, c.Params)
	}

	sdb := tpe.SDB

	meta := NewColumnMeta(c, columnIndex)
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

// PbColumnDecimalPrecision returns the decimal precision for a protobuf column.
func PbColumnDecimalPrecision(c *pb.Column) uint32 {
	if c.Type != pb.DataType_DECIMAL {
		return 0
	}
	params, ok := c.Params.Params.(*pb.DataTypeParams_Decimal)
	if !ok {
		return 0
	}
	return params.Decimal.Precision
}

// NewColumnMeta creates a new ColumnMeta from a protobuf column.
func NewColumnMeta(c *pb.Column, columnIndex int) ColumnMeta {
	return ColumnMeta{
		FtIndex:          columnIndex,
		FtType:           c.Type,
		DecimalPrecision: PbColumnDecimalPrecision(c),
	}
}
