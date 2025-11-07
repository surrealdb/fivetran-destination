package server

import (
	"context"
	"fmt"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// Reads CSV files and replaces existing records accordingly.
func (s *Server) handleReplaceFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, replaceFiles []string, fileParams *pb.FileParams, keys map[string][]byte, table *pb.Table) error {
	unmodifiedString := fileParams.UnmodifiedString
	return s.processCSVRecords(replaceFiles, fileParams, keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Replacing record", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		cols, vals, err := s.getPKColumnsAndValues(values, table)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
		}

		var thing models.RecordID
		if len(cols) == 1 {
			thing = models.NewRecordID(table.Name, vals[0])
		} else {
			thing = models.NewRecordID(table.Name, vals)
		}

		vars := map[string]interface{}{}
		for k, v := range values {
			if unmodifiedString != "" && v == unmodifiedString {
				if s.Debugging() {
					s.LogDebug("Skipping unmodified column", "column", k, "value", v)
				}
				continue
			}

			if k == "id" {
				if s.Debugging() {
					s.LogDebug("Skipping id column")
				}
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			if v == fileParams.NullString {
				vars[k] = models.None
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return err
			}

			vars[k] = typedV
		}

		res, err := surrealdb.Upsert[any](ctx, db, thing, vars)
		if err != nil {
			if s.metrics != nil {
				s.metrics.DBWriteError()
			}
			return fmt.Errorf("unable to upsert record %s: %w", thing, err)
		}

		// Track successful DB write
		if s.metrics != nil {
			s.metrics.DBWriteCompleted(1)
		}

		if s.Debugging() {
			s.LogDebug("Replaced record", "values", values, "thing", thing, "vars", fmt.Sprintf("%+v", vars), "result", fmt.Sprintf("%+v", *res))
		}

		return nil
	})
}

func (s *Server) getPKColumnsAndValues(values map[string]string, table *pb.Table) ([]string, []any, error) {
	var pkColumns []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkColumns = append(pkColumns, c.Name)
		}
	}

	// Note that we intentionally do not sort the primary key columns.
	// We assume Fivetran in history mode sends us primary key columns containing the primary key in the source
	// along with _fivetran_start.
	// In that case, I want to use [id_from_src, _fivetran_start] as the primary key assumng
	// Fivetran gives us columns definitions in this specific order.
	// If we sorted it like the below, we might end up with [_fivetran_start, id_from_src] as the primary key.
	// That's not wrong but I think it's not intuitive from users perspective.
	//
	// sort.Slice(pkColumns, func(i, j int) bool {
	// 	return pkColumns[i] < pkColumns[j]
	// })

	var pkValues []any
	for _, pkColumn := range pkColumns {
		pkValues = append(pkValues, values[pkColumn])
	}

	return pkColumns, pkValues, nil
}
