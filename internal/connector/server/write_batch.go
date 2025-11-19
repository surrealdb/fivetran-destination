package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func (s *Server) writeBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	if s.Debugging() {
		s.LogDebug("WriteBatch called", "schema", req.SchemaName, "table", req.Table.Name, "config", req.Configuration)
		s.LogDebug("Replace files", "count", len(req.ReplaceFiles))
		s.LogDebug("Update files", "count", len(req.UpdateFiles))
		s.LogDebug("Delete files", "count", len(req.DeleteFiles))
		s.LogDebug("Keys", "keys", req.Keys)
		s.LogDebug("FileParams",
			"compression", req.FileParams.Compression,
			"encryption", req.FileParams.Encryption,
			"null_string", req.FileParams.NullString,
			"unmodified_string", req.FileParams.UnmodifiedString)
	}

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: fmt.Sprintf("failed parsing write batch config: %v", err.Error()),
				},
			},
		}, err
	}

	db, err := s.connectAndUse(ctx, cfg, req.SchemaName)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.LogWarning("failed to close db", err)
		}
	}()

	if s.Debugging() {
		s.LogDebug("WriteBatch using", "namespace", cfg.ns, "database", req.SchemaName)
	}

	tb, err := s.infoForTable(ctx, req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	fields := make(map[string]tablemapper.ColumnInfo)
	for _, column := range tb.Columns {
		fields[column.Name] = column
	}

	if err := s.handleReplaceFiles(ctx, db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchUpdate(ctx, db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchDelete(ctx, db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

// Reads CSV files and updates existing records accordingly.
func (s *Server) batchUpdate(ctx context.Context, db *surrealdb.DB, fields map[string]tablemapper.ColumnInfo, req *pb.WriteBatchRequest) error {
	unmodifiedString := req.FileParams.UnmodifiedString

	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Updating record", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		// Note that the below is not correct!
		// Although `table:id` is a valid record ID in SurrealQL,
		// it's different from a string "table:id".
		// If you created a record using db.Upsert(db, models.NewRecordID("mytb", "myid"), ...)
		// and then tried to update using db.Update(db, "mytb:myid", ...), it doesn't work!
		// You can notice it by seeing the result of the Update being `[[]]`, which indicates
		// there was nothing to Update.
		//
		// thing := fmt.Sprintf("%s:%s", req.Table.Name, values["_fivetran_id"])

		cols, vals, err := s.getPKColumnsAndValues(values, req.Table, fields)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
		}

		var thing models.RecordID
		if len(cols) == 1 {
			thing = models.NewRecordID(req.Table.Name, vals[0])
		} else {
			thing = models.NewRecordID(req.Table.Name, vals)
		}

		var hasUnmodifiedColumns bool

		vars := map[string]interface{}{}
		for k, v := range values {
			if unmodifiedString != "" && v == unmodifiedString {
				if s.Debugging() {
					s.LogDebug("Skipping unmodified column", "column", k, "value", v)
				}
				hasUnmodifiedColumns = true
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("soft delete mode update file: column %s not found in the table info: %v", k, fields)
			}

			var typedV interface{}

			typedV, err := f.StrToSurrealType(v)
			if err != nil {
				return fmt.Errorf("unable to convert value %s to surreal type %+v: %w", v, f, err)
			}

			vars[k] = typedV
		}

		var res *any
		if hasUnmodifiedColumns {
			if s.Debugging() {
				s.LogDebug("Doing upsert-merge to deal with unmodified columns in update with soft-delete sync mode", "thing", thing, "vars", vars)
			}
			var r any
			r, err = s.upsertMerge(ctx, db, thing, vars)
			if err != nil {
				if s.metrics != nil {
					s.metrics.DBWriteError()
				}
				return err
			}
			res = &r
		} else {
			res, err = surrealdb.Update[any](ctx, db, thing, vars)
			if err != nil {
				if s.metrics != nil {
					s.metrics.DBWriteError()
				}
				return err
			}
		}

		// Track successful DB write
		if s.metrics != nil {
			s.metrics.DBWriteCompleted(1)
		}

		if s.Debugging() {
			s.LogDebug("Updated record", "thing", thing, "vars", vars, "result", *res)
		}

		return nil
	})
}

func (s *Server) upsertMerge(ctx context.Context, db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) (*[]surrealdb.QueryResult[any], error) {
	var content string
	for k := range vars {
		content += fmt.Sprintf("%s: $%s, ", k, k)
	}
	content = strings.TrimSuffix(content, ", ")

	varsWithTB := map[string]interface{}{
		"tb": thing.Table,
		"id": thing,
	}
	for k, v := range vars {
		varsWithTB[k] = v
	}

	res, err := surrealdb.Query[any](ctx, db, `UPSERT type::thing($tb, $id) MERGE {`+content+`};`, varsWithTB)
	if err != nil {
		return nil, fmt.Errorf("unable to upsert merge record %s: %w", thing, err)
	}

	return res, nil
}

// Reads CSV files and deletes existing records accordingly.
func (s *Server) batchDelete(ctx context.Context, db *surrealdb.DB, fields map[string]tablemapper.ColumnInfo, req *pb.WriteBatchRequest) error {
	return s.processCSVRecords(req.DeleteFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Deleting record", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		thing := models.NewRecordID(req.Table.Name, values["_fivetran_id"])

		_, err := surrealdb.Delete[any](ctx, db, thing)
		if err != nil {
			if s.metrics != nil {
				s.metrics.DBWriteError()
			}
			return fmt.Errorf("unable to delete record %s: %w", thing, err)
		}

		// Track successful DB write (delete)
		if s.metrics != nil {
			s.metrics.DBWriteCompleted(1)
		}

		if s.Debugging() {
			s.LogDebug("Deleted record", "thing", thing)
		}

		return nil
	})
}
