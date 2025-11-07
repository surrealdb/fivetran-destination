package connector

import (
	"context"
	"fmt"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func (s *Server) writeBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	if s.debugging() {
		s.logDebug("WriteBatch called", "schema", req.SchemaName, "table", req.Table.Name, "config", req.Configuration)
		s.logDebug("Replace files", "count", len(req.ReplaceFiles))
		s.logDebug("Update files", "count", len(req.UpdateFiles))
		s.logDebug("Delete files", "count", len(req.DeleteFiles))
		s.logDebug("Keys", "keys", req.Keys)
		s.logDebug("FileParams",
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

	db, err := s.connect(ctx, cfg, req.SchemaName)
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
			s.logWarning("failed to close db", err)
		}
	}()

	if s.debugging() {
		s.logDebug("WriteBatch using", "namespace", cfg.ns, "database", req.SchemaName)
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

	fields := make(map[string]columnInfo)
	for _, column := range tb.columns {
		fields[column.Name] = column
	}

	if err := s.batchReplace(ctx, db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
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
func (s *Server) batchUpdate(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
	unmodifiedString := req.FileParams.UnmodifiedString

	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.debugging() {
			s.logDebug("Updating record", "columns", columns, "record", record)
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

		cols, vals, err := s.getPKColumnsAndValues(values, req.Table)
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
				if s.debugging() {
					s.logDebug("Skipping unmodified column", "column", k, "value", v)
				}
				hasUnmodifiedColumns = true
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return fmt.Errorf("unable to convert value %s to surreal type %+v: %w", v, f, err)
			}

			vars[k] = typedV
		}

		var res *any
		if hasUnmodifiedColumns {
			if s.debugging() {
				s.logDebug("Doing upsert-merge to deal with unmodified columns in update with soft-delete sync mode", "thing", thing, "vars", vars)
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

		if s.debugging() {
			s.logDebug("Updated record", "thing", thing, "vars", vars, "result", *res)
		}

		return nil
	})
}

// Reads CSV files and deletes existing records accordingly.
func (s *Server) batchDelete(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
	return s.processCSVRecords(req.DeleteFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.debugging() {
			s.logDebug("Deleting record", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		thing := fmt.Sprintf("%s:%s", req.Table.Name, values["_fivetran_id"])

		type DeleteResponse struct {
			ID     int                    `json:"id"`
			Result map[string]interface{} `json:"result"`
		}
		var res connection.RPCResponse[DeleteResponse]

		err := surrealdb.Send(ctx, db, &res, "delete", thing)
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

		if s.debugging() {
			s.logDebug("Deleted record", "thing", thing, "result", res)
		}

		return nil
	})
}
