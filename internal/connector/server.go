package connector

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	_ "google.golang.org/grpc/encoding/gzip"
)

func LoggerFromEnv() (zerolog.Logger, error) {
	level := zerolog.InfoLevel
	if os.Getenv("SURREAL_FIVETRAN_DEBUG") != "" {
		level = zerolog.DebugLevel
	}
	return initLogger(nil, level), nil
}

func NewServer(logger zerolog.Logger) *Server {
	return &Server{
		mu: &sync.Mutex{},
		Logging: &Logging{
			logger: logger,
		},
	}
}

type Server struct {
	pb.UnimplementedDestinationConnectorServer

	mu *sync.Mutex

	*Logging
}

// ConfigurationForm implements the ConfigurationForm method required by the DestinationConnectorServer interface
func (s *Server) ConfigurationForm(ctx context.Context, req *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
	var fields []*pb.FormField
	var tests []*pb.ConfigurationTest

	// Helper functions to create pointers to primitive types
	boolPtr := func(b bool) *bool { return &b }
	stringPtr := func(s string) *string { return &s }

	fields = append(fields, &pb.FormField{
		Name:        "url",
		Label:       "URL",
		Placeholder: stringPtr("wss://your.surrealdb.instance/rpc"),
		Description: stringPtr("Input the externally accessible URL of the SurrealDB instance"),
		Required:    boolPtr(true),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_PlainText},
	})

	fields = append(fields, &pb.FormField{
		Name:        "user",
		Label:       "User",
		Placeholder: stringPtr("user"),
		Description: stringPtr("User for user/pass authentication. Leave blank if using token authentication."),
		Required:    boolPtr(false),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
	})

	fields = append(fields, &pb.FormField{
		Name:        "pass",
		Label:       "Password",
		Placeholder: stringPtr("password"),
		Description: stringPtr("Pass for user/pass authentication. Leave blank if using token authentication."),
		Required:    boolPtr(false),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
	})

	fields = append(fields, &pb.FormField{
		Name:        "token",
		Label:       "Token",
		Placeholder: stringPtr("token"),
		Description: stringPtr("Token for token authentication. Leave blank if using user/pass authentication."),
		Required:    boolPtr(false),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
	})

	fields = append(fields, &pb.FormField{
		Name:        "ns",
		Label:       "Namespace",
		Placeholder: stringPtr("namespace"),
		Description: stringPtr("Input the namespace for the SurrealDB instance"),
		Required:    boolPtr(true),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_PlainText},
	})

	tests = append(tests, &pb.ConfigurationTest{
		Name:  "database-connection",
		Label: "Database Connection",
	})

	if s.debugging() {
		s.logDebug("ConfigurationForm called")
	}
	return &pb.ConfigurationFormResponse{
		Fields: fields,
		Tests:  tests,
	}, nil
}

// Capabilities implements the Capabilities method required by the DestinationConnectorServer interface
func (s *Server) Capabilities(ctx context.Context, req *pb.CapabilitiesRequest) (*pb.CapabilitiesResponse, error) {
	if s.debugging() {
		s.logDebug("Capabilities called")
	}
	return &pb.CapabilitiesResponse{
		// TODO: Parquet support?
		BatchFileFormat: pb.BatchFileFormat_CSV,
	}, nil
}

// Test implements the Test method required by the DestinationConnectorServer interface
//
// It basically checks if the provided configuration is valid,
// by trying to connect to the SurrealDB instance using the connection information
// included in the configuration.
func (s *Server) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	startTime := time.Now()
	s.logDebug("Starting configuration test",
		"config_name", req.Name,
		"config", req.Configuration)

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		s.logSevere("Failed to parse test configuration", err,
			"config_name", req.Name)
		return &pb.TestResponse{
			Response: &pb.TestResponse_Failure{
				Failure: fmt.Sprintf("failed parsing test config: %v", err.Error()),
			},
		}, err
	}

	if _, err := s.connect(cfg, ""); err != nil {
		s.logSevere("Failed to connect to database", err,
			"config_name", req.Name)
		return &pb.TestResponse{
			Response: &pb.TestResponse_Failure{
				Failure: err.Error(),
			},
		}, err
	}

	s.logDebug("Finished configuration test",
		"config_name", req.Name,
		"duration_ms", time.Since(startTime).Milliseconds())

	return &pb.TestResponse{
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

// DescribeTable implements the DescribeTable method required by the DestinationConnectorServer interface
func (s *Server) DescribeTable(ctx context.Context, req *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	if s.debugging() {
		s.logDebug("DescribeTable called", "schema", req.SchemaName, "table", req.TableName, "config", req.Configuration)
	}
	tb, err := s.infoForTable(req.SchemaName, req.TableName, req.Configuration)
	if err != nil {
		if err == ErrTableNotFound {
			return &pb.DescribeTableResponse{
				Response: &pb.DescribeTableResponse_NotFound{
					NotFound: true,
				},
			}, nil
		}

		return &pb.DescribeTableResponse{
			Response: &pb.DescribeTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("infoForTable result", "table_info", tb)
	}

	ftColumns, err := s.columnsFromSurrealToFivetran(tb.columns)
	if err != nil {
		return &pb.DescribeTableResponse{
			// notfound, table, warning, task
			Response: &pb.DescribeTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	return &pb.DescribeTableResponse{
		// notfound, table, warning, task
		Response: &pb.DescribeTableResponse_Table{
			Table: &pb.Table{
				Name:    req.TableName,
				Columns: ftColumns,
			},
		},
	}, nil
}

// CreateTable implements the CreateTable method required by the DestinationConnectorServer interface
func (s *Server) CreateTable(ctx context.Context, req *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	if s.debugging() {
		s.logDebug("CreateTable called", "schema", req.SchemaName, "table", req.Table.Name)
	}

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.CreateTableResponse{
			Response: &pb.CreateTableResponse_Warning{
				Warning: &pb.Warning{
					Message: fmt.Sprintf("failed parsing create table config: %v", err.Error()),
				},
			},
		}, err
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.CreateTableResponse{
			// success, warning, task
			Response: &pb.CreateTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if err := s.defineTable(db, req.Table); err != nil {
		return &pb.CreateTableResponse{
			// success, warning, task
			Response: &pb.CreateTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	tbInfo, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.CreateTableResponse{
			// success, warning, task
			Response: &pb.CreateTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("infoForTable result", "table_info", tbInfo)
	}

	return &pb.CreateTableResponse{
		// success, warning, task
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

// AlterTable implements the AlterTable method required by the DestinationConnectorServer interface
func (s *Server) AlterTable(ctx context.Context, req *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	if s.debugging() {
		s.logDebug("AlterTable called", "schema", req.SchemaName, "table", req.Table.Name)
	}
	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.AlterTableResponse{
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: fmt.Sprintf("failed parsing alter table config: %v", err.Error()),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("AlterTable config", "config", cfg)
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.AlterTableResponse{
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if err := s.defineTable(db, req.Table); err != nil {
		return &pb.AlterTableResponse{
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	tbInfo, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.AlterTableResponse{
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("infoForTable result", "table_info", tbInfo)
	}

	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

// Truncate implements the Truncate method required by the DestinationConnectorServer interface
func (s *Server) Truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	if s.debugging() {
		s.logDebug("Truncate called",
			"schema", req.SchemaName,
			"table", req.TableName,
			// SyncedColumn is e.g. `_sivetran_synced` which is timestamp-like column/field
			"syncedColumn", req.SyncedColumn,
		)
		if req.Soft != nil {
			s.logDebug("This is a soft truncation. See https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide.md#truncate",
				"soft.deletedColumn", req.Soft.DeletedColumn,
			)
		}
		if req.UtcDeleteBefore != nil {
			s.logDebug("UtcDeleteBefore", "time", req.UtcDeleteBefore.AsTime().Format(time.RFC3339))
		}

		// You usually do something like:
		//   SOFT DELETE:  `UPDATE <table> SET _fivetran_deleted = true WHERE _fivetran_synced <= <UtcDeleteBefore>`
		//   HARD DELETE:  `DELETE FROM <table> WHERE _fivetran_synced <= <UtcDeleteBefore>`
	}
	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.TruncateResponse{
			Response: &pb.TruncateResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.TruncateResponse{
			Response: &pb.TruncateResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if req.Soft != nil {
		// DeletedColumn is e.g. `_sivetran_deleted` which is bool-like column/field
		s.logInfo("Doing a soft truncation",
			"soft.deletedColumn", req.Soft.DeletedColumn,
		)

		if err := s.softTruncate(db, req); err != nil {
			return &pb.TruncateResponse{
				Response: &pb.TruncateResponse_Warning{
					Warning: &pb.Warning{
						Message: err.Error(),
					},
				},
			}, err
		}
	}

	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) softTruncate(db *surrealdb.DB, req *pb.TruncateRequest) error {
	deletedColumn := req.Soft.DeletedColumn

	res, err := surrealdb.Query[any](db, "UPDATE type::table($tb) SET "+deletedColumn+" = true WHERE type::field($sc) <= type::datetime($utc)", map[string]interface{}{
		"tb":  req.TableName,
		"dc":  deletedColumn,
		"sc":  req.SyncedColumn,
		"utc": req.UtcDeleteBefore.AsTime().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("failed to soft truncate: %w", err)
	}

	if s.debugging() {
		s.logDebug("SoftTruncate result", "result", res)
	}

	return nil
}

// WriteBatch implements the WriteBatch method required by the DestinationConnectorServer interface
func (s *Server) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
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

	db, err := s.connect(cfg, req.SchemaName)
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
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if s.debugging() {
		s.logDebug("WriteBatch using", "namespace", cfg.ns, "database", req.SchemaName)
	}

	tb, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
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

	if err := s.batchReplace(db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchUpdate(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchDelete(db, fields, req); err != nil {
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

// Reads CSV files and replaces existing records accordingly.
func (s *Server) batchReplace(db *surrealdb.DB, fields map[string]columnInfo, replaceFiles []string, fileParams *pb.FileParams, keys map[string][]byte, table *pb.Table) error {
	unmodifiedString := fileParams.UnmodifiedString
	return s.processCSVRecords(replaceFiles, fileParams, keys, func(columns []string, record []string) error {
		if s.debugging() {
			s.logDebug("Replacing record", "columns", columns, "record", record)
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
				if s.debugging() {
					s.logDebug("Skipping unmodified column", "column", k, "value", v)
				}
				continue
			}

			if k == "id" {
				if s.debugging() {
					s.logDebug("Skipping id column")
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

		res, err := surrealdb.Upsert[any](db, thing, vars)
		if err != nil {
			return fmt.Errorf("unable to upsert record %s: %w", thing, err)
		}

		if s.debugging() {
			s.logDebug("Replaced record", "values", values, "thing", thing, "vars", vars, "result", *res)
		}

		return nil
	})
}

// Reads CSV files and updates existing records accordingly.
func (s *Server) batchUpdate(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
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
			r, err = s.upsertMerge(db, thing, vars)
			if err != nil {
				return err
			}
			res = &r
		} else {
			res, err = surrealdb.Update[any](db, thing, vars)
			if err != nil {
				return err
			}
		}

		if s.debugging() {
			s.logDebug("Updated record", "thing", thing, "vars", vars, "result", *res)
		}

		return nil
	})
}

// Reads CSV files and deletes existing records accordingly.
func (s *Server) batchDelete(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
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

		err := db.Send(&res, "delete", thing)
		if err != nil {
			return fmt.Errorf("unable to delete record %s: %w", thing, err)
		}

		if s.debugging() {
			s.logDebug("Deleted record", "thing", thing, "result", res)
		}

		return nil
	})
}

func (s *Server) WriteHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	if s.debugging() {
		s.logDebug("WriteHistoryBatch called", "schema", req.SchemaName, "table", req.Table.Name)
		s.logDebug("Earliest start files", "count", len(req.EarliestStartFiles))
		s.logDebug("Replace files", "count", len(req.ReplaceFiles))
		s.logDebug("Update files", "count", len(req.UpdateFiles))
		s.logDebug("Delete files", "count", len(req.DeleteFiles))
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
					Message: fmt.Sprintf("failed parsing write history batch config: %v", err.Error()),
				},
			},
		}, err
	}

	db, err := s.connect(cfg, req.SchemaName)
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
		if err := db.Close(); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if s.debugging() {
		s.logDebug("WriteHistoryBatch using", "namespace", cfg.ns, "database", req.SchemaName)
	}

	tb, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
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

	if s.debugging() {
		s.logDebug("Batch processing earliest start files")
	}

	if err := s.batchProcessEarliestStartFiles(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("Batch processing replace files")
	}

	if err := s.batchReplace(db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("Batch processing update files")
	}

	if err := s.batchHistoryUpdate(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		s.logDebug("Batch processing delete files")
	}

	if err := s.batchReplace(db, fields, req.DeleteFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	// }

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) batchProcessEarliestStartFiles(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.EarliestStartFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.debugging() {
			s.logDebug("Processing earliest start file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.debugging() {
			s.logDebug("Earliest start record", "values", values)
		}

		cols, _, err := s.getPKColumnsAndValues(values, req.Table)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
		}

		// Let's remove everything all the records with the same primary key(s)
		// whose `_fivetran_start` is GREATER THAN this `_fivetran_start`.

		vars := map[string]interface{}{}
		for k, v := range values {
			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return err
			}

			vars[k] = typedV
		}

		vars["tb"] = req.Table.Name

		var conds []string

		for _, col := range cols {
			conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
		}

		byID := strings.Join(conds, " AND ")

		res, err := surrealdb.Query[any](
			db,
			"DELETE FROM type::table($tb) WHERE "+byID+" AND _fivetran_start > type::datetime($_fivetran_start);",
			vars,
		)
		if err != nil {
			return fmt.Errorf("unable to delete from table %s: %w", req.Table.Name, err)
		}

		if s.debugging() {
			s.logDebug("Removed records", "byID", byID, "_fivetran_start_gt", vars["_fivetran_start"], "result", *res)
		}

		return nil
	})
}

func (s *Server) batchHistoryUpdate(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.debugging() {
			s.logDebug("Processing update file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.debugging() {
			s.logDebug("batchHistoryUpdate record", "values", values)
		}

		var id any
		if v, ok := values["_fivetran_id"]; ok {
			id = v
		} else if v, ok := values["id"]; ok {
			id = v
		} else {
			return fmt.Errorf("id nor _fivetran_id not found in the record: %v", values)
		}

		thing := models.NewRecordID(req.Table.Name, id)

		var unmodifiedFields []string

		vars := map[string]interface{}{}
		for k, v := range values {
			if k == "id" {
				if s.debugging() {
					s.logDebug("Skipping id")
				}
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			if v == req.FileParams.UnmodifiedString {
				unmodifiedFields = append(unmodifiedFields, k)
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return err
			}

			vars[k] = typedV
		}

		cols, vals, err := s.getPKColumnsAndValues(values, req.Table)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %s: %w", thing, err)
		}

		// There could be one or more unmodified fields even though
		// it is the first time for Fivetran and the connector to upsert this record.
		// We try to obtain the previous values from SurrealDB anyway.
		// In case the record is not found, we are sure that the fields noted as unmodified are actually empty.
		if len(unmodifiedFields) > 0 {
			// Get the preivous values to populate the fields with values set to the unmodeified string
			previousFieldsAndValues, err := s.getPreviousValues(db, unmodifiedFields, req.Table, cols, vals)
			if err != nil {
				return fmt.Errorf("unable to get previous values for record %s: %w", thing, err)
			}

			for k, v := range previousFieldsAndValues {
				vars[k] = v
			}
		}

		err = s.upsertHistoryMode(db, thing, vars)
		if err != nil {
			return fmt.Errorf("batchHistoryUpdate failed: %w", err)
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

// Get a slice of the primary key values from the values map.
// The values are sorted by the column name so that it does not depend on the order of the columns in the table definition.
func (s *Server) getPKValues(values map[string]string, table *pb.Table) ([]any, error) {
	var pkColumns []*pb.Column
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkColumns = append(pkColumns, c)
		}
	}
	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("no primary key columns found for table %s", table.Name)
	}

	sort.Slice(pkColumns, func(i, j int) bool {
		return pkColumns[i].Name < pkColumns[j].Name
	})

	var pkValues []any
	for _, pkColumn := range pkColumns {
		pkValues = append(pkValues, values[pkColumn.Name])
	}

	return pkValues, nil
}

func (s *Server) upsertHistoryMode(db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) error {
	if _, found := vars["id"]; found {
		return fmt.Errorf("id is not allowed to be set in the vars")
	}

	res, err := surrealdb.Upsert[any](db, thing, vars)
	if err != nil {
		return fmt.Errorf("unable to upsert record %s: %w", thing, err)
	}

	if s.debugging() {
		s.logDebug("Added history record", "thing", thing, "vars", vars, "result", *res)
	}

	return nil
}

func (s *Server) upsertMerge(db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) (*[]surrealdb.QueryResult[any], error) {
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

	res, err := surrealdb.Query[any](db, `UPSERT type::thing($tb, $id) MERGE {`+content+`};`, varsWithTB)
	if err != nil {
		return nil, fmt.Errorf("unable to upsert merge record %s: %w", thing, err)
	}

	return res, nil
}

func (s *Server) getPreviousValues(db *surrealdb.DB, fields []string, table *pb.Table, pkColumns []string, pkValues []any) (map[string]interface{}, error) {
	// Get the previous values for the thing (where the SurrealDB table field that corresponds to the source table's primary key column matches, and its fivetran_active is true)
	var conds []string

	for _, col := range pkColumns {
		conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
	}

	byID := strings.Join(conds, " AND ")

	vars := map[string]interface{}{
		"tb":     table.Name,
		"fields": fields,
	}

	for i, col := range pkColumns {
		vars[col] = pkValues[i]
	}

	req, err := surrealdb.Query[[]map[string]interface{}](
		db,
		fmt.Sprintf(
			"SELECT type::fields($fields) FROM type::table($tb) WHERE %s AND fivetran_active = true;",
			byID,
		),
		vars,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get previous values for %s where %s: %w", table.Name, byID, err)
	}

	if len(*req) == 0 {
		return nil, fmt.Errorf("got empty query response while getting previous values found for %s where %s", table.Name, byID)
	}

	if len((*req)[0].Result) == 0 {
		// We assume the record has not been created yet.
		// The caller should omit the unmodified fields from the vars,
		// so that SurrealDB will create the record without those fields noted unmodified.
		return nil, nil
	}

	return (*req)[0].Result[0], nil
}
