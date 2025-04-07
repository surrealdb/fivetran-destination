package connector

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor
)

func NewServer() *Server {
	return &Server{
		mu: &sync.Mutex{},
	}
}

type Server struct {
	pb.UnimplementedDestinationConnectorServer

	connection *surrealdb.DB
	mu         *sync.Mutex
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
		Name:  "test-connection",
		Label: "Test Connection",
	})

	log.Printf("ConfigurationForm called")
	return &pb.ConfigurationFormResponse{
		Fields: fields,
		Tests:  tests,
	}, nil
}

// Capabilities implements the Capabilities method required by the DestinationConnectorServer interface
func (s *Server) Capabilities(ctx context.Context, req *pb.CapabilitiesRequest) (*pb.CapabilitiesResponse, error) {
	log.Printf("Capabilities called")
	return &pb.CapabilitiesResponse{
		// Or Parquet
		BatchFileFormat: pb.BatchFileFormat_CSV,
	}, nil
}

// Test implements the Test method required by the DestinationConnectorServer interface
//
// It basically checks if the provided configuration is valid,
// by trying to connect to the SurrealDB instance using the connection information
// included in the configuration.
func (s *Server) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	log.Printf("Test called with config named %q: %v", req.Name, req.Configuration)

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.TestResponse{
			// success, failure
			Response: &pb.TestResponse_Failure{
				Failure: err.Error(),
			},
		}, err
	}

	if _, err := s.connect(cfg, ""); err != nil {
		return &pb.TestResponse{
			// success, failure
			Response: &pb.TestResponse_Failure{
				Failure: err.Error(),
			},
		}, err
	}

	return &pb.TestResponse{
		// success, failure
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

// DescribeTable implements the DescribeTable method required by the DestinationConnectorServer interface
func (s *Server) DescribeTable(ctx context.Context, req *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	if s.debugging() {
		log.Printf("DescribeTable called for schema %q table %q: %v", req.SchemaName, req.TableName, req.Configuration)
	}
	tb, err := s.infoForTable(req.SchemaName, req.TableName, req.Configuration)
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

	if s.debugging() {
		log.Printf("infoForTable result: %v", tb)
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
		log.Printf("CreateTable called for schema: %s, table: %s", req.SchemaName, req.Table.Name)
	}

	cfg, err := s.parseConfig(req.Configuration)
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
	defer db.Close()

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
		log.Printf("infoForTable result: %v", tbInfo)
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
		log.Printf("AlterTable called for schema: %s, table: %s", req.SchemaName, req.Table.Name)
	}
	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.AlterTableResponse{
			// success, warning, task
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		log.Printf("AlterTable config: %v", cfg)
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.AlterTableResponse{
			// success, warning, task
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer db.Close()

	if err := s.defineTable(db, req.Table); err != nil {
		return &pb.AlterTableResponse{
			// success, warning, task
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
			// success, warning, task
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.debugging() {
		log.Printf("infoForTable result: %v", tbInfo)
	}

	return &pb.AlterTableResponse{
		// success, warning, task
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

// Truncate implements the Truncate method required by the DestinationConnectorServer interface
func (s *Server) Truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	if s.debugging() {
		log.Printf("Truncate called for schema: %s, table: %s", req.SchemaName, req.TableName)
		// SyncedColumn is e.g. `_sivetran_synced` which is timestamp-like column/field
		log.Printf("  SyncedColumn: %s", req.SyncedColumn)
		if req.Soft != nil {
			// DeletedColumn is e.g. `_sivetran_deleted` which is bool-like column/field
			log.Printf("  Soft.DeletedColumn: %s", req.Soft.DeletedColumn)
		}
		if req.UtcDeleteBefore != nil {
			log.Printf("  UtcDeleteBefore: %s", req.UtcDeleteBefore.AsTime().Format(time.RFC3339))
		}

		// You usually do something like:
		//   SOFT DELETE:  `UPDATE <table> SET _fivetran_deleted = true WHERE _fivetran_synced <= <UtcDeleteBefore>`
		//   HARD DELETE:  `DELETE FROM <table> WHERE _fivetran_synced <= <UtcDeleteBefore>`
	}

	return &pb.TruncateResponse{
		// success, warning, task
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

// WriteBatch implements the WriteBatch method required by the DestinationConnectorServer interface
func (s *Server) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	if s.debugging() {
		log.Printf("WriteBatch called for schema: %s, table: %s, config: %v", req.SchemaName, req.Table.Name, req.Configuration)
		log.Printf("  Replace files: %d, Update files: %d, Delete files: %d",
			len(req.ReplaceFiles), len(req.UpdateFiles), len(req.DeleteFiles))
		log.Printf("  Keys: %v", req.Keys)
		log.Printf("  FileParams.Compression: %v", req.FileParams.Compression)
		log.Printf("  FileParams.Encryption: %v", req.FileParams.Encryption)
		log.Printf("  FileParams.NullString: %v", req.FileParams.NullString)
		log.Printf("  FileParams.UnmodifiedString: %v", req.FileParams.UnmodifiedString)
	}

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer db.Close()

	if s.debugging() {
		log.Printf("WriteBatch using namespace %s and database %s", cfg.ns, req.SchemaName)
	}

	tb, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
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
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchUpdate(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchDelete(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	return &pb.WriteBatchResponse{
		// success, warning, task
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

// Reads CSV files and replaces existing records accordingly.
func (s *Server) batchReplace(db *surrealdb.DB, fields map[string]columnInfo, replaceFiles []string, fileParams *pb.FileParams, keys map[string][]byte, table *pb.Table) error {
	unmodifiedString := fileParams.UnmodifiedString
	return s.processCSVRecords(replaceFiles, fileParams, keys, func(columns []string, record []string) error {
		log.Printf("  Replacing record: %v %v", columns, record)

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		var id any
		if v, ok := values["_fivetran_id"]; ok {
			id = v
		} else if v, ok := values["id"]; ok {
			id = v
		} else {
			return fmt.Errorf("id nor _fivetran_id not found in the record: %v", values)
		}

		thing := models.NewRecordID(table.Name, id)

		vars := map[string]interface{}{}
		for k, v := range values {
			if unmodifiedString != "" && v == unmodifiedString {
				log.Printf("  Skipping column %s with value %s", k, v)
				continue
			}

			if k == "id" {
				log.Printf("Skipping id")
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

		log.Printf("  Replaced record %s with %v: %+v", thing, vars, *res)

		return nil
	})
}

// Reads CSV files and updates existing records accordingly.
func (s *Server) batchUpdate(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
	unmodifiedString := req.FileParams.UnmodifiedString

	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		log.Printf("  Updating record: %v %v", columns, record)

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		thing := fmt.Sprintf("%s:%s", req.Table.Name, values["_fivetran_id"])

		vars := map[string]interface{}{}
		for k, v := range values {
			if unmodifiedString != "" && v == unmodifiedString {
				log.Printf("  Skipping column %s with value %s", k, v)
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

		res, err := surrealdb.Upsert[any](db, thing, vars)
		if err != nil {
			return err
		}

		log.Printf("  Updated record %s with %v: %+v", thing, vars, *res)

		return nil
	})
}

// Reads CSV files and deletes existing records accordingly.
func (s *Server) batchDelete(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
	return s.processCSVRecords(req.DeleteFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		log.Printf("  Deleting record: %v %v", columns, record)

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

		log.Printf("  Deleted record %s: %+v", thing, res)

		return nil
	})
}

func (s *Server) WriteHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Printf("WriteHistoryBatch called for schema: %s, table: %s", req.SchemaName, req.Table.Name)
	log.Printf("  Earliest start files: %d, Replace files: %d, Update files: %d, Delete files: %d",
		len(req.EarliestStartFiles), len(req.ReplaceFiles), len(req.UpdateFiles), len(req.DeleteFiles))
	log.Printf("  FileParams.Compression: %v", req.FileParams.Compression)
	log.Printf("  FileParams.Encryption: %v", req.FileParams.Encryption)
	log.Printf("  FileParams.NullString: %v", req.FileParams.NullString)
	log.Printf("  FileParams.UnmodifiedString: %v", req.FileParams.UnmodifiedString)

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	db, err := s.connect(cfg, req.SchemaName)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer db.Close()

	if s.debugging() {
		log.Printf("WriteHistoryBatch using namespace %s and database %s", cfg.ns, req.SchemaName)
	}

	tb, err := s.infoForTable(req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
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

	if err := s.batchProcessEarliestStartFiles(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	// for _, files := range [][]string{req.ReplaceFiles, req.UpdateFiles, req.DeleteFiles} {
	if err := s.batchReplace(db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchHistoryUpdate(db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	if err := s.batchReplace(db, fields, req.DeleteFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
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
		log.Printf("  Processing earliest start file: %v %v", columns, record)

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		log.Printf("  Earliest start record: values %v", values)

		// We have `id` and `_fivetran_start`.
		// Let's remove everything all the records with the same `id`,
		// whose `_fivetran_start` is GREATER THAN this `_fivetran_start`.

		var id any
		if v, ok := values["_fivetran_id"]; ok {
			id = v
		} else if v, ok := values["id"]; ok {
			id = v
		} else {
			return fmt.Errorf("id nor _fivetran_id not found in the record: %v", values)
		}

		thing := models.NewRecordID(req.Table.Name, id)

		vars := map[string]interface{}{}
		for k, v := range values {
			if k == "id" {
				log.Printf("Skipping id")
				continue
			}

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
		vars["rc"] = models.NewRecordID(req.Table.Name, id)

		res, err := surrealdb.Query[any](
			db,
			"DELETE FROM type::table($tb) WHERE id = type::record($rc) AND _fivetran_start > type::datetime($_fivetran_start);",
			vars,
		)
		if err != nil {
			return fmt.Errorf("unable to upsert record %s: %w", thing, err)
		}

		log.Printf("  Removed records with id %s and _fivetran_start greater than %s: %+v", id, vars["_fivetran_start"], *res)

		return nil
	})
}

func (s *Server) batchHistoryUpdate(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		log.Printf("  Processing update file: %v %v", columns, record)

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		log.Printf("  Update record: values %v", values)

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
				log.Printf("Skipping id")
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

		// Get the preivous values to populate the fields with values set to the unmodeified string
		previousFieldsAndValues, err := s.getPreviousValues(db, thing, unmodifiedFields)
		if err != nil {
			return fmt.Errorf("unable to get previous values for record %s: %w", thing, err)
		}

		for k, v := range previousFieldsAndValues {
			vars[k] = v
		}

		res, err := surrealdb.Upsert[any](db, thing, vars)
		if err != nil {
			return fmt.Errorf("unable to upsert record %s: %w", thing, err)
		}

		log.Printf("  Added record %s with %v: %+v", thing, vars, *res)

		return nil
	})
}

func (s *Server) getPreviousValues(db *surrealdb.DB, thing models.RecordID, fields []string) (map[string]interface{}, error) {
	req, err := surrealdb.Query[map[string]interface{}](
		db,
		"SELECT $fields FROM type::record($rc) WHERE id = type::record($rc);",
		map[string]interface{}{
			"rc":     thing,
			"fields": fields,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get previous values for record %s: %w", thing, err)
	}

	return req.Result, nil
}
