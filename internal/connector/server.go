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
		Description: stringPtr("Input the user name for the SurrealDB instance"),
		Required:    boolPtr(true),
		Type:        &pb.FormField_TextField{TextField: pb.TextField_PlainText},
	})

	fields = append(fields, &pb.FormField{
		Name:        "pass",
		Label:       "Password",
		Placeholder: stringPtr("password"),
		Description: stringPtr("Input the password for the SurrealDB instance"),
		Required:    boolPtr(true),
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
		// SyncedColumn is e.g. `_sivetran_synced`
		log.Printf("  SyncedColumn: %s", req.SyncedColumn)
		if req.Soft != nil {
			// DeletedColumn is e.g. `_sivetran_deleted`
			log.Printf("  Soft.DeletedColumn: %s", req.Soft.DeletedColumn)
		}
		if req.UtcDeleteBefore != nil {
			log.Printf("  UtcDeleteBefore: %s", req.UtcDeleteBefore.AsTime().Format(time.RFC3339))
		}
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

	if err := s.batchReplace(db, fields, req); err != nil {
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
func (s *Server) batchReplace(db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteBatchRequest) error {
	unmodifiedString := req.FileParams.UnmodifiedString
	return s.processCSVRecords(req.ReplaceFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		log.Printf("  Replacing record: %v %v", columns, record)

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
				return err
			}

			vars[k] = typedV
		}

		type UpsertResponse struct {
			ID     int                    `json:"id"`
			Result map[string]interface{} `json:"result"`
		}
		var res connection.RPCResponse[UpsertResponse]

		err := db.Send(&res, "upsert", thing, vars)
		if err != nil {
			return err
		}

		log.Printf("  Replced record: %+v", res)

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

		type UpsertResponse struct {
			ID     int                    `json:"id"`
			Result map[string]interface{} `json:"result"`
		}
		var res connection.RPCResponse[UpsertResponse]

		err := db.Send(&res, "upsert", thing, vars)
		if err != nil {
			return err
		}

		log.Printf("  Updated record: %+v", res)

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

		log.Printf("  Deleted record: %+v", res)

		return nil
	})
}

func (s *Server) WriteHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Printf("WriteHistoryBatch called for schema: %s, table: %s", req.SchemaName, req.Table.Name)
	log.Printf("  Earliest start files: %d, Replace files: %d, Update files: %d, Delete files: %d",
		len(req.EarliestStartFiles), len(req.ReplaceFiles), len(req.UpdateFiles), len(req.DeleteFiles))

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}
