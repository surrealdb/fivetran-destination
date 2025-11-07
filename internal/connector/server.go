package connector

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
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
	logging := &Logging{
		logger: logger,
	}

	// Get metrics interval from environment variable
	metricsInterval := 30 * time.Second
	if interval := os.Getenv("METRICS_LOG_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			metricsInterval = d
		}
	}

	return &Server{
		mu:      &sync.Mutex{},
		Logging: logging,
		metrics: NewMetricsCollector(logging, metricsInterval),
	}
}

type Server struct {
	pb.UnimplementedDestinationConnectorServer

	mu *sync.Mutex

	*Logging
	metrics *MetricsCollector
}

// Start initializes and starts the server components
func (s *Server) Start(ctx context.Context) {
	// Start metrics collection
	if s.metrics != nil {
		s.metrics.Start(ctx)
		s.logInfo("Metrics collection started", "interval", s.metrics.logInterval)
	}
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

	if _, err := s.connect(ctx, cfg, ""); err != nil {
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
	tb, err := s.infoForTable(ctx, req.SchemaName, req.TableName, req.Configuration)
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

	db, err := s.connect(ctx, cfg, req.SchemaName)
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
		if err := db.Close(ctx); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if err := s.defineTable(ctx, db, req.Table); err != nil {
		return &pb.CreateTableResponse{
			// success, warning, task
			Response: &pb.CreateTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	tbInfo, err := s.infoForTable(ctx, req.SchemaName, req.Table.Name, req.Configuration)
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

	db, err := s.connect(ctx, cfg, req.SchemaName)
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
		if err := db.Close(ctx); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if err := s.defineTable(ctx, db, req.Table); err != nil {
		return &pb.AlterTableResponse{
			Response: &pb.AlterTableResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	tbInfo, err := s.infoForTable(ctx, req.SchemaName, req.Table.Name, req.Configuration)
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
	return s.truncate(ctx, req)
}

// WriteBatch implements the WriteBatch method required by the DestinationConnectorServer interface
func (s *Server) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	return s.writeBatch(ctx, req)
}

func (s *Server) WriteHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	return s.writeHistoryBatch(ctx, req)
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

func (s *Server) Migrate(ctx context.Context, req *pb.MigrateRequest) (*pb.MigrateResponse, error) {
	if err := s.migrate(ctx, req); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}
	return &pb.MigrateResponse{
		Response: &pb.MigrateResponse_Success{
			Success: true,
		},
	}, nil
}
