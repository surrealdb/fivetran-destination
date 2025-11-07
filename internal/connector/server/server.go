package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/surrealdb/fivetran-destination/internal/connector/log"
	"github.com/surrealdb/fivetran-destination/internal/connector/metrics"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	_ "google.golang.org/grpc/encoding/gzip"
)

func New(logger zerolog.Logger) *Server {
	logging := &log.Logging{
		Logger: logger,
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
		metrics: metrics.NewCollector(logging, metricsInterval),
	}
}

type Server struct {
	pb.UnimplementedDestinationConnectorServer

	mu *sync.Mutex

	*log.Logging
	metrics *metrics.Collector
}

// Start initializes and starts the server components
func (s *Server) Start(ctx context.Context) {
	// Start metrics collection
	if s.metrics != nil {
		s.metrics.Start(ctx)
		s.LogInfo("Metrics collection started", "interval", s.metrics.LogInterval)
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
		Name:        "auth_level",
		Label:       "Authentication Level",
		Description: stringPtr("Select the authentication level to use when signing in to SurrealDB using user/pass. Leave blank if using token authentication."),
		Required:    boolPtr(false),
		Type: &pb.FormField_DropdownField{DropdownField: &pb.DropdownField{
			DropdownField: []string{AuthLevelIDRoot, AuthLevelIDNamespace},
		}},
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

	if s.Debugging() {
		s.LogDebug("ConfigurationForm called")
	}
	return &pb.ConfigurationFormResponse{
		Fields: fields,
		Tests:  tests,
	}, nil
}

// Capabilities implements the Capabilities method required by the DestinationConnectorServer interface
func (s *Server) Capabilities(ctx context.Context, req *pb.CapabilitiesRequest) (*pb.CapabilitiesResponse, error) {
	if s.Debugging() {
		s.LogDebug("Capabilities called")
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
	s.LogDebug("Starting configuration test",
		"config_name", req.Name,
		"config", req.Configuration)

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		s.LogSevere("Failed to parse test configuration", err,
			"config_name", req.Name)
		return &pb.TestResponse{
			Response: &pb.TestResponse_Failure{
				Failure: fmt.Sprintf("failed parsing test config: %v", err.Error()),
			},
		}, err
	}

	if _, err := s.connect(ctx, cfg); err != nil {
		s.LogSevere("Failed to connect to database", err,
			"config_name", req.Name)
		return &pb.TestResponse{
			Response: &pb.TestResponse_Failure{
				Failure: err.Error(),
			},
		}, err
	}

	s.LogDebug("Finished configuration test",
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
	if s.Debugging() {
		s.LogDebug("DescribeTable called", "schema", req.SchemaName, "table", req.TableName, "config", req.Configuration)
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

	if s.Debugging() {
		s.LogDebug("infoForTable result", "table_info", tb)
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
	if s.Debugging() {
		s.LogDebug("CreateTable called", "schema", req.SchemaName, "table", req.Table.Name)
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

	db, err := s.connectAndUse(ctx, cfg, req.SchemaName)
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
			s.LogWarning("failed to close db", err)
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

	if s.Debugging() {
		s.LogDebug("infoForTable result", "table_info", tbInfo)
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
	if s.Debugging() {
		s.LogDebug("AlterTable called", "schema", req.SchemaName, "table", req.Table.Name)
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

	if s.Debugging() {
		s.LogDebug("AlterTable config", "config", cfg)
	}

	db, err := s.connectAndUse(ctx, cfg, req.SchemaName)
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
			s.LogWarning("failed to close db", err)
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

	if s.Debugging() {
		s.LogDebug("infoForTable result", "table_info", tbInfo)
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
