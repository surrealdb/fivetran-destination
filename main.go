package main

import (
	"context"
	"crypto/cipher"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor
)

var (
	port = flag.Int("port", 50052, "The server port")
)

type server struct {
	pb.UnimplementedDestinationConnectorServer

	connection *surrealdb.DB
	mu         *sync.Mutex
}

// ConfigurationForm implements the ConfigurationForm method required by the DestinationConnectorServer interface
func (s *server) ConfigurationForm(ctx context.Context, req *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
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
func (s *server) Capabilities(ctx context.Context, req *pb.CapabilitiesRequest) (*pb.CapabilitiesResponse, error) {
	log.Printf("Capabilities called")
	return &pb.CapabilitiesResponse{
		// Or Parquet
		BatchFileFormat: pb.BatchFileFormat_CSV,
	}, nil
}

type config struct {
	url  string
	user string
	pass string
	ns   string
}

func (c *config) validate() error {
	var missingFields []string

	if c.url == "" {
		missingFields = append(missingFields, "url")
	}
	if c.user == "" {
		missingFields = append(missingFields, "user")
	}
	if c.pass == "" {
		missingFields = append(missingFields, "pass")
	}
	if c.ns == "" {
		missingFields = append(missingFields, "ns")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}

	return nil
}

// Test implements the Test method required by the DestinationConnectorServer interface
//
// It basically checks if the provided configuration is valid,
// by trying to connect to the SurrealDB instance using the connection information
// included in the configuration.
func (s *server) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
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
func (s *server) DescribeTable(ctx context.Context, req *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	log.Printf("DescribeTable called for schema %q table %q: %v", req.SchemaName, req.TableName, req.Configuration)

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

func (s *server) debugging() bool {
	return os.Getenv("SURREAL_FIVETRAN_DEBUG") != ""
}

type tableInfo struct {
	columns []columnInfo
}

type columnInfo struct {
	Name       string
	Type       string
	PrimaryKey bool
}

func (s *server) infoForTable(schemaName string, tableName string, configuration map[string]string) (tableInfo, error) {
	cfg, err := s.parseConfig(configuration)
	if err != nil {
		return tableInfo{}, err
	}

	db, err := s.connect(cfg, schemaName)
	if err != nil {
		return tableInfo{}, err
	}
	defer db.Close()

	// the result is formatted like:
	// {
	// 	"events": {},
	// 	"fields": {
	// 		"name": "DEFINE FIELD name ON user TYPE string PERMISSIONS FULL"
	// 	},
	// 	"indexes": {},
	// 	"lives": {},
	// 	"tables": {}
	// }
	type infoForTableResult struct {
		Fields map[string]string `json:"fields"`
	}

	if err := validateTableName(tableName); err != nil {
		return tableInfo{}, err
	}

	query := fmt.Sprintf(`INFO FOR TABLE %s;`, tableName)

	var result infoForTableResult
	if err := db.Send(&result, "query", query); err != nil {
		return tableInfo{}, err
	}

	columns := []columnInfo{}
	for _, field := range result.Fields {
		i := strings.Index(field, "TYPE")
		if i == -1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		// the right part is the column name
		tpeStart := strings.Index(field[i:], " ") + i + 1
		if tpeStart < i+1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		tpeEnd := strings.Index(field[tpeStart:], " ") + tpeStart
		if tpeEnd < tpeStart {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		tpe := field[tpeStart:tpeEnd]

		// the left part is the column name
		colEnd := strings.Index(field[:i], " ")
		if colEnd == -1 {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		colStart := strings.LastIndex(field[:colEnd], " ") + 1
		if colStart > colEnd {
			return tableInfo{}, fmt.Errorf("invalid field: %s", field)
		}

		col := field[colStart:colEnd]

		columns = append(columns, columnInfo{
			Name: col,
			Type: tpe,
		})
	}

	return tableInfo{
		columns: columns,
	}, nil
}

func (s *server) columnsFromSurrealToFivetran(sColumns []columnInfo) ([]*pb.Column, error) {
	var ftColumns []*pb.Column

	for _, c := range sColumns {
		var pbDataType pb.DataType
		switch c.Type {
		case "string":
			pbDataType = pb.DataType_STRING
		case "int":
			pbDataType = pb.DataType_INT
		case "float":
			pbDataType = pb.DataType_FLOAT
		case "double":
			pbDataType = pb.DataType_DOUBLE
		case "boolean":
			pbDataType = pb.DataType_BOOLEAN
		default:
			return nil, fmt.Errorf("unsupported data type: %s", c.Type)
		}
		ftColumns = append(ftColumns, &pb.Column{
			Name: c.Name,
			Type: pbDataType,
		})
	}

	return ftColumns, nil
}

// CreateTable implements the CreateTable method required by the DestinationConnectorServer interface
func (s *server) CreateTable(ctx context.Context, req *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	log.Printf("CreateTable called for schema: %s, table: %s", req.SchemaName, req.Table.Name)

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

	return &pb.CreateTableResponse{
		// success, warning, task
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

// parseConfig parses the Fivetran connector configuration and returns a config instance
func (s *server) parseConfig(configuration map[string]string) (config, error) {
	cfg := config{
		url:  configuration["url"],
		ns:   configuration["ns"],
		user: configuration["user"],
		pass: configuration["pass"],
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

// connect connects to SurrealDB and returns a DB instance
// The caller is responsible for "Use"ing ns/db after calling this function
func (s *server) connect(cfg config, schema string) (*surrealdb.DB, error) {
	db, err := surrealdb.New(cfg.url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	// Note that we assume Fivetran `schema` is the same as SurrealDB `database`.
	// So we treat SurrealDB `namespace` as a global setting, that limits every operation from this
	// connector to SurrealDB within the namespace.
	//
	// If you read this connector's implementation,
	// you'll notice Fivetran calls our RPCs like `hey, create a table named <schema>.<table>`,
	// and we interpret it as `ok let's create a table <table> in namespace <schema>`.
	if err := db.Use(cfg.ns, schema); err != nil {
		return nil, fmt.Errorf("failed to use namespace %s: %w", cfg.ns, err)
	}

	token, err := db.SignIn(&surrealdb.Auth{
		Username: cfg.user,
		Password: cfg.pass,
		// Use `Use` instead of setting `Namespace` and `Database` here.
		// Otherwise, you end up with: failed to sign in to SurrealDB: namespace or database or both are not set
		// Probably related to https://github.com/surrealdb/surrealdb.node/issues/26#issuecomment-2057102554
		// Namespace: cfg.ns,
		// Database:  cfg.db,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	// If you end up panicking here like `panic: cbor: 18 bytes of extraneous data starting at index 21`,
	// Use the WebSocket endpoint instead of the HTTP endpoint.
	// See https://github.com/surrealdb/surrealdb.go/pull/201
	//
	// Just for anyone reading this, by HTTP and WebSocket endpoints, I mean `http://localhost:8000/rpc` and `ws://localhost:8000/rpc`
	// respectively.
	if err := db.Authenticate(token); err != nil {
		return nil, fmt.Errorf("failed to authenticate with SurrealDB: %w", err)
	}

	return db, nil
}

func (s *server) createTable(db *surrealdb.DB, tableName string) error {
	type Schemaless struct {
	}
	_, err := surrealdb.Create[Schemaless](db, models.Table(tableName), map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}
	return nil
}

func (s *server) defineTable(db *surrealdb.DB, table *pb.Table) error {
	type TableData struct {
		Table string `json:"table"`
	}
	var ver connection.RPCResponse[TableData]
	if err := validateTableName(table.Name); err != nil {
		return err
	}
	tb := table.Name
	var query string
	if len(table.Columns) > 0 {
		query = fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMAFULL;`, tb)
	} else {
		query = fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMALESS;`, tb)
	}
	if err := db.Send(&ver, "query", query); err != nil {
		return err
	}

	for _, c := range table.Columns {
		if err := validateColumnName(c.Name); err != nil {
			return err
		}
		q, err := s.defineFieldQueryFromFt(tb, c)
		if err != nil {
			return err
		}
		if err := db.Send(&ver, "query", q); err != nil {
			return err
		}
	}

	return nil
}

// The only allowed characters are alphanumeric and underscores.
func validateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("column name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("column name contains invalid characters")
		}
	}
	return nil
}

// See https://surrealdb.com/docs/surrealql/datamodel#data-types for
// available SurrealDB data types.
type typeMapping struct {
	sdb string
	ft  pb.DataType
}

var typeMappings = []typeMapping{
	{
		sdb: "string",
		ft:  pb.DataType_STRING,
	},
	{
		sdb: "int",
		ft:  pb.DataType_INT,
	},
	{
		sdb: "int",
		ft:  pb.DataType_SHORT,
	},
	{
		sdb: "int",
		ft:  pb.DataType_LONG,
	},
	{
		sdb: "bytes",
		ft:  pb.DataType_BINARY,
	},
	{
		sdb: "float",
		ft:  pb.DataType_FLOAT,
	},
	{
		sdb: "float",
		ft:  pb.DataType_DOUBLE,
	},
	{
		sdb: "bool",
		ft:  pb.DataType_BOOLEAN,
	},
	{
		sdb: "decimal",
		ft:  pb.DataType_DECIMAL,
	},
	{
		sdb: "datetime",
		ft:  pb.DataType_UTC_DATETIME,
	},
}

func (s *server) defineFieldQueryFromFt(tb string, c *pb.Column) (string, error) {
	t := `DEFINE FIELD %s on %s TYPE %s;`

	var sdb string
	for _, m := range typeMappings {
		if m.ft == c.Type {
			sdb = m.sdb
			break
		}
	}

	if sdb == "" {
		return "", fmt.Errorf("unsupported data type: %s", c.Type)
	}

	return fmt.Sprintf(t, tb, c.Name, sdb), nil
}

// The only allowed characters are alphanumeric and underscores.
func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("table name contains invalid characters")
		}
	}
	return nil
}

// AlterTable implements the AlterTable method required by the DestinationConnectorServer interface
func (s *server) AlterTable(ctx context.Context, req *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	log.Printf("AlterTable called for schema: %s, table: %s", req.SchemaName, req.Table.Name)

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

	return &pb.AlterTableResponse{
		// success, warning, task
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

// Truncate implements the Truncate method required by the DestinationConnectorServer interface
func (s *server) Truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
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

	return &pb.TruncateResponse{
		// success, warning, task
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

// WriteBatch implements the WriteBatch method required by the DestinationConnectorServer interface
func (s *server) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Printf("WriteBatch called for schema: %s, table: %s, config: %v", req.SchemaName, req.Table.Name, req.Configuration)
	log.Printf("  Replace files: %d, Update files: %d, Delete files: %d",
		len(req.ReplaceFiles), len(req.UpdateFiles), len(req.DeleteFiles))
	log.Printf("  Keys: %v", req.Keys)
	log.Printf("  FileParams.Compression: %v", req.FileParams.Compression)
	log.Printf("  FileParams.Encryption: %v", req.FileParams.Encryption)
	log.Printf("  FileParams.NullString: %v", req.FileParams.NullString)
	log.Printf("  FileParams.UnmodifiedString: %v", req.FileParams.UnmodifiedString)

	if err := s.batchReplace(req.ReplaceFiles, req.FileParams, req.Keys); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchUpdate(req.UpdateFiles, req.FileParams, req.Keys); err != nil {
		return &pb.WriteBatchResponse{
			// success, warning, task
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if err := s.batchDelete(req.DeleteFiles, req.FileParams, req.Keys); err != nil {
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
func (s *server) batchReplace(files []string, fileParams *pb.FileParams, keys map[string][]byte) error {
	return s.processCSVRecords(files, fileParams, keys, func(columns []string, record []string) error {
		log.Printf("  Replacing record: %v %v", columns, record)
		return nil
	})
}

// Reads CSV files and updates existing records accordingly.
func (s *server) batchUpdate(files []string, fileParams *pb.FileParams, keys map[string][]byte) error {
	return s.processCSVRecords(files, fileParams, keys, func(columns []string, record []string) error {
		log.Printf("  Updating record: %v %v", columns, record)
		return nil
	})
}

// Reads CSV files and deletes existing records accordingly.
func (s *server) batchDelete(files []string, fileParams *pb.FileParams, keys map[string][]byte) error {
	return s.processCSVRecords(files, fileParams, keys, func(columns []string, record []string) error {
		log.Printf("  Deleting record: %v %v", columns, record)
		return nil
	})
}

func (s *server) processCSVRecords(files []string, fileParams *pb.FileParams, keys map[string][]byte, process func(columns []string, record []string) error) error {
	for _, f := range files {
		r, err := s.openFivetranFile(f, fileParams, keys)
		if err != nil {
			return fmt.Errorf("failed to open fivetran file: %w", err)
		}
		defer r.Close()

		cr := csv.NewReader(r)

		// TODO: ReuseRecord to avoid allocating a new slice for each record?

		columns, err := cr.Read()
		if err != nil {
			return fmt.Errorf("failed to read csv columns: %w", err)
		}

		for {
			record, err := cr.Read()
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed to read csv record: %w", err)
			}
			if err == io.EOF {
				break
			}

			if err := process(columns, record); err != nil {
				return fmt.Errorf("failed to process csv record: %w", err)
			}
		}
	}
	return nil
}

// Returns a decrypted and decompressed stream of the file content.
// The original file is compressed using zstd, and then encrypted.
// The encryption algorithm is specified in fileParams.Encryption.
// The key is specified in keys.
// In case of the CBC mode of AES, iv is prepended to the ciphertext within the file.
//
// It's the caller's responsibility to close the returned reader.
func (s *server) openFivetranFile(file string, fileParams *pb.FileParams, keys map[string][]byte) (io.ReadCloser, error) {
	key, ok := keys[file]
	if !ok {
		return nil, fmt.Errorf("key not found for file: %s", file)
	}

	r, err := NewFivetranFileReader(file, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create fivetran file reader: %w", err)
	}

	return r, nil
}

type cipherReader struct {
	file  *os.File
	block cipher.Block
}

var _ io.ReadCloser = &cipherReader{}

func (r *cipherReader) Read(p []byte) (n int, err error) {
	return r.file.Read(p)
}

func (r *cipherReader) Close() error {
	return r.file.Close()
}

func (s *server) WriteHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Printf("WriteHistoryBatch called for schema: %s, table: %s", req.SchemaName, req.Table.Name)
	log.Printf("  Earliest start files: %d, Replace files: %d, Update files: %d, Delete files: %d",
		len(req.EarliestStartFiles), len(req.ReplaceFiles), len(req.UpdateFiles), len(req.DeleteFiles))

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server with increased message size limits
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*50), // 50MB
		grpc.MaxSendMsgSize(1024*1024*50), // 50MB
	)
	srv := &server{
		mu: &sync.Mutex{},
	}

	pb.RegisterDestinationConnectorServer(s, srv)

	log.Printf("Starting SurrealDB destination connector on port %d", *port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
