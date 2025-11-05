package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/surrealdb/surrealdb.go"
	"gopkg.in/yaml.v3"
)

type Expected struct {
	Tables map[string][]map[string]interface{} `yaml:"tables"`
}

func main() {
	// Parse command line flags
	expectedFile := flag.String("f", "", "Path to expected.yaml file")
	tables := flag.String("t", "", "Comma-separated list of table names to truncate")
	flag.Parse()

	if *expectedFile == "" && *tables == "" {
		fmt.Fprintf(os.Stderr, "Error: Must specify either -f <expected.yaml> or -t <table1,table2,...>\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s -f path/to/expected.yaml\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -t table1,table2,table3\n", os.Args[0])
		os.Exit(1)
	}

	var tablesToTruncate []string
	if *tables != "" {
		// If tables are specified via command line, use those
		tablesToTruncate = strings.Split(*tables, ",")
		log.Printf("Will truncate specified tables: %v", tablesToTruncate)
	} else {
		// Otherwise read from expected.yaml
		log.Printf("Reading tables from %s", *expectedFile)
		yamlFile, err := os.ReadFile(*expectedFile)
		if err != nil {
			log.Fatalf("Error reading YAML file: %v", err)
		}

		var expected Expected
		err = yaml.Unmarshal(yamlFile, &expected)
		if err != nil {
			log.Fatalf("Error parsing YAML file: %v", err)
		}

		// Extract table names from the expected state
		for tableName := range expected.Tables {
			tablesToTruncate = append(tablesToTruncate, tableName)
		}
		log.Printf("Will truncate tables from expected.yaml: %v", tablesToTruncate)
	}

	// Get SurrealDB connection details from environment variables
	endpoint := os.Getenv("SURREALDB_ENDPOINT")
	token := os.Getenv("SURREALDB_TOKEN")
	namespace := os.Getenv("SURREALDB_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}
	database := os.Getenv("SURREALDB_DATABASE")
	if database == "" {
		database = "test"
	}

	// If endpoint is not set, fall back to local instance
	if endpoint == "" {
		endpoint = "ws://localhost:8000/rpc"
	} else {
		if !strings.HasSuffix(endpoint, "/rpc") {
			endpoint = endpoint + "/rpc"
		}
	}

	ctx := context.Background()

	// Connect to SurrealDB
	db, err := surrealdb.FromEndpointURLString(ctx, endpoint)
	if err != nil {
		log.Fatalf("Error connecting to SurrealDB at %s: %v", endpoint, err)
	}
	defer db.Close(ctx)

	// Use namespace and database first
	err = db.Use(ctx, namespace, database)
	if err != nil {
		log.Fatalf("Error selecting namespace/database: %v", err)
	}

	if token != "" {
		// If token is provided, use it for authentication
		err = db.Authenticate(ctx, token)
		if err != nil {
			log.Fatalf("Error authenticating with token: %v", err)
		}
	} else {
		// Otherwise use username/password authentication
		username := os.Getenv("SURREALDB_USERNAME")
		if username == "" {
			username = "root"
		}
		password := os.Getenv("SURREALDB_PASSWORD")
		if password == "" {
			password = "root"
		}

		// Sign in
		token, err := db.SignIn(ctx, map[string]interface{}{
			"user": username,
			"pass": password,
		})
		if err != nil {
			log.Fatalf("Error signing in to SurrealDB: %v", err)
		}

		// Authenticate the connection
		err = db.Authenticate(ctx, token)
		if err != nil {
			log.Fatalf("Error authenticating with SurrealDB: %v", err)
		}
	}

	// Truncate tables
	for _, tableName := range tablesToTruncate {
		query := fmt.Sprintf("DELETE %s", tableName)
		_, err := surrealdb.Query[any](ctx, db, query, map[string]interface{}{})
		if err != nil {
			log.Printf("Error truncating table %s: %v", tableName, err)
		} else {
			fmt.Printf("Successfully truncated table %s\n", tableName)
		}
	}
}
