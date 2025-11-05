package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	"gopkg.in/yaml.v3"
)

type DBValidator struct {
	db *surrealdb.DB
}

func NewDBValidator() (*DBValidator, error) {
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
	}

	ctx := context.Background()

	db, err := surrealdb.FromEndpointURLString(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %v", err)
	}

	if err := db.Use(ctx, namespace, database); err != nil {
		return nil, fmt.Errorf("failed to use namespace and database: %v", err)
	}

	if token != "" {
		// If token is provided, use it for authentication
		if err := db.Authenticate(ctx, token); err != nil {
			return nil, fmt.Errorf("failed to authenticate with token: %v", err)
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

		// Sign in as a namespace, database, or root user
		auth := &surrealdb.Auth{
			Username: username,
			Password: password,
		}
		token, err := db.SignIn(ctx, auth)
		if err != nil {
			return nil, fmt.Errorf("failed to sign in to SurrealDB: %v", err)
		}

		// Authenticate the connection
		if err := db.Authenticate(ctx, token); err != nil {
			return nil, fmt.Errorf("failed to authenticate with SurrealDB: %v", err)
		}
	}

	return &DBValidator{db: db}, nil
}

func loadExpectedTables(expectedPath string) (*ExpectedDBState, error) {
	// Read expected state from YAML to get table names
	expectedData, err := os.ReadFile(expectedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read expected state file: %v", err)
	}

	var expected ExpectedDBState
	if err := yaml.Unmarshal(expectedData, &expected); err != nil {
		return nil, fmt.Errorf("failed to parse expected state YAML: %v", err)
	}

	return &expected, nil
}

func (v *DBValidator) DumpCurrentState(ctx context.Context, tableNames []string) (map[string][]map[string]interface{}, error) {
	result := make(map[string][]map[string]interface{})
	for _, tableName := range tableNames {
		// Query all records from the table
		records, err := surrealdb.Select[[]map[string]interface{}](ctx, v.db, models.Table(tableName))
		if err != nil {
			return nil, fmt.Errorf("failed to select from table %s: %v", tableName, err)
		}

		log.Printf("SELECT * FROM %s: %v", tableName, *records)

		result[tableName] = *records
	}

	return result, nil
}

func (v *DBValidator) CompareWithExpected(ctx context.Context, expectedPath string) error {
	// Load expected tables from YAML
	expected, err := loadExpectedTables(expectedPath)
	if err != nil {
		return fmt.Errorf("failed to load expected tables: %v", err)
	}

	// Get current state
	tableNames := make([]string, 0, len(expected.Tables))
	for tableName := range expected.Tables {
		tableNames = append(tableNames, tableName)
	}
	current, err := v.DumpCurrentState(ctx, tableNames)
	if err != nil {
		return fmt.Errorf("failed to get current state: %v", err)
	}

	var validationErrors []string

	// Compare states
	for tableName, expectedRecords := range expected.Tables {
		currentRecords, exists := current[tableName]
		if !exists {
			validationErrors = append(validationErrors, fmt.Sprintf("table %s exists in expected state but not in current state", tableName))
			continue
		}

		// Change all the DateTime to UTC within the current records
		// This is to make the expected.yaml files easy to maintain by using UTC time for
		// all the timestamps.
		for _, record := range currentRecords {
			for key, value := range record {
				if value, ok := value.(models.CustomDateTime); ok {
					record[key] = models.CustomDateTime{Time: value.UTC()}
				}
			}
		}

		// Convert expected records to regular maps for comparison
		expectedMaps := make([]map[string]interface{}, len(expectedRecords))
		for i, record := range expectedRecords {
			expectedMaps[i] = ConvertToMap(record)
		}

		// Create a custom comparer that handles RecentEnoughTime vs CustomDateTime comparison
		comparer := cmp.Comparer(func(x, y interface{}) bool {
			// If x is RecentEnoughTime, check if y is CustomDateTime and within range
			if ret, ok := x.(RecentEnoughTime); ok {
				if cdt, ok := y.(models.CustomDateTime); ok {
					return ret.IsRecentEnough(cdt.Time)
				}
			}
			// If y is RecentEnoughTime, check if x is CustomDateTime and within range
			if ret, ok := y.(RecentEnoughTime); ok {
				if cdt, ok := x.(models.CustomDateTime); ok {
					return ret.IsRecentEnough(cdt.Time)
				}
			}
			// For all other cases, use default equality
			return x == y
		})

		filtered := cmp.FilterValues(func(x, y interface{}) bool {
			_, xIsRecentEnough := x.(RecentEnoughTime)
			_, yIsCustom := y.(models.CustomDateTime)
			_, xIsCustom := x.(models.CustomDateTime)
			_, yIsRecentEnough := y.(RecentEnoughTime)
			return (xIsRecentEnough && yIsCustom) || (yIsRecentEnough && xIsCustom)
		}, comparer)

		diff := cmp.Diff(expectedMaps, currentRecords, filtered)
		if diff != "" {
			validationErrors = append(validationErrors, fmt.Sprintf("mismatch in table %s:\nExpected: -\nGot: +\n%s",
				tableName, diff))
		}
	}

	// Check for extra tables in current state
	for tableName := range current {
		if _, exists := expected.Tables[tableName]; !exists {
			validationErrors = append(validationErrors, fmt.Sprintf("table %s exists in current state but not in expected state", tableName))
		}
	}

	if len(validationErrors) > 0 {
		return fmt.Errorf("validation failed with %d errors:\n%s", len(validationErrors), strings.Join(validationErrors, "\n\n"))
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: db-validator <path-to-expected-state.yaml>")
	}

	expectedPath := os.Args[1]
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		log.Fatalf("Expected state file not found: %s", expectedPath)
	}

	validator, err := NewDBValidator()
	if err != nil {
		log.Fatalf("Failed to create validator: %v", err)
	}

	ctx := context.Background()

	if err := validator.CompareWithExpected(ctx, expectedPath); err != nil {
		log.Fatalf("Validation failed: %v", err)
	}

	fmt.Println("Database state matches expected state!")
}
