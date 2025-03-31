package main

import (
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
	db, err := surrealdb.New("ws://localhost:8000/rpc")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %v", err)
	}

	if err := db.Use("testns", "tester"); err != nil {
		return nil, fmt.Errorf("failed to use namespace and database: %v", err)
	}

	// Sign in as a namespace, database, or root user
	auth := &surrealdb.Auth{
		Username: "root",
		Password: "root",
	}
	token, err := db.SignIn(auth)
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %v", err)
	}

	// Authenticate the connection
	if err := db.Authenticate(token); err != nil {
		return nil, fmt.Errorf("failed to authenticate with SurrealDB: %v", err)
	}

	return &DBValidator{db: db}, nil
}

func (v *DBValidator) DumpCurrentState() (map[string][]map[string]interface{}, error) {
	// TODO Use `info for db` to get table names
	tableNames := []string{"transaction", "campaign"}

	result := make(map[string][]map[string]interface{})
	for _, tableName := range tableNames {
		// Query all records from the table
		records, err := surrealdb.Select[[]map[string]interface{}](v.db, models.Table(tableName))
		if err != nil {
			return nil, fmt.Errorf("failed to select from table %s: %v", tableName, err)
		}

		log.Printf("SELECT * FROM %s: %v", tableName, *records)

		result[tableName] = *records
	}

	return result, nil
}

func (v *DBValidator) CompareWithExpected(expectedPath string) error {
	// Read expected state from YAML
	expectedData, err := os.ReadFile(expectedPath)
	if err != nil {
		return fmt.Errorf("failed to read expected state file: %v", err)
	}

	var expected ExpectedDBState
	if err := yaml.Unmarshal(expectedData, &expected); err != nil {
		return fmt.Errorf("failed to parse expected state YAML: %v", err)
	}

	// Get current state
	current, err := v.DumpCurrentState()
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
					record[key] = models.CustomDateTime{Time: value.Time.UTC()}
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

	if err := validator.CompareWithExpected(expectedPath); err != nil {
		log.Fatalf("Validation failed: %v", err)
	}

	fmt.Println("Database state matches expected state!")
}
