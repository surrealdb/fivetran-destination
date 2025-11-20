package migrator

import (
	"context"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// RenameTable renames an existing table in the schema.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#rename_table
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Primary: ALTER TABLE <schema.from_table> RENAME TO <to_table>
// - Fallback: CREATE TABLE with new name, then DROP old table
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC
// creation without historical data preservation.
func (m *Migrator) RenameTable(ctx context.Context, schema, table, fromTable, toTable string) error {
	// SurrealDB doesn't have a direct RENAME TABLE command, so use fallback approach:
	// 1. Get the table definition from fromTable (INFO FOR TABLE)
	// 2. Create toTable with same schema
	// 3. Copy all data
	// 4. Create indices on the new table
	// 5. Drop the old table

	// Result type for INFO FOR TABLE query
	type InfoForTableResult struct {
		Fields  map[string]string `cbor:"fields"`
		Indexes map[string]string `cbor:"indexes"`
	}

	// Step 1: Get table info (fields and indexes)
	infoQuery := fmt.Sprintf("INFO FOR TABLE %s", fromTable)
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, infoQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", fromTable, err)
	}

	if infoResults == nil || len(*infoResults) == 0 {
		return fmt.Errorf("no table info returned for %s", fromTable)
	}

	// Extract the result from the first query response
	infoForTableRes := (*infoResults)[0].Result

	// Get fields definitions
	fieldsMap := infoForTableRes.Fields
	if fieldsMap == nil {
		return fmt.Errorf("unexpected nil fields in table info for %s", fromTable)
	}

	// Get indexes definitions
	indexesMap := infoForTableRes.Indexes
	if indexesMap == nil {
		return fmt.Errorf("unexpected nil indexes in table info for %s", fromTable)
	}

	// Step 2: Create the new table with SCHEMAFULL
	createTableQuery := fmt.Sprintf("DEFINE TABLE %s SCHEMAFULL", toTable)
	_, err = surrealdb.Query[any](ctx, m.db, createTableQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to create new table %s: %w", toTable, err)
	}

	// Define all fields on the new table
	for _, fieldDef := range fieldsMap {
		// Replace the old table name with the new table name in the field definition
		// e.g., "DEFINE FIELD name ON old_table TYPE string" -> "DEFINE FIELD name ON new_table TYPE string"
		newFieldDef := strings.Replace(fieldDef, " ON "+fromTable+" ", " ON "+toTable+" ", 1)

		_, err = surrealdb.Query[any](ctx, m.db, newFieldDef, nil)
		if err != nil {
			// Try to clean up on failure
			_, _ = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("REMOVE TABLE %s", toTable), nil)
			return fmt.Errorf("failed to define field on new table %s: %w", toTable, err)
		}
	}

	// Step 3: Copy all data from old table to new table
	copyQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", toTable, fromTable)
	_, err = surrealdb.Query[any](ctx, m.db, copyQuery, nil)
	if err != nil {
		// Try to clean up on failure
		_, _ = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("REMOVE TABLE %s", toTable), nil)
		return fmt.Errorf("failed to copy data from %s to %s: %w", fromTable, toTable, err)
	}

	// Step 4: Create indices on the new table (after data copy for better performance)
	for _, indexDef := range indexesMap {
		// Replace the old table name with the new table name in the index definition
		// e.g., "DEFINE INDEX idx ON old_table FIELDS id" -> "DEFINE INDEX idx ON new_table FIELDS id"
		newIndexDef := strings.Replace(indexDef, " ON "+fromTable+" ", " ON "+toTable+" ", 1)

		_, err = surrealdb.Query[any](ctx, m.db, newIndexDef, nil)
		if err != nil {
			// Try to clean up on failure
			_, _ = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("REMOVE TABLE %s", toTable), nil)
			return fmt.Errorf("failed to create index on new table %s: %w", toTable, err)
		}
	}

	// Step 5: Drop the old table
	dropQuery := fmt.Sprintf("REMOVE TABLE %s", fromTable)
	_, err = surrealdb.Query[any](ctx, m.db, dropQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove old table %s: %w", fromTable, err)
	}

	m.LogInfo("Renamed table",
		"from", fromTable,
		"to", toTable,
	)

	return nil
}
