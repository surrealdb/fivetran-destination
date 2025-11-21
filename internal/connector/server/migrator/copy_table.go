package migrator

import (
	"context"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// CopyTable creates a new table and copies all data from a source table to a destination table.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#copy_table
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: CREATE TABLE <schema.to_table> AS SELECT * FROM <schema.from_table>
//
// If this operation returns an unsupported error, Fivetran will fall back to CreateTable RPC
// without data copying.
func (m *Migrator) CopyTable(ctx context.Context, schema, fromTable, toTable string) error {
	// 1. Get schema from source table
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", fromTable), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", fromTable, err)
	}

	// 2. Create destination table
	_, err = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("DEFINE TABLE %s SCHEMAFULL", toTable), nil)
	if err != nil {
		return fmt.Errorf("failed to create destination table %s: %w", toTable, err)
	}

	// 3. Copy field definitions from source, replacing table name
	if infoResults != nil && len(*infoResults) > 0 {
		fields := (*infoResults)[0].Result.Fields
		for _, fieldDef := range fields {
			// Replace table name in field definition
			newFieldDef := strings.Replace(fieldDef, " ON "+fromTable+" ", " ON "+toTable+" ", 1)
			_, err = surrealdb.Query[any](ctx, m.db, newFieldDef, nil)
			if err != nil {
				m.LogInfo("Warning: could not copy field definition", "error", err.Error())
			}
		}
	}

	// 4. Copy all data from source to destination
	insertQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", toTable, fromTable)
	_, err = surrealdb.Query[any](ctx, m.db, insertQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s: %w", fromTable, toTable, err)
	}

	m.LogInfo("Copied table",
		"from_table", fromTable,
		"to_table", toTable,
	)

	return nil
}
