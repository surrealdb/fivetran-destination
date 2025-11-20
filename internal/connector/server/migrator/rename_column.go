package migrator

import (
	"context"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// RenameColumn renames a column within a table.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#rename_column
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Primary: ALTER TABLE <schema.table> RENAME COLUMN <from_column> TO <to_column>
// - Fallback: Add new column, copy data via UPDATE, drop old column
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC,
// resulting in new columns lacking historical data.
func (m *Migrator) RenameColumn(ctx context.Context, schema, table, fromColumn, toColumn string) error {
	// SurrealDB doesn't have a direct RENAME COLUMN command, so use fallback approach:
	// 1. Get the field type of fromColumn from table schema (INFO FOR TABLE)
	// 2. Add new field with same type
	// 3. Copy data from old to new column
	// 4. Unset old field data from all records
	// 5. Remove the old field definition

	// Result type for INFO FOR TABLE query
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}

	// Step 1: Get table info to find the field definition
	infoQuery := fmt.Sprintf("INFO FOR TABLE %s", table)
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, infoQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	if infoResults == nil || len(*infoResults) == 0 {
		return fmt.Errorf("no table info returned for %s", table)
	}

	// Extract the result from the first query response
	infoForTableRes := (*infoResults)[0].Result

	// Get fields definitions
	fieldsMap := infoForTableRes.Fields
	if fieldsMap == nil {
		return fmt.Errorf("unexpected nil fields in table info for %s", table)
	}

	// Find the field definition for fromColumn
	fieldDef, ok := fieldsMap[fromColumn]
	if !ok {
		return fmt.Errorf("column %s not found in table %s", fromColumn, table)
	}

	// Step 2: Create new field with same definition but different name
	// Replace the field name in the DEFINE FIELD statement
	// e.g., "DEFINE FIELD old_name ON table TYPE string" -> "DEFINE FIELD new_name ON table TYPE string"
	newFieldDef := strings.Replace(fieldDef, "DEFINE FIELD "+fromColumn+" ", "DEFINE FIELD "+toColumn+" ", 1)

	_, err = surrealdb.Query[any](ctx, m.db, newFieldDef, nil)
	if err != nil {
		return fmt.Errorf("failed to create new field %s on table %s: %w", toColumn, table, err)
	}

	// Step 3: Copy data from old column to new column
	copyQuery := fmt.Sprintf("UPDATE %s SET %s = %s", table, toColumn, fromColumn)
	_, err = surrealdb.Query[any](ctx, m.db, copyQuery, nil)
	if err != nil {
		// Try to clean up the new field on failure
		_, _ = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("REMOVE FIELD %s ON %s", toColumn, table), nil)
		return fmt.Errorf("failed to copy data from %s to %s: %w", fromColumn, toColumn, err)
	}

	// Step 4: Remove the old field definition first
	// This must be done before unsetting the data to avoid schema validation errors
	removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", fromColumn, table)
	_, err = surrealdb.Query[any](ctx, m.db, removeQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove old field %s from table %s: %w", fromColumn, table, err)
	}

	// Step 5: Unset the old field data from all records
	// Now that the field is not in the schema, we can safely unset it
	unsetQuery := fmt.Sprintf("UPDATE %s UNSET %s", table, fromColumn)
	_, err = surrealdb.Query[any](ctx, m.db, unsetQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to unset old field %s data: %w", fromColumn, err)
	}

	m.LogInfo("Renamed column",
		"table", table,
		"from", fromColumn,
		"to", toColumn,
	)

	return nil
}
