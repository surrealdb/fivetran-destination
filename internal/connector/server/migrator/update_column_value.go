package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// UpdateColumnValue updates all values in a specified column with a new value.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#update_column_value_operation
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: UPDATE <schema.table> SET <column_name> = <new_value>
//
// NULL is supported as a valid update value.
func (m *Migrator) UpdateColumnValue(ctx context.Context, schema, table, column, value string) error {
	var query string
	var params map[string]any

	// Handle NULL/NONE value case - use NONE literal in SurrealDB for option types
	if value == "" || value == "NULL" || value == "null" {
		// Use NONE literal directly in query for option types
		query = fmt.Sprintf("UPDATE %s SET %s = NONE", table, column)
		params = nil
	} else {
		// Use parameterized query for regular values
		query = fmt.Sprintf("UPDATE %s SET %s = $value", table, column)
		params = map[string]any{
			"value": value,
		}
	}

	_, err := surrealdb.Query[[]map[string]any](ctx, m.db, query, params)
	if err != nil {
		return fmt.Errorf("failed to update column value: %w", err)
	}

	m.LogInfo("Updated column value",
		"table", table,
		"column", column,
		"value", value,
	)

	return nil
}
