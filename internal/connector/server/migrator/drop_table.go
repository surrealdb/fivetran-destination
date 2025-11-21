package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// DropTable removes a table from the database.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_table
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: DROP TABLE <schema.table>
func (m *Migrator) DropTable(ctx context.Context, schema, table string) error {
	removeQuery := fmt.Sprintf("REMOVE TABLE %s", table)
	_, err := surrealdb.Query[any](ctx, m.db, removeQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", table, err)
	}

	m.LogInfo("Dropped table", "table", table)

	return nil
}
