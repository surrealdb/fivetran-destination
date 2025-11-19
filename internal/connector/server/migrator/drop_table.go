package migrator

import (
	"context"
)

// DropTable removes a table from the database.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_table
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: DROP TABLE <schema.table>
func (m *Migrator) DropTable(ctx context.Context, schema, table string) error {
	// TODO: Implement drop table logic
	// 1. Remove the table from the database:
	//    REMOVE TABLE table
	// 2. Return any errors encountered

	return nil
}
