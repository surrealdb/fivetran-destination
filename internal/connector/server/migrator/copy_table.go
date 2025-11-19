package migrator

import (
	"context"
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
func (m *Migrator) CopyTable(ctx context.Context, schema, table, fromTable, toTable string) error {
	// TODO: Implement copy table logic
	// 1. Create the destination table with the same schema as source:
	//    In SurrealDB, this requires:
	//    - Get table definition from fromTable (INFO FOR TABLE)
	//    - Create toTable with same field definitions
	// 2. Copy all data from source to destination:
	//    INSERT INTO toTable SELECT * FROM fromTable
	// 3. Return any errors encountered

	return nil
}
