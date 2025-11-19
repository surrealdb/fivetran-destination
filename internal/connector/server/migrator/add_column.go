package migrator

import (
	"context"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// AddColumnWithDefaultValue adds a new column with a specified data type and default value.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#add_column_with_default_value
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Primary: ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type> DEFAULT <default_value>
// - Fallback: Add column without DEFAULT clause, then UPDATE with default values
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC
// without back-dated data.
func (m *Migrator) AddColumnWithDefaultValue(ctx context.Context, schema, table, column string, columnType pb.DataType, defaultValue string) error {
	// TODO: Implement add column with default value logic
	// 1. Map columnType to SurrealDB type
	// 2. Add the new field with default value:
	//    DEFINE FIELD OVERWRITE column ON table TYPE <mapped_type> DEFAULT <default_value>
	//    Note: SurrealDB supports DEFAULT clause (applies on CREATE)
	//    Reference: https://surrealdb.com/docs/surrealql/statements/define/field#using-the-default-clause-to-set-a-default-value
	// 3. Update existing records to have the default value:
	//    UPDATE table SET column = <default_value> WHERE column IS NONE
	// 4. Return any errors encountered

	return nil
}
