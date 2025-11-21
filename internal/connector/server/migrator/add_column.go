package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
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
	// 1. Get existing field count for column index
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	columnIndex := 0
	if infoResults != nil && len(*infoResults) > 0 {
		columnIndex = len((*infoResults)[0].Result.Fields)
	}

	// 2. Create pb.Column for type mapping
	pbColumn := &pb.Column{
		Name: column,
		Type: columnType,
	}

	// 3. Get type mapping
	typeMapping := tablemapper.FindTypeMappingByPbColumn(pbColumn)
	if typeMapping == nil {
		return fmt.Errorf("unsupported data type for column %s: %v", column, columnType)
	}

	// 4. Convert default value to proper SurrealDB type
	defaultVal, err := typeMapping.SurrealType(defaultValue)
	if err != nil {
		return fmt.Errorf("failed to convert default value %q for column %s: %w", defaultValue, column, err)
	}

	// 5. Generate and execute field definition
	defineFieldQuery, err := tablemapper.DefineFieldQueryFromFt(table, pbColumn, columnIndex)
	if err != nil {
		return fmt.Errorf("failed to generate field definition: %w", err)
	}
	_, err = surrealdb.Query[any](ctx, m.db, defineFieldQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to add column %s to table %s: %w", column, table, err)
	}

	// 6. Update existing records to have the default value
	updateQuery := fmt.Sprintf("UPDATE %s SET %s = $default_value WHERE %s IS NONE", table, column, column)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
		"default_value": defaultVal,
	})
	if err != nil {
		return fmt.Errorf("failed to set default value for column %s in table %s: %w", column, table, err)
	}

	m.LogInfo("Added column with default value",
		"table", table,
		"column", column,
		"default_value", defaultValue,
	)

	return nil
}
