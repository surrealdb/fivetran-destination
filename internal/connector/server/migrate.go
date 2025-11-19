package server

import (
	"context"
	"fmt"

	"github.com/surrealdb/fivetran-destination/internal/connector/server/migrator"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

func (s *Server) migrate(ctx context.Context, req *pb.MigrateRequest) error {
	schema, table := req.Details.Schema, req.Details.Table
	s.LogInfo("Starting migration operation on %s.%s", schema, table)

	db, err := s.parseConfigAndConnect(ctx, req.Configuration, schema)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.LogWarning("failed to close db", err)
		}
	}()

	m := migrator.New(db, s.Logging)

	switch v := req.Details.Operation.(type) {
	case *pb.MigrationDetails_Add:
		if err := s.migrateAdd(ctx, m, schema, table, v.Add); err != nil {
			return err
		}
	case *pb.MigrationDetails_UpdateColumnValue:
		if err := s.migrateUpdateColumnValue(ctx, m, schema, table, v.UpdateColumnValue); err != nil {
			return err
		}
	case *pb.MigrationDetails_Rename:
		if err := s.migrateRename(ctx, m, schema, table, v.Rename); err != nil {
			return err
		}
	case *pb.MigrationDetails_Copy:
		if err := s.migrateCopy(ctx, m, schema, table, v.Copy); err != nil {
			return err
		}
	case *pb.MigrationDetails_Drop:
		if err := s.migrateDrop(ctx, m, schema, table, v.Drop); err != nil {
			return err
		}
	case *pb.MigrationDetails_TableSyncModeMigration:
		if err := s.migrateTableSyncModeMigration(ctx, m, schema, table, v.TableSyncModeMigration); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown migration operation: %T", v)
	}
	return nil
}

func (s *Server) migrateDrop(ctx context.Context, m *migrator.Migrator, schema string, table string, drop *pb.DropOperation) error {
	switch v := drop.Entity.(type) {
	case *pb.DropOperation_DropTable:
		s.LogInfo("Dropping table",
			"schema", schema,
			"table", table,
			"drop_table", v.DropTable,
		)
		if !v.DropTable {
			return fmt.Errorf("encountered drop table flag set to false")
		}

		return m.DropTable(ctx, schema, table)
	case *pb.DropOperation_DropColumnInHistoryMode:
		dropCol := v.DropColumnInHistoryMode
		s.LogInfo("Dropping column in history mode",
			"schema", schema,
			"table", table,
			"column", dropCol.Column,
			"operation_timestamp", dropCol.OperationTimestamp,
		)

		return m.DropColumnInHistoryMode(ctx, schema, table, dropCol.Column, dropCol.OperationTimestamp)
	default:
		return fmt.Errorf("unknown drop operation: %T", v)
	}
}

func (s *Server) migrateCopy(ctx context.Context, m *migrator.Migrator, schema string, table string, copy *pb.CopyOperation) error {
	switch v := copy.Entity.(type) {
	case *pb.CopyOperation_CopyColumn:
		copyCol := v.CopyColumn
		s.LogInfo("Copying column",
			"schema", schema,
			"table", table,
			"from_column", copyCol.FromColumn,
			"to_column", copyCol.ToColumn,
		)

		return m.CopyColumn(ctx, schema, table, copyCol.FromColumn, copyCol.ToColumn)
	case *pb.CopyOperation_CopyTable:
		copyTbl := v.CopyTable
		s.LogInfo("Copying table",
			"schema", schema,
			"table", table,
			"from_table", copyTbl.FromTable,
			"to_table", copyTbl.ToTable,
		)

		return m.CopyTable(ctx, schema, table, copyTbl.FromTable, copyTbl.ToTable)
	case *pb.CopyOperation_CopyTableToHistoryMode:
		copyHist := v.CopyTableToHistoryMode
		s.LogInfo("Copying table to history mode",
			"schema", schema,
			"table", table,
			"from_table", copyHist.FromTable,
			"to_table", copyHist.ToTable,
			"soft_deleted_column", copyHist.SoftDeletedColumn,
		)

		return m.CopyTableToHistoryMode(ctx, schema, table, copyHist.FromTable, copyHist.ToTable, copyHist.SoftDeletedColumn)
	default:
		return fmt.Errorf("unknown copy operation: %T", v)
	}
}

func (s *Server) migrateRename(ctx context.Context, m *migrator.Migrator, schema string, table string, rename *pb.RenameOperation) error {
	switch v := rename.Entity.(type) {
	case *pb.RenameOperation_RenameColumn:
		renameCol := v.RenameColumn
		s.LogInfo("Renaming column",
			"schema", schema,
			"table", table,
			"from_column", renameCol.FromColumn,
			"to_column", renameCol.ToColumn,
		)

		return m.RenameColumn(ctx, schema, table, renameCol.FromColumn, renameCol.ToColumn)
	case *pb.RenameOperation_RenameTable:
		renameTbl := v.RenameTable
		s.LogInfo("Renaming table",
			"schema", schema,
			"table", table,
			"from_table", renameTbl.FromTable,
			"to_table", renameTbl.ToTable,
		)

		return m.RenameTable(ctx, schema, table, renameTbl.FromTable, renameTbl.ToTable)
	default:
		return fmt.Errorf("unknown rename operation: %T", v)
	}
}

func (s *Server) migrateAdd(ctx context.Context, m *migrator.Migrator, schema string, table string, add *pb.AddOperation) error {
	switch v := add.Entity.(type) {
	case *pb.AddOperation_AddColumnWithDefaultValue:
		addCol := v.AddColumnWithDefaultValue
		s.LogInfo("Adding column with default value",
			"schema", schema,
			"table", table,
			"column", addCol.Column,
			"column_type", addCol.ColumnType,
			"default_value", addCol.DefaultValue,
		)

		return m.AddColumnWithDefaultValue(ctx, schema, table, addCol.Column, addCol.ColumnType, addCol.DefaultValue)
	case *pb.AddOperation_AddColumnInHistoryMode:
		addColHist := v.AddColumnInHistoryMode
		s.LogInfo("Adding column in history mode",
			"schema", schema,
			"table", table,
			"column", addColHist.Column,
			"column_type", addColHist.ColumnType,
			"default_value", addColHist.DefaultValue,
			"operation_timestamp", addColHist.OperationTimestamp,
		)

		return m.AddColumnInHistoryMode(ctx, schema, table, addColHist.Column, addColHist.ColumnType, addColHist.DefaultValue, addColHist.OperationTimestamp)
	default:
		return fmt.Errorf("unknown add operation: %T", v)
	}
}

func (s *Server) migrateUpdateColumnValue(ctx context.Context, m *migrator.Migrator, schema string, table string, update *pb.UpdateColumnValueOperation) error {
	s.LogInfo("Updating column values",
		"schema", schema,
		"table", table,
		"column", update.Column,
		"value", update.Value,
	)
	return m.UpdateColumnValue(ctx, schema, table, update.Column, update.Value)
}

func (s *Server) migrateTableSyncModeMigration(ctx context.Context, m *migrator.Migrator, schema string, table string, migration *pb.TableSyncModeMigrationOperation) error {
	s.LogInfo("Migrating table sync mode",
		"schema", schema,
		"table", table,
		"type", migration.Type,
		"soft_deleted_column", migration.SoftDeletedColumn,
		"keep_deleted_row", migration.KeepDeletedRows,
	)
	// Extract optional values with defaults
	softDeletedColumn := ""
	if migration.SoftDeletedColumn != nil {
		softDeletedColumn = *migration.SoftDeletedColumn
	}
	keepDeletedRows := false
	if migration.KeepDeletedRows != nil {
		keepDeletedRows = *migration.KeepDeletedRows
	}

	switch migration.Type {
	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_LIVE:
		return m.ModeSoftDeleteToLive(ctx, schema, table, softDeletedColumn)
	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_HISTORY:
		return m.ModeSoftDeleteToHistory(ctx, schema, table, softDeletedColumn)
	case pb.TableSyncModeMigrationType_HISTORY_TO_SOFT_DELETE:
		return m.ModeHistoryToSoftDelete(ctx, schema, table, softDeletedColumn)
	case pb.TableSyncModeMigrationType_HISTORY_TO_LIVE:
		return m.ModeHistoryToLive(ctx, schema, table, keepDeletedRows)
	case pb.TableSyncModeMigrationType_LIVE_TO_SOFT_DELETE:
		return m.ModeLiveToSoftDelete(ctx, schema, table, softDeletedColumn)
	case pb.TableSyncModeMigrationType_LIVE_TO_HISTORY:
		return m.ModeLiveToHistory(ctx, schema, table)
	default:
		return fmt.Errorf("unknown table sync mode migration type: %v", migration.Type)
	}
}
