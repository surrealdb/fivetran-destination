package server

import (
	"context"
	"errors"
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

func (s *Server) migrateDrop(_ context.Context, m *migrator.Migrator, schema string, table string, drop *pb.DropOperation) error {
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

		return errors.New("drop table is not implemented yet")
	case *pb.DropOperation_DropColumnInHistoryMode:
		drop := v.DropColumnInHistoryMode
		s.LogInfo("Dropping column in history mode",
			"schema", schema,
			"table", table,
			"column", drop.Column,
			"operation_timestamp", drop.OperationTimestamp,
		)

		return errors.New("drop column in history mode is not implemented yet")
	default:
		return fmt.Errorf("unknown drop operation: %T", v)
	}
}

func (s *Server) migrateCopy(_ context.Context, m *migrator.Migrator, schema string, table string, copy *pb.CopyOperation) error {
	switch v := copy.Entity.(type) {
	case *pb.CopyOperation_CopyColumn:
		copy := v.CopyColumn
		s.LogInfo("Copying column",
			"schema", schema,
			"table", table,
			"from_column", copy.FromColumn,
			"to_column", copy.ToColumn,
		)

		return errors.New("copy column is not implemented yet")
	case *pb.CopyOperation_CopyTable:
		copy := v.CopyTable
		s.LogInfo("Copying table",
			"schema", schema,
			"table", table,
			"from_table", copy.FromTable,
			"to_table", copy.ToTable,
		)

		return errors.New("copy table is not implemented yet")
	case *pb.CopyOperation_CopyTableToHistoryMode:
		copy := v.CopyTableToHistoryMode
		s.LogInfo("Copying table",
			"schema", schema,
			"table", table,
			"from_table", copy.FromTable,
			"to_table", copy.ToTable,
			"soft_deleted_column", copy.SoftDeletedColumn,
		)

		return errors.New("copy table to history mode is not implemented yet")
	default:
		return fmt.Errorf("unknown copy operation: %T", v)
	}
}

func (s *Server) migrateRename(_ context.Context, m *migrator.Migrator, schema string, table string, rename *pb.RenameOperation) error {
	switch v := rename.Entity.(type) {
	case *pb.RenameOperation_RenameColumn:
		rename := v.RenameColumn
		s.LogInfo("Renaming column",
			"schema", schema,
			"table", table,
			"from_column", rename.FromColumn,
			"to_column", rename.ToColumn,
		)

		return errors.New("rename column is not implemented yet")
	case *pb.RenameOperation_RenameTable:
		rename := v.RenameTable
		s.LogInfo("Renaming table",
			"schema", schema,
			"table:", table,
			"from_table", rename.FromTable,
			"to_table", rename.ToTable,
		)

		return errors.New("rename table is not implemented yet")
	default:
		return fmt.Errorf("unknown rename operation: %T", v)
	}
}

func (s *Server) migrateAdd(_ context.Context, m *migrator.Migrator, schema string, table string, add *pb.AddOperation) error {
	switch v := add.Entity.(type) {
	case *pb.AddOperation_AddColumnWithDefaultValue:
		add := v.AddColumnWithDefaultValue
		s.LogInfo("Adding column with default value",
			"schema", schema,
			"table", table,
			"column", add.Column,
			"column_type", add.ColumnType,
			"default_value", add.DefaultValue,
		)

		return errors.New("add column is not implemented yet")
	case *pb.AddOperation_AddColumnInHistoryMode:
		add := v.AddColumnInHistoryMode
		s.LogInfo("Adding column in history mode",
			"schema", schema,
			"table", table,
			"column", add.Column,
			"column_type", add.ColumnType,
			"default_value", add.DefaultValue,
			"operation_timestamp", add.OperationTimestamp,
		)

		return errors.New("add column in history mode is not implemented yet")
	default:
		return fmt.Errorf("unknown add operation: %T", v)
	}
}

func (s *Server) migrateUpdateColumnValue(_ context.Context, m *migrator.Migrator, schema string, table string, update *pb.UpdateColumnValueOperation) error {
	s.LogInfo("Updating column values",
		"schema", schema,
		"table", table,
		"column", update.Column,
		"value", update.Value,
	)
	return errors.New("update column value migration not implemented yet")
}

func (s *Server) migrateTableSyncModeMigration(_ context.Context, m *migrator.Migrator, schema string, table string, migration *pb.TableSyncModeMigrationOperation) error {
	s.LogInfo("Migrating table sync mode",
		"schema", schema,
		"table", table,
		"type", migration.Type,
		"soft_deleted_column", migration.SoftDeletedColumn,
		"keep_deleted_row", migration.KeepDeletedRows,
	)
	switch migration.Type {
	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_LIVE:
	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_HISTORY:
	case pb.TableSyncModeMigrationType_HISTORY_TO_SOFT_DELETE:
	case pb.TableSyncModeMigrationType_HISTORY_TO_LIVE:
	case pb.TableSyncModeMigrationType_LIVE_TO_SOFT_DELETE:
	case pb.TableSyncModeMigrationType_LIVE_TO_HISTORY:
	default:
		return fmt.Errorf("unknown table sync mode migration type: %v", migration.Type)
	}
	return errors.New("table sync mode migration not implemented yet")
}
