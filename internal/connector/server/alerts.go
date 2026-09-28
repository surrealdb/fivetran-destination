package server

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

// errUnsupportedMigration marks a migration operation this connector does not implement.
// The RPC returns unsupported rather than a user-facing task.
var errUnsupportedMigration = errors.New("unsupported migration operation")

// taskFor turns an operation failure into the dashboard task Fivetran shows to the user.
// The gRPC handler must return this with a nil error so the task is not dropped.
func taskFor(err error) *pb.Task {
	if errors.Is(err, ErrTokenExpired) {
		return NewTokenExpiredTask()
	}
	if err == nil {
		return &pb.Task{Message: "unknown error"}
	}
	return &pb.Task{Message: err.Error()}
}

func (s *Server) describeTableFailure(err error) (*pb.DescribeTableResponse, error) {
	s.LogSevere("DescribeTable failed", err)
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Task{Task: taskFor(err)},
	}, nil
}

func (s *Server) createTableFailure(err error) (*pb.CreateTableResponse, error) {
	s.LogSevere("CreateTable failed", err)
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Task{Task: taskFor(err)},
	}, nil
}

func (s *Server) alterTableFailure(err error) (*pb.AlterTableResponse, error) {
	s.LogSevere("AlterTable failed", err)
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Task{Task: taskFor(err)},
	}, nil
}

func (s *Server) truncateFailure(err error) (*pb.TruncateResponse, error) {
	s.LogSevere("Truncate failed", err)
	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Task{Task: taskFor(err)},
	}, nil
}

func (s *Server) batchFailure(rpc string, err error) (*pb.WriteBatchResponse, error) {
	s.LogSevere(rpc+" failed", err)
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Task{Task: taskFor(err)},
	}, nil
}

func (s *Server) migrateResult(err error) (*pb.MigrateResponse, error) {
	if err == nil {
		return &pb.MigrateResponse{
			Response: &pb.MigrateResponse_Success{Success: true},
		}, nil
	}
	if errors.Is(err, errUnsupportedMigration) {
		s.LogWarning("unsupported migration operation", err)
		return &pb.MigrateResponse{
			Response: &pb.MigrateResponse_Unsupported{Unsupported: true},
		}, nil
	}
	wrapped := fmt.Errorf("migration failed: %w", err)
	s.LogSevere("migration failed", wrapped)
	return &pb.MigrateResponse{
		Response: &pb.MigrateResponse_Task{Task: taskFor(wrapped)},
	}, nil
}

func (s *Server) closeDB(ctx context.Context, db *surrealdb.DB) {
	if db == nil {
		return
	}
	if err := db.Close(ctx); err != nil {
		s.LogWarning("failed to close db", err)
	}
}
