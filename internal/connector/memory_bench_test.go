package connector

import (
	"context"
	"testing"

	"github.com/surrealdb/fivetran-destination/internal/connector/server"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// BenchmarkMemoryAllocation benchmarks memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	logger, err := LoggerFromEnv()
	if err != nil {
		b.Fatal(err)
	}

	server := server.New(logger)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := server.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSurrealDBOperations benchmarks SurrealDB operation allocations
func BenchmarkSurrealDBOperations(b *testing.B) {
	logger, err := LoggerFromEnv()
	if err != nil {
		b.Fatal(err)
	}

	server := server.New(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := server.Test(ctx, &pb.TestRequest{
			Name:          "benchmark",
			Configuration: testConfig,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
