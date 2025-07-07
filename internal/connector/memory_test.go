package connector

import (
	"fmt"
	"os"
)

// We use two approaches for memory analysis:
//
// 1. Benchmarks with b.ReportAllocs() for optimization:
//    - Measure per-operation efficiency (B/op, allocs/op)
//    - Great for optimizing specific functions and detecting regressions
//    - Answer: "How efficient is this operation?"
//
// 2. Tests with runtime.ReadMemStats() for leak detection:
//    - Measure total memory impact and detect leaks
//    - Include GC behavior and real-world usage patterns
//    - Answer: "What's the total memory footprint?"
//
// All tests assumes a SurrealDB instance is running and listening on
// $SURREALDB_HOST:$SURREALDB_PORT, which should be automatically set if
// running in the devcontainer.
//
// See memory_bench_test.go and memory_usage_test.go for respective benchmarks
// and tests.
//
// TODO: We might also want to see the trends in memory usage over time, which
// could be done by running tests while taking snapshots at intervals.
// Probably that could be an extension of what are tested currently in memory_usage_test.go.

// getSurrealDBConfig returns standard test configuration for localhost:8000
func getSurrealDBConfig() map[string]string {
	host := "localhost"
	port := "8000"

	if envHost := os.Getenv("SURREALDB_HOST"); envHost != "" {
		host = envHost
	}
	if envPort := os.Getenv("SURREALDB_PORT"); envPort != "" {
		port = envPort
	}

	return map[string]string{
		"url":  fmt.Sprintf("ws://%s:%s/rpc", host, port),
		"ns":   "test",
		"db":   "memory_test",
		"user": "root",
		"pass": "root",
	}
}
