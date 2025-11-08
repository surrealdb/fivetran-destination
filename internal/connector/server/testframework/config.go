package testframework

import (
	"os"

	"github.com/rs/zerolog"
)

// GetSurrealDBConfig returns a standard test configuration map for connecting to SurrealDB
func GetSurrealDBConfig() map[string]string {
	return map[string]string{
		"url":  GetSurrealDBEndpoint(),
		"ns":   "test",
		"user": "root",
		"pass": "root",
	}
}

// GetTestLogger creates a logger for tests based on the debug environment variable
func GetTestLogger() zerolog.Logger {
	level := zerolog.InfoLevel
	if os.Getenv("SURREAL_FIVETRAN_DEBUG") != "" {
		level = zerolog.DebugLevel
	}
	return zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
}
