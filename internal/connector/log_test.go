package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/surrealdb/fivetran-destination/internal/connector/log"
	"github.com/surrealdb/fivetran-destination/internal/connector/server"
)

type testServer struct {
	server.Server
	buf *bytes.Buffer
}

func newTestServer(level zerolog.Level) *testServer {
	buf := &bytes.Buffer{}
	return &testServer{
		Server: server.Server{
			Logging: &log.Logging{
				Logger: log.InitLogger(buf, level),
			},
		},
		buf: buf,
	}
}

func (s *testServer) getLogOutput() string {
	return s.buf.String()
}

func TestLogInfo(t *testing.T) {
	s := newTestServer(zerolog.InfoLevel)
	s.LogInfo("test message", "key", "value")

	output := s.getLogOutput()
	var msg map[string]interface{}
	if err := json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "INFO",
		"message":        "test message",
		"message-origin": log.MessageOrigin,
		"key":            "value",
	}

	assert.Subset(t, msg, expected)

	s = newTestServer(zerolog.WarnLevel)
	s.LogInfo("test message", "key", "value")
	output = s.getLogOutput()
	assert.Empty(t, output)
}

func TestLogWarning(t *testing.T) {
	s := newTestServer(zerolog.WarnLevel)
	s.LogWarning("warning message", nil, "key", "value")

	output := s.getLogOutput()
	var msg map[string]interface{}
	if err := json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "WARNING",
		"message":        "warning message",
		"message-origin": log.MessageOrigin,
		"key":            "value",
	}

	assert.Subset(t, msg, expected)

	s = newTestServer(zerolog.ErrorLevel)
	s.LogWarning("warning message", nil, "key", "value")
	output = s.getLogOutput()
	assert.Empty(t, output)
}

func TestLogSevere(t *testing.T) {
	levels := []zerolog.Level{
		zerolog.DebugLevel,
		zerolog.InfoLevel,
		zerolog.WarnLevel,
		zerolog.ErrorLevel,
	}

	for _, level := range levels {
		t.Run(fmt.Sprintf("level=%s", level), func(t *testing.T) {
			s := newTestServer(level)
			err := errors.New("test error")
			s.LogSevere("error message", err, "key", "value")

			output := s.getLogOutput()
			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(output), &msg); err != nil {
				t.Fatalf("Failed to parse log output: %v", err)
			}

			expected := map[string]interface{}{
				"level":          "SEVERE",
				"message":        "error message",
				"message-origin": log.MessageOrigin,
				"error":          "test error",
				"key":            "value",
			}

			assert.Subset(t, msg, expected)
		})
	}
}

func TestLogDebug(t *testing.T) {
	// Test debug logging when debug is enabled
	s := newTestServer(zerolog.DebugLevel)
	s.LogDebug("debug message", "key", "value")

	output := s.getLogOutput()
	var msg map[string]interface{}
	if err := json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "INFO",
		"message":        "debug message",
		"message-origin": log.MessageOrigin,
		"debug":          true,
		"key":            "value",
	}

	assert.Subset(t, msg, expected)

	// Test debug logging when debug is disabled
	s = newTestServer(zerolog.InfoLevel)
	s.LogDebug("debug message", "key", "value")
	output = s.getLogOutput()
	assert.Empty(t, output)
}
