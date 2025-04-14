package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

type testServer struct {
	Server
	buf *bytes.Buffer
}

func newTestServer(level zerolog.Level) *testServer {
	buf := &bytes.Buffer{}
	return &testServer{
		Server: Server{
			Logging: &Logging{
				logger: initLogger(buf, level),
			},
		},
		buf: buf,
	}
}

func (s *testServer) getLogOutput() string {
	return s.buf.String()
}

func TestParseLogLevel(t *testing.T) {
	levels := []struct {
		input    string
		expected zerolog.Level
	}{
		{"info", zerolog.InfoLevel},
		{"warn", zerolog.WarnLevel},
		{"error", zerolog.ErrorLevel},
		{"debug", zerolog.DebugLevel},
		{"INFO", zerolog.InfoLevel},
		{"WARNING", zerolog.WarnLevel},
		{"SEVERE", zerolog.ErrorLevel},
	}

	for _, level := range levels {
		t.Run(fmt.Sprintf("input=%s", level.input), func(t *testing.T) {
			result, err := parseLogLevel(level.input)
			assert.Equal(t, level.expected, result)
			assert.NoError(t, err)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		result, err := parseLogLevel("invalid")
		assert.Equal(t, zerolog.NoLevel, result)
		assert.Error(t, err)
	})
}

func TestLogInfo(t *testing.T) {
	s := newTestServer(zerolog.InfoLevel)
	s.logInfo("test message", "key", "value")

	output := s.getLogOutput()
	var log map[string]interface{}
	if err := json.Unmarshal([]byte(output), &log); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "INFO",
		"message":        "test message",
		"message-origin": MessageOrigin,
		"key":            "value",
	}

	assert.Subset(t, log, expected)

	s = newTestServer(zerolog.WarnLevel)
	s.logInfo("test message", "key", "value")
	output = s.getLogOutput()
	assert.Empty(t, output)
}

func TestLogWarning(t *testing.T) {
	s := newTestServer(zerolog.WarnLevel)
	s.logWarning("warning message", nil, "key", "value")

	output := s.getLogOutput()
	var log map[string]interface{}
	if err := json.Unmarshal([]byte(output), &log); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "WARNING",
		"message":        "warning message",
		"message-origin": MessageOrigin,
		"key":            "value",
	}

	assert.Subset(t, log, expected)

	s = newTestServer(zerolog.ErrorLevel)
	s.logWarning("warning message", nil, "key", "value")
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
			s.logSevere("error message", err, "key", "value")

			output := s.getLogOutput()
			var log map[string]interface{}
			if err := json.Unmarshal([]byte(output), &log); err != nil {
				t.Fatalf("Failed to parse log output: %v", err)
			}

			expected := map[string]interface{}{
				"level":          "SEVERE",
				"message":        "error message",
				"message-origin": MessageOrigin,
				"error":          "test error",
				"key":            "value",
			}

			assert.Subset(t, log, expected)
		})
	}
}

func TestLogDebug(t *testing.T) {
	// Test debug logging when debug is enabled
	s := newTestServer(zerolog.DebugLevel)
	s.logDebug("debug message", "key", "value")

	output := s.getLogOutput()
	var log map[string]interface{}
	if err := json.Unmarshal([]byte(output), &log); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	expected := map[string]interface{}{
		"level":          "INFO",
		"message":        "debug message",
		"message-origin": MessageOrigin,
		"debug":          true,
		"key":            "value",
	}

	assert.Subset(t, log, expected)

	// Test debug logging when debug is disabled
	s = newTestServer(zerolog.InfoLevel)
	s.logDebug("debug message", "key", "value")
	output = s.getLogOutput()
	assert.Empty(t, output)
}
