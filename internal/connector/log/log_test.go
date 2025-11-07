package log

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

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
