package connector

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog"
)

const (
	MessageOrigin = "sdk_destination"
)

const (
	LevelInfo    = "INFO"
	LevelWarning = "WARNING"
	LevelSevere  = "SEVERE"
)

func parseLogLevel(level string) (zerolog.Level, error) {
	switch level {
	case "info", LevelInfo:
		return zerolog.InfoLevel, nil
	case "warn", LevelWarning:
		return zerolog.WarnLevel, nil
	case "error", LevelSevere:
		return zerolog.ErrorLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	default:
		return zerolog.NoLevel, fmt.Errorf("invalid log level: %s", level)
	}
}

// initLogger initializes the zerolog logger with JSON output format
func initLogger(w io.Writer, level zerolog.Level) zerolog.Logger {
	if w == nil {
		w = os.Stdout
	}

	// Create logger with standard fields
	return zerolog.New(w).
		With().
		Str("message-origin", MessageOrigin).
		Logger().
		Level(level)
}

type Logging struct {
	logger zerolog.Logger
}

func (s *Logging) debugging() bool {
	return s.logger.GetLevel() == zerolog.DebugLevel
}

// logInfo logs an informational message
func (s *Logging) logInfo(msg string, fields ...interface{}) {
	event := s.logger.Info()
	if !event.Enabled() {
		return
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelInfo).Msg(msg)
}

// logWarning logs a warning message
func (s *Logging) logWarning(msg string, err error, fields ...interface{}) {
	event := s.logger.Warn()
	if !event.Enabled() {
		return
	}

	if err != nil {
		event.Err(err)
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelWarning).Msg(msg)
}

// logSevere logs a severe error message
func (s *Logging) logSevere(msg string, err error, fields ...interface{}) {
	event := s.logger.Error()
	if !event.Enabled() {
		return
	}

	if err != nil {
		event.Err(err)
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelSevere).Msg(msg)
}

// logDebug logs a debug message only when debug mode is enabled
func (s *Logging) logDebug(msg string, fields ...interface{}) {
	event := s.logger.Debug()
	if !event.Enabled() {
		return
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelInfo).Bool("debug", true).Msg(msg)
}
