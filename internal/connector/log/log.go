package log

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
func InitLogger(w io.Writer, level zerolog.Level) zerolog.Logger {
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
	Logger zerolog.Logger
}

func (s *Logging) Debugging() bool {
	return s.Logger.GetLevel() == zerolog.DebugLevel
}

// LogInfo logs an informational message
func (s *Logging) LogInfo(msg string, fields ...interface{}) {
	event := s.Logger.Info()
	if !event.Enabled() {
		return
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelInfo).Msg(msg)
}

// LogWarning logs a warning message
func (s *Logging) LogWarning(msg string, err error, fields ...interface{}) {
	event := s.Logger.Warn()
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

// LogSevere logs a severe error message
func (s *Logging) LogSevere(msg string, err error, fields ...interface{}) {
	event := s.Logger.Error()
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

// LogDebug logs a debug message only when debug mode is enabled
func (s *Logging) LogDebug(msg string, fields ...interface{}) {
	event := s.Logger.Debug()
	if !event.Enabled() {
		return
	}

	if len(fields) > 0 {
		event.Fields(fields)
	}

	event.Str("level", LevelInfo).Bool("debug", true).Msg(msg)
}
