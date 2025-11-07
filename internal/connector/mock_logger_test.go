package connector

import (
	"sync"
)

// MockLogging is a mock implementation of Logging for testing
type MockLogging struct {
	mu       sync.Mutex
	messages []LogMessage
}

// LogMessage represents a captured log message
type LogMessage struct {
	Level   string
	Message string
	Fields  map[string]interface{}
}

// NewMockLogging creates a new mock logger
func NewMockLogging() *MockLogging {
	return &MockLogging{
		messages: make([]LogMessage, 0),
	}
}

// Debugging returns false for the mock
func (m *MockLogging) Debugging() bool {
	return false
}

// LogInfo captures info messages
func (m *MockLogging) LogInfo(msg string, fields ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logMsg := LogMessage{
		Level:   "INFO",
		Message: msg,
		Fields:  m.parseFields(fields),
	}
	m.messages = append(m.messages, logMsg)
}

// LogWarning captures warning messages
func (m *MockLogging) LogWarning(msg string, err error, fields ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logMsg := LogMessage{
		Level:   "WARNING",
		Message: msg,
		Fields:  m.parseFields(fields),
	}
	if err != nil {
		logMsg.Fields["error"] = err.Error()
	}
	m.messages = append(m.messages, logMsg)
}

// LogSevere captures severe messages
func (m *MockLogging) LogSevere(msg string, err error, fields ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logMsg := LogMessage{
		Level:   "SEVERE",
		Message: msg,
		Fields:  m.parseFields(fields),
	}
	if err != nil {
		logMsg.Fields["error"] = err.Error()
	}
	m.messages = append(m.messages, logMsg)
}

// LogDebug captures debug messages
func (m *MockLogging) LogDebug(msg string, fields ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logMsg := LogMessage{
		Level:   "DEBUG",
		Message: msg,
		Fields:  m.parseFields(fields),
	}
	m.messages = append(m.messages, logMsg)
}

// parseFields converts the variadic fields into a map
func (m *MockLogging) parseFields(fields []interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Fields come in pairs: key, value, key, value...
	for i := 0; i < len(fields)-1; i += 2 {
		if key, ok := fields[i].(string); ok {
			result[key] = fields[i+1]
		}
	}

	return result
}

// GetMessages returns all captured messages
func (m *MockLogging) GetMessages() []LogMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return a copy to avoid race conditions
	messages := make([]LogMessage, len(m.messages))
	copy(messages, m.messages)
	return messages
}

// GetLastMessage returns the last captured message
func (m *MockLogging) GetLastMessage() *LogMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) == 0 {
		return nil
	}

	// Return a copy of the last message
	msg := m.messages[len(m.messages)-1]
	return &msg
}

// FindMessage finds a message by its content
func (m *MockLogging) FindMessage(messageContent string) *LogMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, msg := range m.messages {
		if msg.Message == messageContent {
			// Return a copy
			msgCopy := msg
			return &msgCopy
		}
	}

	return nil
}

// Clear removes all captured messages
func (m *MockLogging) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = m.messages[:0]
}
