package framework

type Logger interface {
	Debugging() bool
	LogInfo(msg string, fields ...interface{})
	LogWarning(msg string, err error, fields ...interface{})
	LogSevere(msg string, err error, fields ...interface{})
	LogDebug(msg string, fields ...interface{})
}
