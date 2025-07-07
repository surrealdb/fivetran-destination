package connector

type Logger interface {
	debugging() bool
	logInfo(msg string, fields ...interface{})
	logWarning(msg string, err error, fields ...interface{})
	logSevere(msg string, err error, fields ...interface{})
	logDebug(msg string, fields ...interface{})
}
