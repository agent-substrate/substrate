package oss

import (
	"fmt"
	"log"
	"strings"
)

// A LogPrinter is a interface for the SDK to log messages to.
type LogPrinter interface {
	Print(...any)
}

// A LogPrinterFunc is a convenience type to wrap it so the LogPrinter interface can be used.
type LogPrinterFunc func(...any)

// Print calls the wrapped function with the arguments provided
func (f LogPrinterFunc) Print(v ...any) {
	f(v...)
}

// Define the level of the output log
const (
	LogOff = iota
	LogError
	LogWarn
	LogInfo
	LogDebug
)

var logLevelTag = []string{"", "ERROR ", "WARNING ", "INFO ", "DEBUG "}

// Logger interface to handle logging
type Logger interface {
	Debugf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
	Level() int
}

type nopLogger struct {
}

func (*nopLogger) Debugf(_ string, _ ...any) {}
func (*nopLogger) Infof(_ string, _ ...any)  {}
func (*nopLogger) Warnf(_ string, _ ...any)  {}
func (*nopLogger) Errorf(_ string, _ ...any) {}
func (*nopLogger) Level() int                { return LogOff }

// NewLogger returns a Logger
func NewLogger(level int, printer LogPrinter) Logger {
	if level <= LogOff {
		return &nopLogger{}
	}

	if printer == nil {
		printer = LogPrinterFunc(func(v ...any) {
			log.Print(v...)
		})
	}

	return &standardLogger{
		level:   level,
		printer: printer,
	}
}

type standardLogger struct {
	level   int
	printer LogPrinter
}

func (l *standardLogger) printf(level int, format string, v ...any) {
	if l.printer == nil {
		return
	}
	l.printer.Print(logLevelTag[level], fmt.Sprintf(format, v...))
}

func (l *standardLogger) Debugf(format string, v ...any) {
	if l.level < LogDebug {
		return
	}
	l.printf(LogDebug, format, v...)
}

func (l *standardLogger) Infof(format string, v ...any) {
	if l.level < LogInfo {
		return
	}
	l.printf(LogInfo, format, v...)
}

func (l *standardLogger) Warnf(format string, v ...any) {
	if l.level < LogWarn {
		return
	}
	l.printf(LogWarn, format, v...)
}

func (l *standardLogger) Errorf(format string, v ...any) {
	if l.level < LogError {
		return
	}
	l.printf(LogError, format, v...)
}

func (l *standardLogger) Level() int {
	return l.level
}

func ToLogLevel(s string) int {
	s = strings.ToLower(s)
	switch s {
	case "error", "err":
		return LogError
	case "warning", "warn":
		return LogWarn
	case "info":
		return LogInfo
	case "debug", "dbg":
		return LogDebug
	default:
		return LogOff
	}
}

var _ Logger = (*nopLogger)(nil)
var _ Logger = (*standardLogger)(nil)
