package message_go

import (
	"fmt"
	"log"
	"os"
)

type Logger interface {
	Trace(msg string, args ...any)
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type LogLevel int

const (
	Trace LogLevel = iota
	Debug
	Info
	Warn
	Error
)

var _ Logger = (*logger)(nil)

var DefaultLogger Logger = NewLogger(Trace)

type logger struct {
	minLevel LogLevel
	loggers  map[LogLevel]*log.Logger
}

func NewLogger(minLevel LogLevel) Logger {
	flags := log.Lmsgprefix | log.LstdFlags
	return &logger{
		minLevel: minLevel,
		loggers: map[LogLevel]*log.Logger{
			Trace: log.New(os.Stdout, "\033[1;97;105mTrace\033[0m ", flags),
			Debug: log.New(os.Stdout, "\033[1;97;102mDebug\033[0m ", flags),
			Info:  log.New(os.Stdout, "\033[1;97;104mInfo\033[0m ", flags),
			Warn:  log.New(os.Stdout, "\033[1;97;103mWarn\033[0m ", flags),
			Error: log.New(os.Stdout, "\033[1;97;101mError\033[0m ", flags),
		},
	}
}

func (l *logger) MinLevel() LogLevel {
	return l.minLevel
}

func (l *logger) write(level LogLevel, msg string) {
	if l.minLevel <= level {
		_ = l.loggers[level].Output(2, msg)
	}
}

func (l *logger) Trace(msg string, args ...any) {
	l.write(Trace, fmt.Sprintf(msg, args...))
}

func (l *logger) Debug(msg string, args ...any) {
	l.write(Debug, fmt.Sprintf(msg, args...))
}

func (l *logger) Info(msg string, args ...any) {
	l.write(Info, fmt.Sprintf(msg, args...))
}

func (l *logger) Warn(msg string, args ...any) {
	l.write(Warn, fmt.Sprintf(msg, args...))
}

func (l *logger) Error(msg string, args ...any) {
	l.write(Error, fmt.Sprintf(msg, args...))
}
