//A package that provides logging functionality

package logger

import (
    "log"
    "fmt"
    "os"
)

type Logger struct {
    logger *log.Logger
    infoLabel string
    debugLabel string
    warnLabel string
    errorLabel string
}

//Get a Logger with output directed towards os.Stderr
//This logger includes time by default
//It also includes the process PID as prefix
func GetLogger(colour bool) *Logger {
    flags := log.LstdFlags | log.Lmicroseconds
    pre := fmt.Sprintf("[%d] ", os.Getpid())
    l := &Logger {
        logger: log.New(os.Stderr, pre, flags),
    }
    if colour {
        setColoredLabelFormats(l)
    } else {
        setPrefixes(l)
    }
    return l
}

func setPrefixes(l *Logger) {
    l.infoLabel = "[INFO] "
    l.debugLabel = "[DEBUG] "
    l.warnLabel = "[WARN] "
    l.errorLabel = "[ERROR] "
}

func setColoredLabelFormats(l *Logger) {
	colorFormat := "[\x1b[%sm%s\x1b[0m] "
	l.infoLabel = fmt.Sprintf(colorFormat, "32", "INFO")
	l.debugLabel = fmt.Sprintf(colorFormat, "36", "DEBUG")
	l.warnLabel = fmt.Sprintf(colorFormat, "0;93", "WARN")
	l.errorLabel = fmt.Sprintf(colorFormat, "31", "ERROR")
}

//Raise an Info log message
func (l *Logger) Infof(message string) {
    l.logger.Printf(l.infoLabel + message)
}

//Raise a Warn log message
func (l *Logger) Warnf(message string) {
    l.logger.Printf(l.warnLabel + message)
}

//Raise a Debug log message
func (l *Logger) Debugf(message string) {
    l.logger.Printf(l.debugLabel + message)
}

//Raise an Error log message
func (l *Logger) Errorf(message string) {
    l.logger.Printf(l.errorLabel + message)
}
