package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type fields map[string]interface{}

var GOPATH = os.Getenv("GOPATH")
var GlobalTrace = true

//var log = logrus.New() // Default console logger.

func init() {
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}

// stackInfo returns printable stack trace.
func stackInfo() string {
	// Convert stack-trace bytes to io.Reader.
	rawStack := bufio.NewReader(bytes.NewBuffer(debug.Stack()))
	// Skip stack trace lines until our real caller.
	for i := 0; i <= 4; i++ {
		rawStack.ReadLine()
	}

	// Read the rest of useful stack trace.
	stackBuf := new(bytes.Buffer)
	stackBuf.ReadFrom(rawStack)

	// Strip GOPATH of the build system and return.
	return strings.Replace(stackBuf.String(), GOPATH+"/src/", "", -1)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func ErrorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	fields := log.Fields{
		"cause": err.Error(),
	}
	if GlobalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Errorf(msg, data...)
}

// fatalIf wrapper function which takes error and prints jsonic error messages.
func FatalIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	fields := log.Fields{
		"cause": err.Error(),
	}
	if GlobalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Fatalf(msg, data...)
}

const (
	nocolor = 0
	pink    = 31
	green   = 32
	yellow  = 33
	purple  = 34
	red     = 35
	blue    = 36
	gray    = 37
)

func wrap_msg(msg string, err error, color int) string {
	pc, filename, line, _ := runtime.Caller(2)

	func_fullname := runtime.FuncForPC(pc).Name()
	func_name := filepath.Ext(func_fullname)[1:]

	srcfile := strings.Replace(filename, GOPATH+"/src/github.com/uukuguy/", "", -1)
	if err != nil {
		b := &bytes.Buffer{}
		fmt.Fprintf(b, "\x1b[%dm{ %s } %v\x1b[0m", color, msg, err)
		str_msg_err := b.Bytes()
		return fmt.Sprintf("%s:%d %s() %s ", srcfile, line, func_name, str_msg_err)
	} else {
		b := &bytes.Buffer{}
		fmt.Fprintf(b, "\x1b[%dm{ %s }\x1b[0m", color, msg)
		str_msg_err := b.Bytes()
		return fmt.Sprintf("%s:%d %s() %s", srcfile, line, func_name, str_msg_err)
	}
}

// ======== LogDebugf() ========
func LogDebugf(msg string, data ...interface{}) {
	log.Debugf(wrap_msg(msg, nil, gray), data...)
}

// ======== LogInfof() ========
func LogInfof(msg string, data ...interface{}) {
	log.Infof(wrap_msg(msg, nil, purple), data...)
}

// ======== LogWarnf() ========
func LogWarnf(err error, msg string, data ...interface{}) {
	log.Warnf(wrap_msg(msg, err, yellow), data...)
}

// ======== LogErrorf() ========
func LogErrorf(err error, msg string, data ...interface{}) {
	log.Errorf(wrap_msg(msg, err, pink), data...)
}

// ======== LogFatalf() ========
func LogFatalf(err error, msg string, data ...interface{}) {
	log.Fatalf(wrap_msg(msg, err, pink), data...)
}

// ======== LogPanicf() ========
func LogPanicf(err error, msg string, data ...interface{}) {
	log.Panicf(wrap_msg(msg, err, yellow), data...)
}
