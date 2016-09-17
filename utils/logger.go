package utils

import (
	"bufio"
	"bytes"
	"os"
	"runtime/debug"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type fields map[string]interface{}

var GOPATH = ""
var GlobalTrace = true

//var log = logrus.New() // Default console logger.

func init() {
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})
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

func LogDebug(msg string, fields log.Fields, data ...interface{}) {
	log.WithFields(fields).Debugf(msg, data...)
}

func LogInfo(msg string, fields log.Fields, data ...interface{}) {
	log.WithFields(fields).Infof(msg, data...)
}

func LogWarn(msg string, fields log.Fields, data ...interface{}) {
	log.WithFields(fields).Warnf(msg, data...)
}

func LogError(msg string, fields log.Fields, data ...interface{}) {
	log.WithFields(fields).Errorf(msg, data...)
}
