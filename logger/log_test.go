package logger

import (
    "os"
    "bytes"
    "io"
    "testing"
    "strings"
    "fmt"
)

func TestInfoLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(false)
        logger.Infof("foo")
    }, "[INFO] foo\n")
}

func TestDebugLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(false)
        logger.Debugf("foo")
    }, "[DEBUG] foo\n")
}

func TestWarnLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(false)
        logger.Warnf("foo")
    }, "[WARN] foo\n")
}

func TestErrorLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(false)
        logger.Errorf("foo")
    }, "[ERROR] foo\n")
}

func TestColourInfoLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(true)
        logger.Infof("foo")
    }, "[\x1b[32mINFO\x1b[0m] foo")
}

func TestColourDebugLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(true)
        logger.Debugf("foo")
    }, "[\x1b[36mDEBUG\x1b[0m] foo")
}

func TestColourWarnLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(true)
        logger.Warnf("foo")
    }, "[\x1b[0;93mWARN\x1b[0m] foo")
}

func TestColourErrorLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(true)
        logger.Errorf("foo")
    }, "[\x1b[31mERROR\x1b[0m] foo")
}

func TestPidComesInFinalLog(t *testing.T) {
    expectOutput(t, func() {
        logger := GetLogger(false)
        logger.Errorf("foo")
    }, fmt.Sprintf("[%d]", os.Getpid()))
}

func expectOutput(t *testing.T, f func(), expected string) {
	old := os.Stderr // keep backup of the real stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	f()

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	os.Stderr.Close()
	os.Stderr = old // restoring the real stdout
	out := <-outC
	if !strings.Contains(out, expected) {
		t.Fatalf("Expected '%s', received '%s'\n", expected, out)
	}
}
