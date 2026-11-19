// Package testutil holds the helpers the tests of several packages share. Only _test.go files
// import it.
package testutil

import (
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	zerolog "github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// LogSink is a test binary's one zerolog sink, installed in TestMain before any test runs.
// utils/log writes through the global logger, and a store's credential watcher or a RESP
// cache's health probe keeps logging from its own goroutine for as long as it lives, so a
// helper that swapped the global logger per capture raced every such goroutine still running:
// the swap and restore against its reads of the logger, and its writes against the capture's
// unsynchronized buffer (go test -race, MAG-3671 review). With one sink for the life of the
// process there is nothing to swap; a capture only attaches a buffer to it, and every write
// and read of that buffer takes the same lock.
//
// Outside a capture, records go to stderr as they always did. Captures do not nest, and
// Capture fails the test that tries.
type LogSink struct {
	mu      sync.Mutex
	capture *bytes.Buffer
}

// InstallLogSink makes a new LogSink the global logger's writer for the life of the test
// binary. Call it from TestMain, before m.Run.
func InstallLogSink() *LogSink {
	sink := &LogSink{}
	utils.SetStdLogger(zerolog.New(sink))
	return sink
}

func (s *LogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.capture != nil {
		return s.capture.Write(p)
	}
	return os.Stderr.Write(p)
}

// Capture collects what utils/log writes while fn runs. A goroutine that outlives fn may still
// log afterwards; that goes back to stderr, never into a buffer being read.
func (s *LogSink) Capture(t testing.TB, fn func()) string {
	t.Helper()
	buf := &bytes.Buffer{}
	s.mu.Lock()
	nested := s.capture != nil
	if !nested {
		s.capture = buf
	}
	s.mu.Unlock()
	require.False(t, nested,
		"captures do not nest: another capture is still attached (a capture inside a capture, or a t.Parallel test capturing at the same time)")
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.capture = nil
	}()
	fn()
	s.mu.Lock()
	defer s.mu.Unlock()
	return buf.String()
}
