package performance_test

import (
	"bytes"
	"os"
	"sync"
	"testing"

	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// testLogSink is this test binary's one zerolog sink, installed in TestMain before any
// test runs. lavalog writes through the global logger, and a RESP cache's health probe
// keeps logging from its own goroutine for as long as the cache lives, so a helper that
// swapped the global logger per capture raced every probe still running: the swap and
// restore against the probe's reads of the logger, and the probe's writes against the
// capture's unsynchronized buffer (go test -race, MAG-3671 review). With one sink for the
// life of the process there is nothing to swap; a capture only attaches a buffer to it,
// and every write and read of that buffer takes the same lock.
//
// Outside a capture, records go to stderr as they always did. Captures do not nest, and
// captureLog fails the test that tries.
type testLogSink struct {
	mu      sync.Mutex
	capture *bytes.Buffer
}

func (s *testLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.capture != nil {
		return s.capture.Write(p)
	}
	return os.Stderr.Write(p)
}

// attach makes buf the capture, reporting false when another capture is still attached
// so the caller can fail loudly instead of silently dropping the outer capture's records.
func (s *testLogSink) attach(buf *bytes.Buffer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.capture != nil {
		return false
	}
	s.capture = buf
	return true
}

func (s *testLogSink) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capture = nil
}

// captureLog collects what lavalog writes while fn runs. A probe that outlives fn may
// still log afterwards; that goes back to stderr, never into a buffer being read.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	buf := &bytes.Buffer{}
	require.True(t, logSink.attach(buf),
		"captureLog does not nest: another capture is still attached (a capture inside a capture, or a t.Parallel test capturing at the same time)")
	defer logSink.detach()
	fn()
	logSink.mu.Lock()
	defer logSink.mu.Unlock()
	return buf.String()
}

var logSink = &testLogSink{}

func TestMain(m *testing.M) {
	zerologlog.Logger = zerolog.New(logSink)
	os.Exit(m.Run())
}
