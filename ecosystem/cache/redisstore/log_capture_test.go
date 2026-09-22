package redisstore

import (
	"bytes"
	"os"
	"sync"
	"testing"

	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// testLogSink is this test binary's one zerolog sink, installed in TestMain
// before any test runs. lavalog writes through the global logger, and the
// credential watcher keeps logging from its own goroutine for as long as its
// store lives, so a helper that swapped the global logger per capture raced
// every watcher still running: the swap and restore against the watcher's
// reads of the logger, and the watcher's writes against the capture's
// unsynchronized buffer (the shape go test -race caught in protocol/performance,
// MAG-3671 review). With one sink for the life of the process there is nothing
// to swap; a capture only attaches a buffer to it, and every write and read of
// that buffer takes the same lock.
//
// Outside a capture, records go to stderr as they always did. Captures do not
// nest, and attach fails the test that tries.
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

func (s *testLogSink) attach(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Nil(t, s.capture, "captureLog does not nest: a nested capture would silently take the outer one's records")
	s.capture = buf
}

func (s *testLogSink) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capture = nil
}

// captureLog collects what lavalog writes while fn runs. A goroutine that
// outlives fn may still log afterwards; that goes back to stderr, never into a
// buffer being read.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	buf := &bytes.Buffer{}
	logSink.attach(t, buf)
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
