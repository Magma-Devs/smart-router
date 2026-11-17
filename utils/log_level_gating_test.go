package utils

import (
	"bytes"
	"testing"

	zerolog "github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// captureAtLevel sets the global log level and returns a buffer that receives everything
// Format* writes from here on. SetGlobalLoggingLevel rebuilds stdLogger against os.Stderr every
// time it runs, so the capture has to be injected AFTER it, not before. The injected logger's
// own level is Trace so it never filters internally — the gate under test is FormatLog's
// currentLogLevel() check, not zerolog's own.
func captureAtLevel(t *testing.T, level string) *bytes.Buffer {
	t.Helper()
	SetGlobalLoggingLevel(level)
	buf := &bytes.Buffer{}
	t.Cleanup(SetStdLogger(zerolog.New(buf).Level(zerolog.TraceLevel)))
	return buf
}

// TestSetGlobalLoggingLevel_GatesBySeverity is MAG-1881 item 5.4.1: the ticket's audit found
// --log-level tested only incidentally, as a side effect of a cache test exercising info and
// debug (rpcsmartrouter/cache_lookup_log_level_test.go), and never at warn. This tests the level
// gate itself, directly, for all three.
func TestSetGlobalLoggingLevel_GatesBySeverity(t *testing.T) {
	t.Cleanup(func() { SetGlobalLoggingLevel("debug") }) // this package's test-binary default

	t.Run("warn suppresses info and debug", func(t *testing.T) {
		buf := captureAtLevel(t, "warn")
		_ = FormatInfo("should not appear")
		_ = FormatDebug("should not appear either")
		require.Empty(t, buf.String())
		_ = FormatWarning("should appear", nil)
		require.Contains(t, buf.String(), "should appear")
	})

	t.Run("debug lets debug through", func(t *testing.T) {
		buf := captureAtLevel(t, "debug")
		_ = FormatDebug("debug line")
		require.Contains(t, buf.String(), "debug line")
	})

	t.Run("info suppresses debug but not info", func(t *testing.T) {
		buf := captureAtLevel(t, "info")
		_ = FormatDebug("should not appear")
		require.Empty(t, buf.String())
		_ = FormatInfo("info line")
		require.Contains(t, buf.String(), "info line")
	})
}
