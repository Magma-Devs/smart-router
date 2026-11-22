package utils

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// readStderr runs fn with os.Stderr redirected to a pipe and returns everything written to it.
// Same technique as TestLoggingStateChangesWhileOthersLog's os.Stderr swap (log_concurrency_test.go):
// newStdLogger hardcodes os.Stderr as its sink, so there is no narrower seam to capture through.
func readStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	prev := os.Stderr
	os.Stderr = w
	fn()
	os.Stderr = prev
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return string(out)
}

// lineContaining returns the line of blob holding substr, or fails the test.
func lineContaining(t *testing.T, blob, substr string) string {
	t.Helper()
	for _, line := range strings.Split(blob, "\n") {
		if strings.Contains(line, substr) {
			return line
		}
	}
	t.Fatalf("no line contains %q; full output:\n%s", substr, blob)
	return ""
}

// TestLogFormat_JSONvsText is MAG-1881 item 5.4.3: --log-format had no test at all. JsonFormat is
// read inside newStdLogger, itself only rebuilt by SetGlobalLoggingLevel, so the level is set
// alongside the format on each run rather than exercising JsonFormat through some narrower seam.
func TestLogFormat_JSONvsText(t *testing.T) {
	prevFormat := JsonFormat
	t.Cleanup(func() {
		JsonFormat = prevFormat
		SetGlobalLoggingLevel("debug") // this package's test-binary default
	})

	t.Run("json format emits a valid JSON record", func(t *testing.T) {
		JsonFormat = true
		out := readStderr(t, func() {
			SetGlobalLoggingLevel("info")
			_ = FormatInfo("json line marker")
		})
		line := lineContaining(t, out, "json line marker")
		require.True(t, json.Valid([]byte(line)), "a --log-format json line must be valid JSON: %s", line)
	})

	t.Run("text format emits a human-readable line, not JSON", func(t *testing.T) {
		JsonFormat = false
		out := readStderr(t, func() {
			SetGlobalLoggingLevel("info")
			_ = FormatInfo("text line marker")
		})
		line := lineContaining(t, out, "text line marker")
		require.False(t, json.Valid([]byte(line)), "a --log-format text line must not be raw JSON: %s", line)
	})
}
