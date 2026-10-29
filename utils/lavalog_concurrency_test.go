package utils

import (
	"io"
	"os"
	"sync"
	"testing"

	zerolog "github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// MAG-4740: the level and the stderr sink change while other goroutines log. A test sets the level
// while an earlier test's goroutine is still logging, and a capture test swaps the sink. Under
// -race, any unsynchronised access to either fails this test.
func TestLoggingStateChangesWhileOthersLog(t *testing.T) {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, err)
	prevStderr, prevLevel := os.Stderr, currentLogLevel()
	os.Stderr = devNull // SetGlobalLoggingLevel's sink writes here
	restoreLogger := SetStdLogger(zerolog.New(io.Discard))
	t.Cleanup(func() {
		restoreLogger()
		globalLogLevel.Store(int32(prevLevel))
		os.Stderr = prevStderr
		_ = devNull.Close()
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = LavaFormatDebug("concurrent debug")
				_ = LavaFormatError("concurrent error", nil)
				_ = IsDebugEnabled()
			}
		}()
	}
	for i := range 50 {
		SetGlobalLoggingLevel([]string{"debug", "info", "warn", "error"}[i%4])
		restore := SetStdLogger(zerolog.New(io.Discard))
		restore()
	}
	close(stop)
	wg.Wait()
}
