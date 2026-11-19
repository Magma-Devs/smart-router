package redisstore

import (
	"os"
	"testing"

	"github.com/magma-Devs/smart-router/internal/testutil"
)

// logSink is this test binary's one zerolog sink; logSink.Capture collects what utils/log
// writes while a function runs.
var logSink *testutil.LogSink

func TestMain(m *testing.M) {
	logSink = testutil.InstallLogSink()
	os.Exit(m.Run())
}
