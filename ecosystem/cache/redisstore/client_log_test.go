package redisstore

import (
	"bufio"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func logLines(t *testing.T, logged string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(logged))
	for scanner.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line), "not a structured line: %s", scanner.Text())
		lines = append(lines, line)
	}
	return lines
}

func findLogLine(lines []map[string]any, message string) map[string]any {
	for _, line := range lines {
		if line["message"] == message {
			return line
		}
	}
	return nil
}

// A failure line is printed once per kind per interval. What an outage
// repeats in between is counted, and the count rides on the next line.
func TestClientLoggerLimitsFailureLinesPerKind(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	logger := &clientLogger{now: func() time.Time { return now }}
	const dialFailed = "redis: connection pool: failed to dial after %d attempts: %v"
	const askFailed = "sentinel: GetMasterAddrByName name=%q failed: %s"

	logged := captureLog(t, func() {
		for range 5 {
			logger.Printf(context.Background(), dialFailed, 1, "i/o timeout")
		}
		logger.Printf(context.Background(), askFailed, "mymaster", "i/o timeout")
		now = now.Add(clientLogInterval)
		logger.Printf(context.Background(), dialFailed, 1, "connection refused")
	})

	lines := logLines(t, logged)
	require.Len(t, lines, 3, "log: %s", logged)
	for _, line := range lines {
		require.Equal(t, "warn", line["level"])
		require.Equal(t, clientFailureMessage, line["message"])
	}
	require.Equal(t, "redis: connection pool: failed to dial after 1 attempts: i/o timeout", lines[0]["detail"])
	require.NotContains(t, lines[0], "suppressed")
	require.Equal(t, `sentinel: GetMasterAddrByName name="mymaster" failed: i/o timeout`, lines[1]["detail"], "another kind is not held back by the first")
	require.Equal(t, "redis: connection pool: failed to dial after 1 attempts: connection refused", lines[2]["detail"])
	require.Equal(t, "4", lines[2]["suppressed"])
}

// A change of primary is the line an operator reads after a failover: every
// one is printed, at info, however many arrive together.
func TestClientLoggerPrintsEveryStateChange(t *testing.T) {
	logger := &clientLogger{now: time.Now}

	logged := captureLog(t, func() {
		logger.Printf(context.Background(), "sentinel: new master=%q addr=%q", "mymaster", "10.0.0.1:6379")
		logger.Printf(context.Background(), "sentinel: new master=%q addr=%q", "mymaster", "10.0.0.2:6379")
	})

	lines := logLines(t, logged)
	require.Len(t, lines, 2, "log: %s", logged)
	for i, address := range []string{"10.0.0.1:6379", "10.0.0.2:6379"} {
		require.Equal(t, "info", lines[i]["level"])
		require.Equal(t, "resp-cache client follows a new primary", lines[i]["message"])
		require.Equal(t, "mymaster", lines[i]["master"])
		require.Equal(t, address, lines[i]["address"])
	}
}
