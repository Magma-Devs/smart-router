package common

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// The level is the whole point of the helper: a router at the default level writes a write's
// line and skips a read's. The `stateful` text is what a log collector matches to label the
// line as part of a transaction, so it is pinned exactly.
func TestLogRelayStep_LevelFollowsStateful(t *testing.T) {
	const ringCapacity = 1000
	for _, tc := range []struct {
		name         string
		stateful     uint32
		wantLevel    string
		wantStateful string
	}{
		{name: "a write stays at info", stateful: CONSISTENCY_SELECT_ALL_PROVIDERS, wantLevel: "info", wantStateful: "1"},
		{name: "a read drops to debug", stateful: NO_STATE, wantLevel: "debug", wantStateful: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			utils.EnableDebugLogBuffer(ringCapacity)
			t.Cleanup(utils.DisableDebugLogBuffer)
			utils.ClearDebugLogBuffer()

			LogRelayStep(tc.stateful, "relay step under test", utils.LogAttr("num_endpoints", 3))

			var line map[string]any
			for _, raw := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, ringCapacity) {
				record := map[string]any{}
				if json.Unmarshal(raw, &record) == nil && record["message"] == "relay step under test" {
					line = record
				}
			}
			require.NotNil(t, line, "the line must reach the log")
			require.Equal(t, tc.wantLevel, line["level"])
			require.Equal(t, tc.wantStateful, line["stateful"])
			require.Equal(t, "3", line["num_endpoints"], "the caller's own fields are kept")
		})
	}
}
