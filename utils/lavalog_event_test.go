package utils

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	zerolog "github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type testEvent struct{}

func (testEvent) MarshalZerologObject(e *zerolog.Event) {
	e.Str("event", "test_end").Int("schema", 1).Bool("flag", true)
}

// jsonLines parses every non-empty line of out as one JSON object.
func jsonLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(out), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &line), raw)
		lines = append(lines, line)
	}
	return lines
}

// A router runs at warn unless a tenant overrides it, and readers rely on the event at either
// level, so the configured level filters the narrative lines and never the event.
func TestLavaFormatEvent_WrittenWhateverTheLevel(t *testing.T) {
	for _, level := range []zerolog.Level{zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel} {
		t.Run(level.String(), func(t *testing.T) {
			out := captureLog(t, level, func() {
				LavaFormatInfo("narrative line")
				LavaFormatEvent("test_end", testEvent{})
			})

			var events, narrative int
			for _, line := range jsonLines(t, out) {
				switch line["message"] {
				case "narrative line":
					narrative++
				case "test_end":
					events++
					require.Equal(t, "info", line["level"])
					require.Equal(t, "test_end", line["event"])
					require.Equal(t, float64(1), line["schema"], "typed fields stay typed")
					require.Equal(t, true, line["flag"])
				}
			}
			require.Equal(t, 1, events, "the event is written once at every level")
			if level <= zerolog.InfoLevel {
				require.Equal(t, 1, narrative)
			} else {
				require.Zero(t, narrative, "the level still filters ordinary lines")
			}
		})
	}
}

func TestLavaFormatEvent_ReachesEveryEnabledSink(t *testing.T) {
	prevRolling := rollingLogLogger
	t.Cleanup(func() { rollingLogLogger = prevRolling })
	var rolling bytes.Buffer
	rollingLogLogger = zerolog.New(&rolling).Level(zerolog.ErrorLevel)

	EnableDebugLogBuffer(100)
	t.Cleanup(DisableDebugLogBuffer)

	captureLog(t, zerolog.ErrorLevel, func() { LavaFormatEvent("test_end", testEvent{}) })

	require.Len(t, jsonLines(t, rolling.String()), 1, "the rolling file's own level does not filter the event either")
	var buffered int
	for _, record := range ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
		if strings.Contains(string(record), `"event":"test_end"`) {
			buffered++
		}
	}
	require.Equal(t, 1, buffered)
}
