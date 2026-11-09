package provideroptimizer

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// MAG-3152: a router started with --debug-address keeps its recent log records in a ring for
// /debug/logs, and the automation attaches that window to every failure. The selector used to
// write debug records per candidate per pick (the "Provider score calculation breakdown" and
// the normalisation lines behind it), so on a busy router at debug level scoring filled the
// window and the records that would have explained a failure were already gone. A pick now
// writes the same debug records however many candidates it weighs: one "Provider selection
// completed" that carries every candidate's breakdown, raw inputs included.
func TestAPickWritesOneDebugRecordWhateverItsCandidates(t *testing.T) {
	utils.SetGlobalLoggingLevel("debug")
	t.Cleanup(func() { utils.SetGlobalLoggingLevel("info") }) // the package's level, set in log_level_test.go
	utils.EnableDebugLogBuffer(0)                             // the ring's default capacity, far more than a pick writes
	t.Cleanup(utils.DisableDebugLogBuffer)

	ws := NewUpstreamSelector(DefaultUpstreamSelectorConfig())
	qos := func(string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		return createQoSReport(0.95, 0.2, 2.0), time.Now(), true
	}
	stakes := map[string]int64{"provider1": 1000, "provider2": 2000, "provider3": 3000}
	stake := func(address string) int64 { return stakes[address] }
	pick := func(addresses ...string) []map[string]any {
		utils.ClearDebugLogBuffer()
		scores, _, details := ws.CalculateUpstreamScores(addresses, nil, qos, stake)
		ws.SelectUpstreamWithStats(context.Background(), scores, details)
		records := []map[string]any{}
		for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
			record := map[string]any{}
			require.NoError(t, json.Unmarshal(line, &record))
			records = append(records, record)
		}
		return records
	}

	two, three := pick("provider1", "provider2"), pick("provider1", "provider2", "provider3")
	require.Equal(t, len(two), len(three),
		"a pick over 3 candidates wrote %d debug records and a pick over 2 wrote %d: another candidate must not add one", len(three), len(two))

	// The record that remains names every candidate with its raw stake: the debug ring is the
	// only place the stake a running router loaded can be read back.
	var selection map[string]any
	for _, record := range three {
		if record["message"] == "Provider selection completed" {
			selection = record
		}
	}
	require.NotNil(t, selection, "the pick's debug record is missing: %v", three)
	for i, address := range []string{"provider1", "provider2", "provider3"} {
		prefix := "candidate_" + strconv.Itoa(i+1)
		require.Equal(t, address, selection[prefix+"_provider"])
		require.Equal(t, strconv.FormatInt(stakes[address], 10), selection[prefix+"_raw_stake"], prefix)
	}
}
