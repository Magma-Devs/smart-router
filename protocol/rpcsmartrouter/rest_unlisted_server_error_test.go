package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// unlistedServerStatuses are REST 5xx statuses no status row names.
var unlistedServerStatuses = []int{505, 506, 507, 508, 510, 511, 599}

// TestRESTRelay_UnlistedServerError_IsAProviderFault_MAG5490: a bare REST 5xx that no status row
// names, empty or a proxy's HTML page, is a provider fault (rulebook Part V 3.1 P8): retried on
// another provider, and the endpoint at fault. It was UNKNOWN_ERROR: retried, but recorded and
// counted as nobody's fault.
func TestRESTRelay_UnlistedServerError_IsAProviderFault_MAG5490(t *testing.T) {
	for _, status := range unlistedServerStatuses {
		for _, body := range []string{"", fmt.Sprintf(`<html><body><h1>%d</h1><hr><center>nginx</center></body></html>`, status)} {
			t.Run(fmt.Sprintf("%d/%q", status, body), func(t *testing.T) {
				result := sendRESTThroughMockUpstream(t, status, body)
				assert.True(t, result.IsNodeError)
				assert.False(t, result.IsNonRetryable, "retried on another provider")
				assert.True(t, result.IsNodeAtFault, "a 5xx no row names is the endpoint's fault")
			})
		}
	}
}

// Through relayInnerDirect such a reply is still a transport error, JSON body or not, as a 502 or 504
// is: the request may have reached the node, so a write answered with one stays unclear. Its error
// is classified as the provider fault, and still may have reached the node.
func TestRESTRelay_UnlistedServerError_StaysATransportError_MAG5490(t *testing.T) {
	for _, status := range unlistedServerStatuses {
		for _, body := range []string{"", `{"message":"upstream error"}`} {
			t.Run(fmt.Sprintf("%d/%q", status, body), func(t *testing.T) {
				relayResult, relayErr := relayInnerDirectREST(t, status, body)
				require.Error(t, relayErr, "still a transport error")
				assert.True(t, relayResult.IsNodeAtFault)

				classified, _ := classifyDirectRPCError(relayErr, common.ChainFamilyUnknown, common.TransportREST)
				assert.Same(t, common.RouterErrorNodeUnlistedServerError, classified)
				assert.True(t, classified.MayHaveReachedNode, "a write answered with it stays unclear")
			})
		}
	}
}

// recordedProtocolErrorNames is the error_name of every "could not send relay to provider" line the
// relay with guid wrote: the results manager's record of each attempt that failed without an answer.
func recordedProtocolErrorNames(t *testing.T, guid uint64) []string {
	t.Helper()
	mine := `"GUID":"` + strconv.FormatUint(guid, 10) + `"`
	var names []string
	for _, record := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 200000) {
		line := strings.TrimSpace(string(record))
		if !strings.Contains(line, `"message":"could not send relay to provider"`) || !strings.Contains(line, mine) {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &fields), line)
		names = append(names, fmt.Sprint(fields["error_name"]))
	}
	return names
}

// TestRESTRelay_UnlistedServerError_IsRecordedAsAProviderFault_MAG5490: through a real router, the
// relay's own record of an attempt that failed on such a status names the provider fault, as the
// sender does: relay_end's attempts and its error_code, the coded log line and the error metric. All
// four said UNKNOWN_ERROR. A status another row names, a status no row names, a 501, JSON-RPC and a
// write keep what they had, and the write is still unclear.
func TestRESTRelay_UnlistedServerError_IsRecordedAsAProviderFault_MAG5490(t *testing.T) {
	rand.InitRandomSeed()
	var metricMu sync.Mutex
	metricCounts := map[string]int{}
	common.SetErrorMetricsCallback(func(_ uint32, name, _ string, _ bool, _ string) {
		metricMu.Lock()
		defer metricMu.Unlock()
		metricCounts[name]++
	})
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })
	metricCount := func(name string) int {
		metricMu.Lock()
		defer metricMu.Unlock()
		return metricCounts[name]
	}
	read := captureRelayEnds(t)
	rest := relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest)
	eth := ethJsonRPCParser(t)

	const (
		balance = "/cosmos/bank/v1beta1/balances/cosmos1demoacme"
		txs     = "/cosmos/tx/v1beta1/txs"
		tx      = `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`
		nginx   = `<html><head><title>507 Insufficient Storage</title></head><body><center><h1>507 Insufficient Storage</h1></center><hr><center>nginx</center></body></html>`
	)
	unlisted := common.RouterErrorNodeUnlistedServerError.Name
	cases := []struct {
		name         string
		chainID      string
		apiInterface string
		parser       chainlib.ChainParser
		status       int
		body         string
		method, url  string
		request      string
		// wantAttempt is every attempt's error_code. A transport failure is also a coded line and
		// relay_end's own error_code; a node error is neither.
		wantAttempt string
		transport   bool
		wantOutcome string
	}{
		{"REST read 507, empty", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 507, "", http.MethodGet, balance, "", unlisted, true, relayOutcomeNoAnswer},
		{"REST read 507, a proxy's page", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 507, nginx, http.MethodGet, balance, "", unlisted, true, relayOutcomeNoAnswer},
		{"REST read 599, empty", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 599, "", http.MethodGet, balance, "", unlisted, true, relayOutcomeNoAnswer},
		{"REST write 507, empty", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 507, "", http.MethodPost, txs, tx, unlisted, true, relayOutcomeWriteUnknown},

		{"control: REST read 503", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 503, "", http.MethodGet, balance, "", common.RouterErrorNodeServiceUnavailable.Name, true, relayOutcomeNoAnswer},
		{"control: REST read 600, no row anywhere", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 600, "", http.MethodGet, balance, "", common.RouterErrorUnknown.Name, true, relayOutcomeNoAnswer},
		{"control: REST read 501, a node error", "COSMOSHUB", spectypes.APIInterfaceRest, rest, 501, "", http.MethodGet, balance, "", common.RouterErrorNodeUnimplemented.Name, false, relayOutcomeNodeError},
		{"control: JSON-RPC read 507", "ETH1", spectypes.APIInterfaceJsonRPC, eth, 507, "", http.MethodPost, "", relayEndRead, common.RouterErrorUnknown.Name, true, relayOutcomeNoAnswer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newRelayEndServer(t, tc.chainID, tc.apiInterface, tc.parser,
				relayEndNode{"node-a", relayEndReply(tc.status, tc.body, 0)}, relayEndNode{"node-b", relayEndReply(tc.status, tc.body, 0)})
			guid := relayEndCaseGUID("mag5490 " + tc.name)
			metricsBefore := metricCount(tc.wantAttempt)
			ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), relayEndShortWait)
			defer cancel()
			_, err := server.SendRelay(ctx, tc.url, tc.request, tc.method, relayEndNoCustomerID, "", nil, nil)

			var event map[string]any
			require.NoError(t, json.Unmarshal([]byte(onlyRelayEnd(t, read(), guid)), &event))
			require.Equal(t, tc.wantOutcome, event["outcome"])
			attempts, isList := event["attempts"].([]any)
			require.True(t, isList, "attempts")
			require.NotEmpty(t, attempts)
			for _, item := range attempts {
				attempt, isObject := item.(map[string]any)
				require.True(t, isObject, "an attempt is an object")
				require.Equal(t, tc.wantAttempt, attempt["error_code"], "relay_end attempt %v", attempt)
				if tc.transport {
					require.Equal(t, attemptOutcomeTransportError, attempt["outcome"], "relay_end attempt %v", attempt)
				}
			}

			coded := recordedProtocolErrorNames(t, guid)
			if !tc.transport {
				require.Empty(t, coded, "a node error is not a protocol error")
				return
			}
			require.Equal(t, tc.wantAttempt, event["error_code"], "relay_end error_code")
			require.Len(t, coded, len(attempts), "one coded line per failed attempt")
			for _, name := range coded {
				require.Equal(t, tc.wantAttempt, name, "the coded log line")
			}
			require.GreaterOrEqual(t, metricCount(tc.wantAttempt)-metricsBefore, len(attempts), "the error metric")

			if tc.wantOutcome == relayOutcomeWriteUnknown {
				// Still unclear, through 2022's MayHaveReachedNode, and with the reason UNKNOWN_ERROR gave.
				var unclear *unclearWriteError
				require.ErrorAs(t, err, &unclear)
				require.Same(t, errUnclearWriteNoAnswer, unclear)
			}
		})
	}
}
