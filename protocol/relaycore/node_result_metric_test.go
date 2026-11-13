package relaycore

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// answerMetrics records which provider each metered node error and each metered result was for.
type answerMetrics struct {
	mu         sync.Mutex
	nodeErrors []string
	results    []string
}

func (m *answerMetrics) SetRelayNodeErrorMetric(_, _, providerAddress, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeErrors = append(m.nodeErrors, providerAddress)
}

func (m *answerMetrics) SetRelayNodeResultMetric(_, _, providerAddress, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, providerAddress)
}

func (m *answerMetrics) GetChainIdAndApiInterface() (string, string) {
	return "testId", "testInterface"
}

func (m *answerMetrics) counts() (nodeErrors, results []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.nodeErrors...), append([]string(nil), m.results...)
}

// MAG-5179: smartrouter_node_results_total counts the client reads a provider answered with a
// result, so a reader asking "is this provider serving clients" does not have to take node errors
// off rpc_endpoint_total_relays_serviced, which cannot be done for a write. Each case checks the
// processor took the response in (its results tally), so a case that meters nothing is not passing
// only because the response never arrived.
func TestRelayProcessor_MetersAReadAnsweredWithAResult(t *testing.T) {
	const provider = "lava@test1"
	nodeErrorBody := []byte(`{"message":"invalid argument 0: hex string has length 3","code":400}`)
	type tally struct{ success, nodeErrors, protocolErrors int }

	for _, tc := range []struct {
		name        string
		selection   Selection
		healthCheck bool
		send        func(*RelayProcessor)
		took        tally
		nodeErrors  []string
		results     []string
	}{
		{
			name:      "a read answered with a result",
			selection: Stateless,
			send:      func(rp *RelayProcessor) { SendSuccessResp(rp, provider, time.Millisecond) },
			took:      tally{success: 1},
			results:   []string{provider},
		},
		{
			name:       "a read answered with a node error is a node error, not a result",
			selection:  Stateless,
			send:       func(rp *RelayProcessor) { sendNodeErrorWithData(rp, provider, time.Millisecond, nodeErrorBody) },
			took:       tally{nodeErrors: 1},
			nodeErrors: []string{provider},
		},
		{
			name:      "an attempt with no answer is neither",
			selection: Stateless,
			send: func(rp *RelayProcessor) {
				SendProtocolError(rp, provider, time.Millisecond, errors.New("connection refused"))
			},
			took: tally{protocolErrors: 1},
		},
		{
			name:      "a write's answer is not a read",
			selection: Stateful,
			send:      func(rp *RelayProcessor) { SendSuccessResp(rp, provider, time.Millisecond) },
			took:      tally{success: 1},
		},
		{
			name:      "a write's refusal is metered as neither, as before",
			selection: Stateful,
			send:      func(rp *RelayProcessor) { sendNodeErrorWithData(rp, provider, time.Millisecond, nodeErrorBody) },
			took:      tally{nodeErrors: 1},
		},
		{
			name:        "the router's own health check is not a client's read",
			selection:   Stateless,
			healthCheck: true,
			send:        func(rp *RelayProcessor) { SendSuccessResp(rp, provider, time.Millisecond) },
			took:        tally{success: 1},
		},
		{
			name:        "the router's own health check still meters its node errors, as before",
			selection:   Stateless,
			healthCheck: true,
			send:        func(rp *RelayProcessor) { sendNodeErrorWithData(rp, provider, time.Millisecond, nodeErrorBody) },
			took:        tally{nodeErrors: 1},
			nodeErrors:  []string{provider},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, handler, nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)
			chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "", "")
			usedProviders := lavasession.NewUsedProviders(nil)
			if tc.healthCheck {
				usedProviders.MarkRouterHealthCheck()
			}
			recorded := &answerMetrics{}

			relayProcessor := NewRelayProcessor(ctx, nil, recorded, recorded,
				newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, tc.selection))

			lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			require.Nil(t, usedProviders.TryLockSelection(lockCtx))
			usedProviders.AddUsed(lavasession.ConsumerSessionsMap{provider: &lavasession.SessionInfo{}}, nil)

			// Responses are handled while WaitForResults runs, so the metrics are read after it returns.
			go tc.send(relayProcessor)
			waitCtx, waitCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer waitCancel()
			_ = relayProcessor.WaitForResults(waitCtx)

			success, nodeErrors, _, protocolErrors := relayProcessor.GetResults()
			require.Equal(t, tc.took, tally{success, nodeErrors, protocolErrors}, "the processor took the response in")
			gotNodeErrors, gotResults := recorded.counts()
			require.Equal(t, tc.nodeErrors, gotNodeErrors, "smartrouter_node_errors_total")
			require.Equal(t, tc.results, gotResults, "smartrouter_node_results_total")
		})
	}
}
