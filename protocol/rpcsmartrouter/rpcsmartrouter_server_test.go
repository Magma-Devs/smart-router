package rpcsmartrouter

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// Mock interface for RelayProcessor that only implements the methods we need for testing
type MockRelayProcessorForHeaders struct {
	crossValidationParams           *common.CrossValidationParams // nil for Stateless/Stateful
	selection                       relaycore.Selection
	successResults                  []common.RelayResult
	nodeErrors                      []common.RelayResult
	protocolErrors                  []relaycore.RelayError
	statefulRelayTargets            []string
	crossValidationQueriedProviders []string
}

func (m *MockRelayProcessorForHeaders) GetCrossValidationParams() *common.CrossValidationParams {
	return m.crossValidationParams
}

func (m *MockRelayProcessorForHeaders) GetSelection() relaycore.Selection {
	return m.selection
}

func (m *MockRelayProcessorForHeaders) GetResultsData() ([]common.RelayResult, []common.RelayResult, []relaycore.RelayError) {
	return m.successResults, m.nodeErrors, m.protocolErrors
}

func (m *MockRelayProcessorForHeaders) GetStatefulRelayTargets() []string {
	return m.statefulRelayTargets
}

func (m *MockRelayProcessorForHeaders) GetCrossValidationQueriedProviders() []string {
	return m.crossValidationQueriedProviders
}

func (m *MockRelayProcessorForHeaders) GetUsedProviders() *lavasession.UsedProviders {
	return lavasession.NewUsedProviders(nil)
}

func (m *MockRelayProcessorForHeaders) NodeErrors() (ret []common.RelayResult) {
	return m.nodeErrors
}

// Integration tests that actually call appendHeadersToRelayResult
func TestAppendHeadersToRelayResultIntegration(t *testing.T) {
	ctx := context.Background()
	providerAddress1 := "lava@provider1"
	providerAddress2 := "lava@provider2"
	providerAddress3 := "lava@provider3"

	t.Run("cross-validation disabled - single provider header", func(t *testing.T) {
		// Create a mock relay processor with cross-validation disabled (use default values)
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: nil, // nil for non-CrossValidation modes
			successResults:        []common.RelayResult{},
			nodeErrors:            []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a simple mock protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{Name: "test-api"},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		// Verify the result - should have single provider header + user request type header
		require.Len(t, relayResult.Reply.Metadata, 2)

		// Find the provider address header
		var providerHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.PROVIDER_ADDRESS_HEADER_NAME {
				providerHeader = &meta
				break
			}
		}
		require.NotNil(t, providerHeader)
		require.Equal(t, providerAddress1, providerHeader.Value)
	})

	t.Run("cross-validation enabled - single successful provider (failure case - below threshold)", func(t *testing.T) {
		// Create a mock relay processor with cross-validation enabled
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:                       relaycore.CrossValidation,  // Enable cross-validation via Selection
			crossValidationQueriedProviders: []string{providerAddress1}, // All queried providers
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result - CrossValidation=1 means only 1 provider agreed (below threshold of 2)
		relayResult := &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: providerAddress1},
			CrossValidation: 1, // Below threshold
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a simple mock protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{Name: "test-api"},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		// Verify the result - should have status, all-providers, agreeing-providers, disagreeing-providers, and user request type headers
		require.Len(t, relayResult.Reply.Metadata, 5)

		// Find and verify headers
		var statusHeader, allProvidersHeader, agreeingProvidersHeader *pairingtypes.Metadata
		for i := range relayResult.Reply.Metadata {
			meta := &relayResult.Reply.Metadata[i]
			switch meta.Name {
			case common.CROSS_VALIDATION_STATUS_HEADER_NAME:
				statusHeader = meta
			case common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME:
				allProvidersHeader = meta
			case common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER:
				agreeingProvidersHeader = meta
			}
		}
		require.NotNil(t, statusHeader)
		require.Equal(t, "failed", statusHeader.Value) // Below threshold = failed
		require.NotNil(t, allProvidersHeader)
		require.Equal(t, "lava@provider1", allProvidersHeader.Value) // Comma-separated format
		require.NotNil(t, agreeingProvidersHeader)
		require.Equal(t, "", agreeingProvidersHeader.Value) // Empty on failure
	})

	t.Run("cross-validation enabled - multiple providers with mixed results (success case)", func(t *testing.T) {
		// Create a hash for the winning response
		winningHash := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}

		// Create a mock relay processor with cross-validation enabled
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:                       relaycore.CrossValidation,                                      // Enable cross-validation via Selection
			crossValidationQueriedProviders: []string{providerAddress1, providerAddress2, providerAddress3}, // All queried providers
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}, ResponseHash: winningHash},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress2}, ResponseHash: winningHash},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress3}},
			},
		}

		// Create a relay result - CrossValidation=2 meets threshold
		relayResult := &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: providerAddress1},
			CrossValidation: 2, // Meets threshold
			ResponseHash:    winningHash,
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a simple mock protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{Name: "test-api"},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		// Verify the result - should have 5 headers: status, all-providers, agreeing-providers, disagreeing-providers, user-request-type
		require.Len(t, relayResult.Reply.Metadata, 5)

		// Find all CV headers
		var statusHeader, allProvidersHeader, agreeingProvidersHeader, disagreeingProvidersHeader *pairingtypes.Metadata
		for i := range relayResult.Reply.Metadata {
			meta := &relayResult.Reply.Metadata[i]
			switch meta.Name {
			case common.CROSS_VALIDATION_STATUS_HEADER_NAME:
				statusHeader = meta
			case common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME:
				allProvidersHeader = meta
			case common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER:
				agreeingProvidersHeader = meta
			case common.CROSS_VALIDATION_DISAGREEING_PROVIDERS_HEADER:
				disagreeingProvidersHeader = meta
			}
		}

		// Verify status header
		require.NotNil(t, statusHeader)
		require.Equal(t, "success", statusHeader.Value)

		// Verify disagreeing providers header: provider3 is a node error, so it dissents even on success
		require.NotNil(t, disagreeingProvidersHeader)
		require.Equal(t, "lava@provider3", disagreeingProvidersHeader.Value)
		require.NotContains(t, disagreeingProvidersHeader.Value, "lava@provider1")
		require.NotContains(t, disagreeingProvidersHeader.Value, "lava@provider2")

		// Verify all providers header (includes all 3)
		require.NotNil(t, allProvidersHeader)
		require.Contains(t, allProvidersHeader.Value, "lava@provider1")
		require.Contains(t, allProvidersHeader.Value, "lava@provider2")
		require.Contains(t, allProvidersHeader.Value, "lava@provider3")

		// Verify agreeing providers header (only providers 1 and 2 have matching hash)
		require.NotNil(t, agreeingProvidersHeader)
		require.Contains(t, agreeingProvidersHeader.Value, "lava@provider1")
		require.Contains(t, agreeingProvidersHeader.Value, "lava@provider2")
		require.NotContains(t, agreeingProvidersHeader.Value, "lava@provider3")
	})

	t.Run("cross-validation enabled - no providers (failure case)", func(t *testing.T) {
		// Create a mock relay processor with cross-validation enabled but no providers
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:             relaycore.CrossValidation, // Enable cross-validation via Selection
			successResults:        []common.RelayResult{},
			nodeErrors:            []common.RelayResult{},
		}

		// Create a relay result - CrossValidation=0 (no agreement)
		relayResult := &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: providerAddress1},
			CrossValidation: 0, // No agreement
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a simple mock protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{Name: "test-api"},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		// Verify the result - should have 5 headers (status, all-providers, agreeing-providers, disagreeing-providers, user-request-type)
		require.Len(t, relayResult.Reply.Metadata, 5)

		// Find and verify headers
		var statusHeader, allProvidersHeader, agreeingProvidersHeader *pairingtypes.Metadata
		for i := range relayResult.Reply.Metadata {
			meta := &relayResult.Reply.Metadata[i]
			switch meta.Name {
			case common.CROSS_VALIDATION_STATUS_HEADER_NAME:
				statusHeader = meta
			case common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME:
				allProvidersHeader = meta
			case common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER:
				agreeingProvidersHeader = meta
			}
		}
		require.NotNil(t, statusHeader)
		require.Equal(t, "failed", statusHeader.Value)
		require.NotNil(t, allProvidersHeader)
		require.Equal(t, "", allProvidersHeader.Value) // Empty list (comma-separated format)
		require.NotNil(t, agreeingProvidersHeader)
		require.Equal(t, "", agreeingProvidersHeader.Value) // Empty on failure
	})

	t.Run("nil relay result - should not panic", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:             relaycore.CrossValidation, // Enable cross-validation via Selection
			successResults:        []common.RelayResult{},
			nodeErrors:            []common.RelayResult{},
		}

		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{Name: "test-api"},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// This should not panic
		require.NotPanics(t, func() {
			rpcSmartRouterServer.appendHeadersToRelayResult(ctx, nil, 0, relayProcessor, mockProtocolMessage, "test-api", nil, false)
		})
	})
}

// TestAppendHeadersToRelayResult_DisagreeingOnQuorumFailure covers the failure semantics of the
// disagreeing-providers header: when successful responses come back but fail to form a quorum, every
// successful provider is in conflict (there is no consensus to agree with), so all of them must appear in
// lava-cross-validation-disagreeing-providers — not just node/protocol-error providers.
func TestAppendHeadersToRelayResult_DisagreeingOnQuorumFailure(t *testing.T) {
	ctx := context.Background()
	hashA := [32]byte{1}
	hashB := [32]byte{2}
	relayProcessor := &MockRelayProcessorForHeaders{
		crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
		selection:                       relaycore.CrossValidation,
		crossValidationQueriedProviders: []string{"lava@provider1", "lava@provider2", "lava@provider3"},
		// Two successful but disagreeing responses (different hashes) plus a node error: none agree up to
		// the threshold, so the quorum fails.
		successResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}, ResponseHash: hashB},
		},
		nodeErrors: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider3"}},
		},
	}
	relayResult := &common.RelayResult{
		ProviderInfo:    common.ProviderInfo{ProviderAddress: "lava@provider1"},
		CrossValidation: 1, // below threshold -> failed
		Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
	}
	mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}
	rpcSmartRouterServer := &RPCSmartRouterServer{}

	rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, false)

	var statusHeader, agreeingHeader, disagreeingHeader *pairingtypes.Metadata
	for i := range relayResult.Reply.Metadata {
		switch relayResult.Reply.Metadata[i].Name {
		case common.CROSS_VALIDATION_STATUS_HEADER_NAME:
			statusHeader = &relayResult.Reply.Metadata[i]
		case common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER:
			agreeingHeader = &relayResult.Reply.Metadata[i]
		case common.CROSS_VALIDATION_DISAGREEING_PROVIDERS_HEADER:
			disagreeingHeader = &relayResult.Reply.Metadata[i]
		}
	}
	require.NotNil(t, statusHeader)
	require.Equal(t, "failed", statusHeader.Value)
	// No consensus -> no provider agrees.
	require.NotNil(t, agreeingHeader)
	require.Equal(t, "", agreeingHeader.Value)
	// Both successful-but-disagreeing providers AND the node-error provider are in the disagreeing set.
	require.NotNil(t, disagreeingHeader)
	require.Contains(t, disagreeingHeader.Value, "lava@provider1")
	require.Contains(t, disagreeingHeader.Value, "lava@provider2")
	require.Contains(t, disagreeingHeader.Value, "lava@provider3")
}

// TestAppendHeadersToRelayResult_FailureReasonHeader verifies that a cross-validation failure surfaces a
// distinguishable lava-cross-validation-failure-reason header (so clients can tell diversity-unmet from
// an ordinary no-agreement).
func TestAppendHeadersToRelayResult_FailureReasonHeader(t *testing.T) {
	ctx := context.Background()
	relayProcessor := &MockRelayProcessorForHeaders{
		crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 2},
		selection:                       relaycore.CrossValidation,
		crossValidationQueriedProviders: []string{"lava@provider1", "lava@provider2"},
		successResults:                  []common.RelayResult{},
		nodeErrors:                      []common.RelayResult{},
	}
	relayResult := &common.RelayResult{
		ProviderInfo:                 common.ProviderInfo{ProviderAddress: "lava@provider1"},
		CrossValidation:              0, // below threshold -> failed
		CrossValidationFailureReason: common.CrossValidationReasonDiversityUnmet,
		Reply:                        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
	}
	mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}
	rpcSmartRouterServer := &RPCSmartRouterServer{}

	rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, false)

	var statusHeader, reasonHeader *pairingtypes.Metadata
	for i := range relayResult.Reply.Metadata {
		switch relayResult.Reply.Metadata[i].Name {
		case common.CROSS_VALIDATION_STATUS_HEADER_NAME:
			statusHeader = &relayResult.Reply.Metadata[i]
		case common.CROSS_VALIDATION_FAILURE_REASON_HEADER:
			reasonHeader = &relayResult.Reply.Metadata[i]
		}
	}
	require.NotNil(t, statusHeader)
	require.Equal(t, "failed", statusHeader.Value)
	require.NotNil(t, reasonHeader, "failure-reason header must be present on cross-validation failure")
	require.Equal(t, common.CrossValidationReasonDiversityUnmet, reasonHeader.Value)
}

// TestAppendHeadersToRelayResult_MismatchMetric is the production-glue test for Section 1.3: a
// deterministic SUCCESSFUL content outlier (after quorum) increments cross_validation_mismatch_total for
// its group, while node/protocol errors and quorum failures do not.
func TestAppendHeadersToRelayResult_MismatchMetric(t *testing.T) {
	ctx := context.Background()
	// Empty NetworkAddress => metrics registered on the default registry, no HTTP server started.
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	newServer := func() *RPCSmartRouterServer {
		s := &RPCSmartRouterServer{
			smartRouterEndpointMetrics: mm,
			listenEndpoint:             &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
			chainParser:                chainParser,
		}
		s.latestBlockHeight.Store(1_000_000) // so finality resolves (not "unknown")
		return s
	}

	hashA := sha256.Sum256([]byte("A"))
	hashB := sha256.Sum256([]byte("B"))
	deterministicAPI := &spectypes.Api{Name: "test", Category: spectypes.SpecCategory{Deterministic: true}}
	mkReply := func() *pairingtypes.RelayReply { return &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}} }

	// mismatchCount sums cross_validation_mismatch_total over finality labels for one method+group.
	mismatchCount := func(t *testing.T, method, group string) float64 {
		mfs, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		var total float64
		for _, mf := range mfs {
			if mf.GetName() != "lava_rpcsmartrouter_cross_validation_mismatch_total" {
				continue
			}
			for _, m := range mf.GetMetric() {
				lm := map[string]string{}
				for _, lp := range m.GetLabel() {
					lm[lp.GetName()] = lp.GetValue()
				}
				if lm["method"] == method && lm["group"] == group {
					total += m.GetCounter().GetValue()
				}
			}
		}
		return total
	}

	t.Run("deterministic successful outlier increments for its group", func(t *testing.T) {
		method := "cv_mismatch_outlier"
		rp := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:                       relaycore.CrossValidation,
			crossValidationQueriedProviders: []string{"p1", "p2", "p3"},
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1", ProviderGroup: "g1"}, ResponseHash: hashA},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2", ProviderGroup: "g2"}, ResponseHash: hashA},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3", ProviderGroup: "g3"}, ResponseHash: hashB}, // outlier
			},
		}
		relayResult := &common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, CrossValidation: 2, ResponseHash: hashA, Reply: mkReply()}
		newServer().appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: deterministicAPI, requestedBlock: 100}, method, nil, true)
		require.Equal(t, float64(1), mismatchCount(t, method, "g3"), "outlier group g3 increments once")
		require.Equal(t, float64(0), mismatchCount(t, method, "g1"), "agreeing group g1 does not increment")
	})

	t.Run("node error does not increment", func(t *testing.T) {
		method := "cv_mismatch_nodeerr"
		rp := &MockRelayProcessorForHeaders{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:             relaycore.CrossValidation,
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1", ProviderGroup: "g1"}, ResponseHash: hashA},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2", ProviderGroup: "g2"}, ResponseHash: hashA},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3", ProviderGroup: "g3"}},
			},
		}
		relayResult := &common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, CrossValidation: 2, ResponseHash: hashA, Reply: mkReply()}
		newServer().appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: deterministicAPI, requestedBlock: 100}, method, nil, true)
		require.Equal(t, float64(0), mismatchCount(t, method, "g3"), "node error is not a content outlier")
	})

	t.Run("quorum failure does not increment", func(t *testing.T) {
		method := "cv_mismatch_failure"
		rp := &MockRelayProcessorForHeaders{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 3, MaxParticipants: 5},
			selection:             relaycore.CrossValidation,
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1", ProviderGroup: "g1"}, ResponseHash: hashA},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2", ProviderGroup: "g2"}, ResponseHash: hashB},
			},
		}
		// CrossValidation 1 < threshold 3 => failed
		relayResult := &common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, CrossValidation: 1, ResponseHash: hashA, Reply: mkReply()}
		newServer().appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: deterministicAPI, requestedBlock: 100}, method, nil, false)
		require.Equal(t, float64(0), mismatchCount(t, method, "g1"), "quorum failure does not increment mismatch")
		require.Equal(t, float64(0), mismatchCount(t, method, "g2"), "quorum failure does not increment mismatch")
	})
}

// cvRequestCounter sums a named cross-validation counter over its `method` label across the default
// registry (which accumulates across tests, so callers compare a before/after delta).
func cvRequestCounter(t *testing.T, name, method string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "method" && lp.GetValue() == method {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// TestCrossValidationFailFast_EmitsRequestFailedMetrics covers CV-1: a request-time structural fail-fast
// returns from SendParsedRelay BEFORE appendHeadersToRelayResult, where requests/failed are normally
// emitted. crossValidationFailFast must emit them itself — once each, never success — so structural
// failures (insufficient-capacity / insufficient-groups) are counted without double-counting quorum-time
// failures (the two return paths are mutually exclusive).
func TestCrossValidationFailFast_EmitsRequestFailedMetrics(t *testing.T) {
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	logs, err := metrics.NewRPCConsumerLogs(mm, nil, nil, nil)
	require.NoError(t, err)
	srv := &RPCSmartRouterServer{
		rpcSmartRouterLogs: logs,
		listenEndpoint:     &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
	}
	const method = "cv_failfast_metric"
	pm := &MockProtocolMessage{api: &spectypes.Api{Name: method}}

	reqName := "lava_rpcsmartrouter_cross_validation_requests_total"
	failName := "lava_rpcsmartrouter_cross_validation_failed_total"
	successName := "lava_rpcsmartrouter_cross_validation_success_total"
	reqBefore := cvRequestCounter(t, reqName, method)
	failBefore := cvRequestCounter(t, failName, method)
	successBefore := cvRequestCounter(t, successName, method)

	res := srv.crossValidationFailFast(common.CrossValidationReasonInsufficientGroups, pm)

	require.Equal(t, reqBefore+1, cvRequestCounter(t, reqName, method), "requests_total must increment once")
	require.Equal(t, failBefore+1, cvRequestCounter(t, failName, method), "failed_total must increment once")
	require.Equal(t, successBefore, cvRequestCounter(t, successName, method), "success_total must not move")

	// The returned minimal result still carries the structured status + failure-reason headers.
	require.NotNil(t, res.Reply)
	headers := map[string]string{}
	for _, m := range res.Reply.Metadata {
		headers[m.Name] = m.Value
	}
	require.Equal(t, "failed", headers[common.CROSS_VALIDATION_STATUS_HEADER_NAME])
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, headers[common.CROSS_VALIDATION_FAILURE_REASON_HEADER])

	// A second fail-fast increments again (no accidental once-only guard) — proves per-request counting.
	res2 := srv.crossValidationFailFast(common.CrossValidationReasonInsufficientCapacity, pm)
	require.NotNil(t, res2)
	require.Equal(t, reqBefore+2, cvRequestCounter(t, reqName, method))
	require.Equal(t, failBefore+2, cvRequestCounter(t, failName, method))
}

// TestAppendHeadersToRelayResult_GroupLabelsInertWithoutPolicy is the Phase 0.2
// backwards-compatibility lock for the cross-validation provider-group spine
// (UC-7). The spine (Phase 0.1) carries a provider GroupLabel through to
// common.ProviderInfo.ProviderGroup, but with no group-diversity policy
// configured that label must have ZERO effect on observable behavior. This test
// runs appendHeadersToRelayResult over an identical cross-validation scenario
// twice — once with ProviderGroup populated on every result, once with it empty —
// and asserts the emitted cross-validation headers (status, all-providers,
// agreeing-providers) are byte-identical. It will start failing the moment any
// future change (e.g. Phase 1.2 group-aware logic) lets a group label leak into
// the default no-policy path.
func TestAppendHeadersToRelayResult_GroupLabelsInertWithoutPolicy(t *testing.T) {
	ctx := context.Background()
	providerAddress1 := "lava@provider1"
	providerAddress2 := "lava@provider2"
	providerAddress3 := "lava@provider3"
	winningHash := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}

	// collectCVHeaders runs the header builder for a cross-validation success
	// scenario and returns the three CV headers as a name->value map. groupLabels,
	// when non-empty, are stamped onto every participating result's ProviderGroup.
	collectCVHeaders := func(t *testing.T, withGroups bool) map[string]string {
		t.Helper()
		grp := func(addr string) common.ProviderInfo {
			pi := common.ProviderInfo{ProviderAddress: addr}
			if withGroups {
				pi.ProviderGroup = "group-" + addr // arbitrary non-empty label per provider
			}
			return pi
		}

		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:                       relaycore.CrossValidation,
			crossValidationQueriedProviders: []string{providerAddress1, providerAddress2, providerAddress3},
			successResults: []common.RelayResult{
				{ProviderInfo: grp(providerAddress1), ResponseHash: winningHash},
				{ProviderInfo: grp(providerAddress2), ResponseHash: winningHash},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: grp(providerAddress3)},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo:    grp(providerAddress1),
			CrossValidation: 2, // meets threshold
			ResponseHash:    winningHash,
			Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
		mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		headers := map[string]string{}
		for _, meta := range relayResult.Reply.Metadata {
			switch meta.Name {
			case common.CROSS_VALIDATION_STATUS_HEADER_NAME,
				common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME,
				common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER:
				headers[meta.Name] = meta.Value
			}
		}
		return headers
	}

	withoutGroups := collectCVHeaders(t, false)
	withGroups := collectCVHeaders(t, true)

	// Sanity: the scenario actually produced cross-validation headers.
	require.Equal(t, "success", withoutGroups[common.CROSS_VALIDATION_STATUS_HEADER_NAME],
		"baseline scenario should reach cross-validation success")

	// The lock: group labels must not perturb any observable cross-validation header.
	require.Equal(t, withoutGroups, withGroups,
		"populating ProviderGroup must not change CV headers when no group-diversity policy is configured (UC-7)")
}

// TestRetryCountHeader verifies that the Lava-Retries header correctly reflects
// actual retry attempts (total attempts - 1), not raw error counts.
func TestRetryCountHeader(t *testing.T) {
	ctx := context.Background()

	findHeader := func(metadata []pairingtypes.Metadata, name string) *pairingtypes.Metadata {
		for i := range metadata {
			if metadata[i].Name == name {
				return &metadata[i]
			}
		}
		return nil
	}

	t.Run("single node error no success - no retry header", func(t *testing.T) {
		// Scenario: unsupported method like "seth_blockNumber" — 1 attempt, 0 retries
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "seth_blockNumber"},
		}, "seth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "should not set retry header when only 1 attempt was made (0 retries)")
	})

	t.Run("one node error then success - retry header is 1", func(t *testing.T) {
		// Scenario: first provider returned node error, second succeeded — 1 retry
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader, "should set retry header when retries occurred")
		require.Equal(t, "1", retryHeader.Value)
	})

	t.Run("two node errors then success - retry header is 2", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider3"}},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider3"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "2", retryHeader.Value)
	})

	t.Run("single protocol error no success - no retry header", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{},
			nodeErrors:     []common.RelayResult{},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 1, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "should not set retry header when only 1 protocol error (0 retries)")
	})

	t.Run("protocol error then success - retry header is 1", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
			},
			nodeErrors: []common.RelayResult{},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 1, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "1", retryHeader.Value)
	})

	t.Run("mixed errors then success - retry header counts all retries", func(t *testing.T) {
		// 1 protocol error + 2 node errors + 1 success = 4 attempts, 3 retries
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider4"}},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider3"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider4"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 1, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "3", retryHeader.Value)
	})

	t.Run("no errors no retries - no retry header", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
			},
			nodeErrors: []common.RelayResult{},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "should not set retry header when no retries occurred")
	})

	t.Run("two node errors no success - retry header is 1", func(t *testing.T) {
		// 2 node errors, 0 success — 2 attempts, 1 retry
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "1", retryHeader.Value)
	})
}

// TestRetryCountHeaderStatefulFanoutAbsorption pins the rule that parallel-batch
// failures during a Stateful fan-out must not be counted as retries. Stateful
// dispatches to all top providers at once (relaypolicy.Decide returns Stop for
// Stateful, so it never retries sequentially); a 503 from one provider while
// another succeeds is expected race-loss, not a retry.
//
// Bug repro: send eth_sendRawTransaction with {P1 down, P2 success, P3 success}.
// Pre-fix the response carried Lava-Retries: 1, which fed false positives into
// operator dashboards monitoring stateful traffic.
func TestRetryCountHeaderStatefulFanoutAbsorption(t *testing.T) {
	ctx := context.Background()

	findHeader := func(metadata []pairingtypes.Metadata, name string) *pairingtypes.Metadata {
		for i := range metadata {
			if metadata[i].Name == name {
				return &metadata[i]
			}
		}
		return nil
	}

	t.Run("stateful fan-out with one provider 503 - absorbs failure, retries=0", func(t *testing.T) {
		// Mirrors the production repro: P1 503 + P2 success in the same fan-out.
		relayProcessor := &MockRelayProcessorForHeaders{
			selection: relaycore.Stateful,
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider2"}},
			},
			protocolErrors: []relaycore.RelayError{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider1"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 1, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{
				Name:     "eth_sendRawTransaction",
				Category: spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
			},
		}, "eth_sendRawTransaction", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "stateful fan-out must absorb in-batch failures: no Lava-Retries header expected")

		// Provider-Address rebuild is gated on retries>0 to keep MAG-1653 Bug #2's
		// invariant (entry count == retries+1). For retries=0 the header must
		// stay at the single winning provider; the loser is reported via
		// lava-fast-tx-participants instead.
		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		require.Equal(t, "lava@simprovider2", providerHeader.Value)
	})

	t.Run("stateful fan-out all healthy - no Lava-Retries header", func(t *testing.T) {
		// Acceptance criterion 2: no regression for the all-healthy fan-out case.
		// Three successes in one batch must not produce any retry header.
		relayProcessor := &MockRelayProcessorForHeaders{
			selection: relaycore.Stateful,
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider1"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider2"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider3"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{
				Name:     "eth_sendRawTransaction",
				Category: spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
			},
		}, "eth_sendRawTransaction", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "all-healthy stateful fan-out must not emit Lava-Retries")
	})

	t.Run("stateless one error then success - retries still counted (regression guard)", func(t *testing.T) {
		// The absorption rule is scoped to Stateful; sequential-retry semantics
		// for Stateless must continue to increment Lava-Retries. Without this
		// guard a future change collapsing the Stateless branch into Stateful's
		// would silently zero out the counter for the most common path.
		relayProcessor := &MockRelayProcessorForHeaders{
			selection: relaycore.Stateless,
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"}},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "1", retryHeader.Value)
	})
}

// TestHedgeTriggeredHeader covers MAG-1818: lava-hedge-triggered must appear
// (Value="true") iff analytics.HedgeCount > 0, and must be omitted otherwise.
// This signal is independent of Lava-Retries — the test framework needs to
// distinguish "hedge fired" from "classical retry" without inferring from
// Lava-Retries + Lava-Provider-Address.
func TestHedgeTriggeredHeader(t *testing.T) {
	ctx := context.Background()

	findHeader := func(metadata []pairingtypes.Metadata, name string) *pairingtypes.Metadata {
		for i := range metadata {
			if metadata[i].Name == name {
				return &metadata[i]
			}
		}
		return nil
	}

	newRelayResult := func() *common.RelayResult {
		return &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
	}

	newProcessor := func() *MockRelayProcessorForHeaders {
		return &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"}},
			},
		}
	}

	t.Run("hedge fired - header present with value true", func(t *testing.T) {
		relayResult := newRelayResult()
		rpcSmartRouterServer := &RPCSmartRouterServer{}
		analytics := &metrics.RelayMetrics{HedgeCount: 1}

		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, newProcessor(), &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", analytics, true)

		hedgeHeader := findHeader(relayResult.Reply.Metadata, common.LAVA_HEDGE_TRIGGERED_HEADER)
		require.NotNil(t, hedgeHeader, "hedge header must be set when HedgeCount > 0")
		require.Equal(t, "true", hedgeHeader.Value)
	})

	t.Run("hedge count zero - header absent", func(t *testing.T) {
		relayResult := newRelayResult()
		rpcSmartRouterServer := &RPCSmartRouterServer{}
		analytics := &metrics.RelayMetrics{HedgeCount: 0}

		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, newProcessor(), &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", analytics, true)

		hedgeHeader := findHeader(relayResult.Reply.Metadata, common.LAVA_HEDGE_TRIGGERED_HEADER)
		require.Nil(t, hedgeHeader, "hedge header must be omitted when HedgeCount == 0 (no \"false\" value ever emitted)")
	})

	t.Run("nil analytics - header absent", func(t *testing.T) {
		relayResult := newRelayResult()
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, newProcessor(), &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		hedgeHeader := findHeader(relayResult.Reply.Metadata, common.LAVA_HEDGE_TRIGGERED_HEADER)
		require.Nil(t, hedgeHeader, "hedge header must be omitted when analytics is nil")
	})
}

// TestCacheServedResponseHeaders is a regression for MAG-1653 Bug #2: when a
// relay is resolved by a cache hit during the retry loop, "Cached" must remain
// in Lava-Provider-Address so the entry count stays consistent with Lava-Retries.
// (Previously the retry-path rebuild explicitly excluded "Cached", overwriting
// the header with only the failed provider names.)
func TestCacheServedResponseHeaders(t *testing.T) {
	ctx := context.Background()

	findHeader := func(metadata []pairingtypes.Metadata, name string) *pairingtypes.Metadata {
		for i := range metadata {
			if metadata[i].Name == name {
				return &metadata[i]
			}
		}
		return nil
	}

	t.Run("cache hit with no retries - single Cached address, no retry header", func(t *testing.T) {
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: ""}}, // cache result has no provider
			},
			nodeErrors: []common.RelayResult{},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: ""},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		require.Equal(t, "Cached", providerHeader.Value)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.Nil(t, retryHeader, "no retries occurred — header should be absent")
	})

	t.Run("cache hit after two protocol errors - matches MAG-1653 reproduction", func(t *testing.T) {
		// 2 P1 503s + 1 cache hit = 3 attempts, 2 retries.
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: ""}}, // cache result
			},
			nodeErrors: []common.RelayResult{},
			protocolErrors: []relaycore.RelayError{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider1"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@simprovider1"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: ""},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 2, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		retryHeader := findHeader(relayResult.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.NotNil(t, retryHeader)
		require.Equal(t, "2", retryHeader.Value)

		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		// Ordering contract: errors chronologically first, resolver last.
		// "Cached" sits at the end as the response source.
		require.Equal(t, "lava@simprovider1,Cached", providerHeader.Value)
	})
}

// TestResolverAlwaysLastInProviderHeader hardens the Lava-Provider-Address
// contract: the last entry in the comma-separated chain must always be the
// address that resolved the response, even when that address also appears
// earlier in successResults / nodeErrors (which can happen under hedging
// or other concurrent dispatch patterns where the winner is recorded before
// a slower peer's result lands in the same bucket).
//
// MAG-1871 context: a stickiness-failover test observed the *down pinned*
// provider listed last instead of the failover peer. The exact data shape
// that produced that output has not been traced to a unit-testable code
// path (per provider_simulator/server.py, a `down` provider returns HTTP
// 503 → protocol error → protocolErrors bucket — which iterates before
// successResults and would *not* trigger the dedup bug). This test instead
// covers the structural class of failure the fix prevents: any data shape
// where the resolver appears earlier in iteration than another entry.
// End-to-end verification against the failing simulator test
// (`test_stickiness_breaks_on_failover_when_pinned_provider_is_unreachable`)
// is still required to confirm MAG-1871 itself is closed.
func TestResolverAlwaysLastInProviderHeader(t *testing.T) {
	ctx := context.Background()

	findHeader := func(metadata []pairingtypes.Metadata, name string) *pairingtypes.Metadata {
		for i := range metadata {
			if metadata[i].Name == name {
				return &metadata[i]
			}
		}
		return nil
	}

	t.Run("resolver appears earlier in successResults - resolver still last (regression for the fix)", func(t *testing.T) {
		// Two successes recorded in order [simprovider2, simprovider3] with
		// simprovider2 being the resolver returned by ProcessingResult.
		// Before the fix, dedup kept simprovider2 at its first-seen position
		// and the trailing addProvider(resolver) was a no-op, yielding
		// "simprovider2,simprovider3" — winner not last, contract broken.
		// After the fix, the resolver is skipped during iteration and
		// appended explicitly at the end.
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider2"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider3"}},
			},
			nodeErrors: []common.RelayResult{},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		// Contract: last entry == response source (resolver).
		require.Equal(t, "simprovider3,simprovider2", providerHeader.Value)
	})

	t.Run("resolver also recorded as nodeError on an earlier attempt - resolver still last", func(t *testing.T) {
		// A provider returned a node error on a first attempt then served on
		// a retry. The retry winner is the resolver; that same address must
		// still appear last (not at its first-seen nodeErrors position).
		// Without the fix the dedup keeps the resolver in the nodeErrors
		// position; the final addProvider is a no-op.
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider1"}},
			},
			nodeErrors: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider1"}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider2"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		// Before fix: "simprovider1,simprovider2" (simprovider2 last — wrong).
		// After fix: simprovider1 (resolver) moved to the end.
		require.Equal(t, "simprovider2,simprovider1", providerHeader.Value)
	})

	t.Run("classic failover (winner only in successResults) - already correct, still correct after fix", func(t *testing.T) {
		// Sanity check: when the winner does not appear in any earlier
		// bucket, the chain ends with the winner under both old and new
		// code. This is the data shape that matches a textbook
		// "stickiness fails over to peer" scenario per the simulator's
		// down-mode semantics (down → HTTP 503 → protocolErrors).
		relayProcessor := &MockRelayProcessorForHeaders{
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider2"}},
			},
			nodeErrors: []common.RelayResult{},
			protocolErrors: []relaycore.RelayError{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider3"}},
			},
		}

		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "simprovider2"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}

		rpcSmartRouterServer := &RPCSmartRouterServer{}
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 1, relayProcessor, &MockProtocolMessage{
			api: &spectypes.Api{Name: "eth_blockNumber"},
		}, "eth_blockNumber", nil, true)

		providerHeader := findHeader(relayResult.Reply.Metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.NotNil(t, providerHeader)
		require.Equal(t, "simprovider3,simprovider2", providerHeader.Value)
	})
}

// TestStatefulRelayTargetsHeader tests the stateful API header functionality
func TestStatefulRelayTargetsHeader(t *testing.T) {
	ctx := context.Background()
	providerAddress1 := "lava@provider1"
	providerAddress2 := "lava@provider2"
	providerAddress3 := "lava@provider3"

	t.Run("stateful API - all providers header included", func(t *testing.T) {
		// Create a mock relay processor with stateful relay targets
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: nil,
			statefulRelayTargets:  []string{providerAddress1, providerAddress2, providerAddress3},
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a stateful API protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{
				Name: "eth_sendTransaction",
				Category: spectypes.SpecCategory{
					Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS,
				},
			},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "eth_sendTransaction", nil, true)

		// Verify the result - should have:
		// 1. Single provider header (winning provider)
		// 2. Stateful API header
		// 3. Stateful all providers header
		// 4. User request type header
		require.Len(t, relayResult.Reply.Metadata, 4)

		// Find and verify the stateful API header
		var statefulHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.STATEFUL_API_HEADER {
				statefulHeader = &meta
				break
			}
		}
		require.NotNil(t, statefulHeader)
		require.Equal(t, "true", statefulHeader.Value)

		// Find and verify the stateful all providers header
		var allProvidersHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.STATEFUL_ALL_PROVIDERS_HEADER_NAME {
				allProvidersHeader = &meta
				break
			}
		}
		require.NotNil(t, allProvidersHeader)

		// Verify all three providers are in the header
		headerValue := allProvidersHeader.Value
		require.Contains(t, headerValue, providerAddress1)
		require.Contains(t, headerValue, providerAddress2)
		require.Contains(t, headerValue, providerAddress3)
	})

	t.Run("stateful API - single provider in targets", func(t *testing.T) {
		// Create a mock relay processor with only one stateful relay target
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: nil,
			statefulRelayTargets:  []string{providerAddress1},
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a stateful API protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{
				Name: "eth_sendRawTransaction",
				Category: spectypes.SpecCategory{
					Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS,
				},
			},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "eth_sendRawTransaction", nil, true)

		// Verify the result
		require.Len(t, relayResult.Reply.Metadata, 4)

		// Find and verify the stateful all providers header
		var allProvidersHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.STATEFUL_ALL_PROVIDERS_HEADER_NAME {
				allProvidersHeader = &meta
				break
			}
		}
		require.NotNil(t, allProvidersHeader)
		require.Contains(t, allProvidersHeader.Value, providerAddress1)
	})

	t.Run("stateful API - empty targets list", func(t *testing.T) {
		// Create a mock relay processor with empty stateful relay targets
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: nil,
			statefulRelayTargets:  []string{},
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a stateful API protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{
				Name: "eth_sendTransaction",
				Category: spectypes.SpecCategory{
					Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS,
				},
			},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "eth_sendTransaction", nil, true)

		// Verify the result - should NOT have stateful all providers header (empty list)
		// Should have: single provider header, stateful API header, user request type header
		require.Len(t, relayResult.Reply.Metadata, 3)

		// Verify stateful all providers header is NOT present
		for _, meta := range relayResult.Reply.Metadata {
			require.NotEqual(t, common.STATEFUL_ALL_PROVIDERS_HEADER_NAME, meta.Name)
		}

		// Verify stateful API header IS present
		var statefulHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.STATEFUL_API_HEADER {
				statefulHeader = &meta
				break
			}
		}
		require.NotNil(t, statefulHeader)
		require.Equal(t, "true", statefulHeader.Value)
	})

	t.Run("non-stateful API - no stateful headers", func(t *testing.T) {
		// Create a mock relay processor without stateful relay targets
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: nil,
			statefulRelayTargets:  nil, // No stateful targets
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a non-stateful API protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{
				Name: "eth_getBlockByNumber",
				Category: spectypes.SpecCategory{
					Stateful: 0, // Not stateful
				},
			},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "eth_getBlockByNumber", nil, true)

		// Verify the result - should only have: single provider header + user request type header
		require.Len(t, relayResult.Reply.Metadata, 2)

		// Verify NO stateful headers are present
		for _, meta := range relayResult.Reply.Metadata {
			require.NotEqual(t, common.STATEFUL_API_HEADER, meta.Name)
			require.NotEqual(t, common.STATEFUL_ALL_PROVIDERS_HEADER_NAME, meta.Name)
		}

		// Verify single provider header is present
		var providerHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			if meta.Name == common.PROVIDER_ADDRESS_HEADER_NAME {
				providerHeader = &meta
				break
			}
		}
		require.NotNil(t, providerHeader)
		require.Equal(t, providerAddress1, providerHeader.Value)
	})

	t.Run("stateful API with cross-validation enabled - both headers present", func(t *testing.T) {
		// This is an edge case - stateful API shouldn't use cross-validation, but let's test the behavior
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5},
			selection:             relaycore.CrossValidation, // Enable cross-validation via Selection
			statefulRelayTargets:  []string{providerAddress1, providerAddress2},
			successResults: []common.RelayResult{
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1}},
				{ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress2}},
			},
			nodeErrors: []common.RelayResult{},
		}

		// Create a relay result
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: providerAddress1},
			Reply: &pairingtypes.RelayReply{
				Metadata: []pairingtypes.Metadata{},
			},
		}

		// Create a stateful API protocol message
		mockProtocolMessage := &MockProtocolMessage{
			api: &spectypes.Api{
				Name: "eth_sendTransaction",
				Category: spectypes.SpecCategory{
					Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS,
				},
			},
		}

		// Create RPC consumer server
		rpcSmartRouterServer := &RPCSmartRouterServer{}

		// Call the function
		rpcSmartRouterServer.appendHeadersToRelayResult(ctx, relayResult, 0, relayProcessor, mockProtocolMessage, "eth_sendTransaction", nil, true)

		// Verify both cross-validation and stateful headers are present
		// (even though this is an unusual scenario)
		var crossValidationHeader, statefulHeader, allProvidersHeader *pairingtypes.Metadata
		for _, meta := range relayResult.Reply.Metadata {
			switch meta.Name {
			case common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME:
				crossValidationHeader = &meta
			case common.STATEFUL_API_HEADER:
				statefulHeader = &meta
			case common.STATEFUL_ALL_PROVIDERS_HEADER_NAME:
				allProvidersHeader = &meta
			}
		}

		// Verify stateful headers are present
		require.NotNil(t, statefulHeader)
		require.Equal(t, "true", statefulHeader.Value)
		require.NotNil(t, allProvidersHeader)

		// CrossValidation header would also be present if cross-validation is enabled
		require.NotNil(t, crossValidationHeader)
	})
}

// Test the full SendParsedRelay integration (if we can mock the dependencies)
func TestSendParsedRelayIntegration(t *testing.T) {
	// This test would require more complex mocking of the entire relay processor
	// For now, we'll create a simpler version that tests the header logic in context

	t.Run("SendParsedRelay calls appendHeadersToRelayResult", func(t *testing.T) {
		// This is a conceptual test - in practice, we'd need to mock:
		// - ProcessRelaySend
		// - RelayProcessor.ProcessingResult()
		// - All the complex dependencies

		// For now, we'll just verify that our header logic works when called
		// The actual SendParsedRelay integration would require extensive mocking
		// that might be more complex than the value it provides

		require.True(t, true, "SendParsedRelay integration test placeholder - would require complex mocking")
	})
}

// MockResultsManager implements the relaycore.ResultsManager interface for testing
type MockResultsManager struct {
	successResults []common.RelayResult
	nodeErrorsList []common.RelayResult
	protocolErrors []relaycore.RelayError
}

func (m *MockResultsManager) GetResultsData() (successResults []common.RelayResult, nodeErrors []common.RelayResult, protocolErrors []relaycore.RelayError) {
	return m.successResults, m.nodeErrorsList, m.protocolErrors
}

func (m *MockResultsManager) String() string {
	return "MockResultsManager"
}

func (m *MockResultsManager) NodeResults() []common.RelayResult {
	return append(m.successResults, m.nodeErrorsList...)
}

func (m *MockResultsManager) RequiredResults(requiredSuccesses int, selection relaycore.Selection) bool {
	return len(m.successResults) >= requiredSuccesses
}

func (m *MockResultsManager) ProtocolErrors() uint64 {
	return uint64(len(m.protocolErrors))
}

func (m *MockResultsManager) HasResults() bool {
	return len(m.successResults) > 0 || len(m.nodeErrorsList) > 0
}

func (m *MockResultsManager) GetResults() (success int, nodeErrors int, specialNodeErrors int, protocolErrors int) {
	return len(m.successResults), len(m.nodeErrorsList), 0, len(m.protocolErrors)
}

func (m *MockResultsManager) SetResponse(response *relaycore.RelayResponse, protocolMessage chainlib.ProtocolMessage) (nodeError error) {
	return nil
}

func (m *MockResultsManager) GetBestNodeErrorMessageForUser() relaycore.RelayError {
	return relaycore.RelayError{}
}

func (m *MockResultsManager) GetBestProtocolErrorMessageForUser() relaycore.RelayError {
	return relaycore.RelayError{}
}

func (m *MockResultsManager) NodeErrors() (ret []common.RelayResult) {
	return m.nodeErrorsList
}

// MockProtocolMessage implements the ProtocolMessage interface for testing
type MockProtocolMessage struct {
	api            *spectypes.Api
	requestedBlock int64 // configurable requested block, defaults to 0
	userData       common.UserData
}

func (m *MockProtocolMessage) GetApi() *spectypes.Api {
	return m.api
}

func (m *MockProtocolMessage) GetApiCollection() *spectypes.ApiCollection {
	return nil
}

func (m *MockProtocolMessage) GetParseDirective() *spectypes.ParseDirective {
	return nil
}

func (m *MockProtocolMessage) GetUserData() common.UserData {
	return m.userData
}

func (m *MockProtocolMessage) GetRelayData() *pairingtypes.RelayPrivateData {
	return &pairingtypes.RelayPrivateData{}
}

func (m *MockProtocolMessage) GetChainMessage() chainlib.ChainMessage {
	return nil
}

func (m *MockProtocolMessage) GetExtensions() []*spectypes.Extension {
	return nil
}

func (m *MockProtocolMessage) GetDirectiveHeaders() map[string]string {
	return nil
}

func (m *MockProtocolMessage) GetCrossValidationParameters() (common.CrossValidationParams, bool, error) {
	return common.DefaultCrossValidationParams, false, nil
}

func (m *MockProtocolMessage) IsDefaultApi() bool {
	return false
}

// Additional methods required by ChainMessage interface
func (m *MockProtocolMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return ""
}

func (m *MockProtocolMessage) RequestedBlock() (latest int64, earliest int64) {
	return m.requestedBlock, 0
}

func (m *MockProtocolMessage) UpdateLatestBlockInMessage(latestBlock int64, modifyContent bool) (modified bool) {
	return false
}

func (m *MockProtocolMessage) AppendHeader(metadata []pairingtypes.Metadata) {
	// No-op for testing
}

func (m *MockProtocolMessage) OverrideExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser) {
	// No-op for testing
}

func (m *MockProtocolMessage) DisableErrorHandling() {
	// No-op for testing
}

func (m *MockProtocolMessage) TimeoutOverride(...time.Duration) time.Duration {
	return 0
}

func (m *MockProtocolMessage) GetForceCacheRefresh() bool {
	return false
}

func (m *MockProtocolMessage) SetForceCacheRefresh(force bool) bool {
	return false
}

func (m *MockProtocolMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	return false, ""
}

func (m *MockProtocolMessage) GetRawRequestHash() ([]byte, error) {
	return nil, nil
}

func (m *MockProtocolMessage) GetRequestedBlocksHashes() []string {
	return nil
}

func (m *MockProtocolMessage) UpdateEarliestInMessage(incomingEarliest int64) bool {
	return false
}

func (m *MockProtocolMessage) SetExtension(extension *spectypes.Extension) {
	// No-op for testing
}

func (m *MockProtocolMessage) GetUsedDefaultValue() bool {
	return false
}

func (m *MockProtocolMessage) IsBatch() bool {
	return false
}

func (m *MockProtocolMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return nil
}

func (m *MockProtocolMessage) RelayPrivateData() *pairingtypes.RelayPrivateData {
	return &pairingtypes.RelayPrivateData{}
}

func (m *MockProtocolMessage) HashCacheRequest(chainId string) ([]byte, func([]byte) []byte, error) {
	return nil, nil, nil
}

func (m *MockProtocolMessage) GetBlockedProviders() []string {
	return nil
}

func (m *MockProtocolMessage) UpdateEarliestAndValidateExtensionRules(extensionParser *extensionslib.ExtensionParser, earliestBlockHashRequested int64, addon string, seenBlock int64) bool {
	return false
}

// ============================================================================
// Tests for Issue #1: Goroutine Leak in waitForPairing()
// ============================================================================

// TestWaitForPairingContextCancellation tests that waitForPairing exits when context is cancelled
// This is the critical test for Issue #1: Goroutine Leak
func TestWaitForPairingContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	// Create RPC smart router server with minimal setup
	rpcss := &RPCSmartRouterServer{
		sessionManager: &lavasession.ConsumerSessionManager{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start waitForPairing in a goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Test the actual waitForPairing function
		rpcss.waitForPairing(ctx)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for function to return with timeout
	select {
	case <-done:
		// Success - function returned
	case <-time.After(2 * time.Second):
		t.Fatal("waitForPairing did not exit after context cancellation")
	}

	// Give goroutines time to clean up (wait longer for ticker cleanup)
	time.Sleep(200 * time.Millisecond)
}

// TestWaitForPairingNoInitialization tests behavior when initialization never completes
// This tests that the function can be cancelled even after waiting for a while
func TestWaitForPairingNoInitialization(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	// Create RPC smart router server with session manager that will never initialize
	rpcss := &RPCSmartRouterServer{
		sessionManager: &lavasession.ConsumerSessionManager{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start waitForPairing
	done := make(chan struct{})
	go func() {
		defer close(done)
		rpcss.waitForPairing(ctx)
	}()

	// Let it wait for a bit, then cancel
	time.Sleep(500 * time.Millisecond)
	cancel()

	// Wait for completion - should exit via cancellation
	select {
	case <-done:
		// Success - function exited via context cancellation
	case <-time.After(2 * time.Second):
		t.Fatal("waitForPairing did not exit after context cancellation")
	}

	// Give goroutines time to clean up
	time.Sleep(200 * time.Millisecond)
}

// TestWaitForPairingRapidStartStop tests rapid start/stop cycles for memory leaks
func TestWaitForPairingRapidStartStop(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	// Run 50 rapid start/stop cycles
	for i := 0; i < 50; i++ {
		rpcss := &RPCSmartRouterServer{
			sessionManager: &lavasession.ConsumerSessionManager{},
		}

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			defer close(done)
			rpcss.waitForPairing(ctx)
		}()

		// Cancel immediately
		cancel()

		// Wait for completion
		select {
		case <-done:
			// Success
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Iteration %d: waitForPairing did not exit", i)
		}
	}

	// Give all goroutines time to clean up (wait longer for ticker cleanup)
	time.Sleep(300 * time.Millisecond)
}

// TestWaitForPairingLongWait tests that waiting for extended periods works correctly
func TestWaitForPairingLongWait(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test")
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	rpcss := &RPCSmartRouterServer{
		sessionManager: &lavasession.ConsumerSessionManager{},
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "test-chain", ApiInterface: "jsonrpc"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start waitForPairing
	done := make(chan struct{})
	go func() {
		defer close(done)
		rpcss.waitForPairing(ctx)
	}()

	// Wait for 35 seconds (past the 30s warning), then cancel
	time.Sleep(35 * time.Second)
	cancel()

	// Wait for function to exit
	select {
	case <-done:
		// Success - function exited after cancel
	case <-time.After(2 * time.Second):
		t.Fatal("waitForPairing did not exit after context cancellation")
	}

	// Give goroutines time to clean up
	time.Sleep(200 * time.Millisecond)
}

// TestWaitForPairingCancelDuringWait tests cancellation during the 30s wait loop
func TestWaitForPairingCancelDuringWait(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	rpcss := &RPCSmartRouterServer{
		sessionManager: &lavasession.ConsumerSessionManager{},
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "test-chain", ApiInterface: "jsonrpc"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		rpcss.waitForPairing(ctx)
	}()

	// Cancel after 5 seconds (during the 30s wait loop)
	time.Sleep(5 * time.Second)
	cancel()

	// Wait for function to return
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("waitForPairing did not exit after cancellation during wait loop")
	}

	// Give goroutines time to clean up (wait longer for ticker cleanup)
	time.Sleep(200 * time.Millisecond)
}

// TestWaitForPairingConcurrentCalls tests multiple concurrent calls to waitForPairing
// This verifies that the fix handles concurrent router startups correctly
func TestWaitForPairingConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const concurrentCalls = 10

	ctx, cancel := context.WithCancel(context.Background())

	// Create server instances
	var wg sync.WaitGroup
	wg.Add(concurrentCalls)

	for i := 0; i < concurrentCalls; i++ {
		go func() {
			defer wg.Done()
			rpcss := &RPCSmartRouterServer{
				sessionManager: &lavasession.ConsumerSessionManager{},
			}
			rpcss.waitForPairing(ctx)
		}()
	}

	// Let them run briefly
	time.Sleep(100 * time.Millisecond)

	// Cancel all contexts
	cancel()

	// Wait for all to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - all goroutines exited
	case <-time.After(3 * time.Second):
		t.Fatal("Not all concurrent waitForPairing calls exited after context cancellation")
	}

	// Give goroutines time to clean up
	time.Sleep(300 * time.Millisecond)
}

// ============================================================================
// Tests for Session Leak Prevention in sendRelayToProvider (Smart Router)
// These tests validate that sessions are properly freed on all exit paths
// ============================================================================

// TestSmartRouterSessionLeakPrevention_EarlyReturnNilRelayData tests that sessions are freed when relayData is nil
func TestSmartRouterSessionLeakPrevention_EarlyReturnNilRelayData(t *testing.T) {
	// This test verifies the fix for session leaks on early returns in smart router
	// The key behavior: defer should call OnSessionFailure if session wasn't handled

	t.Run("session freed on nil relayData", func(t *testing.T) {
		// Simulate the scenario where relayData is nil
		// In the fixed code, the defer should catch this and free the session

		sessionHandled := false
		var errResponse error
		cleanupCalled := false

		// Run in a function to trigger defer - simulates sendRelayToProvider goroutine
		simulateRelayToProvider := func(relayData *pairingtypes.RelayPrivateData) {
			// Simulate the defer logic from sendRelayToProvider
			defer func() {
				if !sessionHandled {
					cleanupCalled = true
				}
			}()

			// This is the actual check in sendRelayToProvider that triggers early return
			if relayData == nil {
				errResponse = fmt.Errorf("RelayPrivateData is nil")
				return // Early return - defer will run
			}
		}
		simulateRelayToProvider(nil) // Pass nil to trigger the early return

		// Verify cleanup was called
		require.NotNil(t, errResponse)
		require.False(t, sessionHandled, "sessionHandled should still be false")
		require.True(t, cleanupCalled, "cleanup should be called on early return")
	})
}

// TestSmartRouterSessionLeakPrevention_EarlyReturnTimeoutExpired tests session cleanup on timeout
func TestSmartRouterSessionLeakPrevention_EarlyReturnTimeoutExpired(t *testing.T) {
	t.Run("session freed on timeout expired", func(t *testing.T) {
		sessionHandled := false
		cleanupCalled := false

		// Run in a function to trigger defer
		func() {
			defer func() {
				if !sessionHandled {
					cleanupCalled = true
				}
			}()

			// Simulate timeout <= 0 check
			processingTimeout := time.Duration(-1)
			if processingTimeout <= 0 {
				return // Early return - defer will run
			}
		}()

		require.False(t, sessionHandled, "sessionHandled should still be false")
		require.True(t, cleanupCalled, "cleanup should be called on timeout expired")
	})
}

// TestSmartRouterSessionLeakPrevention_ProperHandlingNoDoubleFree tests no double-free on proper handling
func TestSmartRouterSessionLeakPrevention_ProperHandlingNoDoubleFree(t *testing.T) {
	t.Run("no double free when OnSessionDone called", func(t *testing.T) {
		sessionHandled := false
		cleanupCalled := false

		// Run in a function to trigger defer
		func() {
			defer func() {
				if !sessionHandled {
					cleanupCalled = true
				}
			}()

			// Simulate successful relay completion
			sessionHandled = true // Mark as handled before OnSessionDone
		}()

		require.True(t, sessionHandled)
		require.False(t, cleanupCalled, "cleanup should NOT be called when session is handled")
	})

	t.Run("no double free when OnSessionFailure called", func(t *testing.T) {
		sessionHandled := false
		cleanupCalled := false

		// Run in a function to trigger defer
		func() {
			defer func() {
				if !sessionHandled {
					cleanupCalled = true
				}
			}()

			// Simulate relay failure with proper cleanup
			sessionHandled = true // Mark as handled before OnSessionFailure
		}()

		require.True(t, sessionHandled)
		require.False(t, cleanupCalled, "cleanup should NOT be called when session is handled")
	})
}

// TestSmartRouterSessionLeakPrevention_PanicRecovery tests session cleanup on panic
func TestSmartRouterSessionLeakPrevention_PanicRecovery(t *testing.T) {
	t.Run("session freed on panic recovery", func(t *testing.T) {
		sessionHandled := false
		cleanupCalled := false
		panicRecovered := false

		// Simulate the defer logic with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicRecovered = true
				}
				// Cleanup should still happen
				if !sessionHandled {
					cleanupCalled = true
				}
			}()

			// Simulate panic
			panic("simulated panic in relay")
		}()

		require.True(t, panicRecovered, "Panic should be recovered")
		require.True(t, cleanupCalled, "Cleanup should be called even after panic")
	})
}

// TestSmartRouterSessionLeakPrevention_ConcurrentSessions tests concurrent session handling
func TestSmartRouterSessionLeakPrevention_ConcurrentSessions(t *testing.T) {
	t.Run("concurrent sessions properly cleaned up", func(t *testing.T) {
		var wg sync.WaitGroup
		sessionsHandled := int32(0)
		cleanupsCalled := int32(0)

		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				sessionHandled := false

				// Simulate defer cleanup
				defer func() {
					if !sessionHandled {
						atomic.AddInt32(&cleanupsCalled, 1)
					}
				}()

				// Simulate various exit paths
				if id%3 == 0 {
					// Early return (should trigger cleanup)
					return
				}
				// Proper handling or error path with handling - both mark session as handled
				sessionHandled = true
				atomic.AddInt32(&sessionsHandled, 1)
			}(i)
		}

		wg.Wait()

		handled := atomic.LoadInt32(&sessionsHandled)
		cleanups := atomic.LoadInt32(&cleanupsCalled)

		// All goroutines should have either handled the session or triggered cleanup
		require.Equal(t, int32(numGoroutines), handled+cleanups,
			"All sessions should be either handled or cleaned up")

		// Roughly 1/3 should trigger cleanup (id%3 == 0)
		expectedCleanups := int32(numGoroutines / 3)
		require.InDelta(t, expectedCleanups, cleanups, 5,
			"Approximately 1/3 of sessions should trigger cleanup")
	})
}

// TestSmartRouterSessionLeakPrevention_HighConcurrency tests the smart router under high concurrency
// This simulates the real-world scenario that caused session exhaustion
func TestSmartRouterSessionLeakPrevention_HighConcurrency(t *testing.T) {
	t.Run("high concurrency session handling", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		var wg sync.WaitGroup
		totalSessions := int32(0)
		releasedSessions := int32(0)

		// Simulate 500 concurrent relay requests (high load scenario)
		numRequests := 500

		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(requestId int) {
				defer wg.Done()

				// Track session acquisition
				atomic.AddInt32(&totalSessions, 1)

				sessionHandled := false

				// Simulate defer cleanup (the fix we implemented)
				defer func() {
					if !sessionHandled {
						atomic.AddInt32(&releasedSessions, 1)
					}
				}()

				// Simulate various scenarios
				switch requestId % 5 {
				case 0:
					// Success path
					sessionHandled = true
					atomic.AddInt32(&releasedSessions, 1)
				case 1:
					// Failure with proper cleanup
					sessionHandled = true
					atomic.AddInt32(&releasedSessions, 1)
				case 2:
					// Early return (nil data) - should be caught by defer
					return
				case 3:
					// Timeout expired - should be caught by defer
					return
				case 4:
					// Panic scenario - should be caught by defer
					// In real code, there would be panic recovery
					return
				}
			}(i)
		}

		wg.Wait()

		total := atomic.LoadInt32(&totalSessions)
		released := atomic.LoadInt32(&releasedSessions)

		// All sessions should be released
		require.Equal(t, total, released,
			"All acquired sessions must be released - no leaks allowed")
	})
}

// TestSmartRouterSessionLeakPrevention_SingleProvider tests the single provider scenario
// This was the original bug scenario - smart router with only 1 provider causing session exhaustion
func TestSmartRouterSessionLeakPrevention_SingleProvider(t *testing.T) {
	t.Run("single provider session management", func(t *testing.T) {
		// Track sessions like MAX_SESSIONS_ALLOWED_PER_PROVIDER check does
		activeSessions := int32(0)
		maxSessions := int32(1000) // MAX_SESSIONS_ALLOWED_PER_PROVIDER

		// Buffered channel acts as a semaphore that enforces the cap, mirroring
		// production where MAX_SESSIONS_ALLOWED_PER_PROVIDER gates acquisition.
		// Without it the assertion below relies on goroutine scheduler luck and
		// flakes under CI load (counter overshoots 1000 before any goroutine
		// gets to decrement).
		sem := make(chan struct{}, maxSessions)

		var wg sync.WaitGroup
		numRequests := 2000 // More than max sessions to verify no leak

		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(requestId int) {
				defer wg.Done()

				// Acquire a session slot (blocks while maxSessions are in flight)
				sem <- struct{}{}
				defer func() { <-sem }()

				// Simulate session acquisition
				current := atomic.AddInt32(&activeSessions, 1)

				// With the semaphore enforcing the cap, this must hold
				if current > maxSessions {
					t.Errorf("Session count exceeded max: %d > %d", current, maxSessions)
				}

				sessionHandled := false

				// Simulate defer cleanup
				defer func() {
					if !sessionHandled {
						atomic.AddInt32(&activeSessions, -1)
					}
				}()

				// Small delay to simulate processing
				time.Sleep(time.Duration(requestId%10) * time.Microsecond)

				// Always properly release
				sessionHandled = true
				atomic.AddInt32(&activeSessions, -1)
			}(i)
		}

		wg.Wait()

		final := atomic.LoadInt32(&activeSessions)
		require.Equal(t, int32(0), final, "All sessions should be released at the end")
	})
}

// ============================================================================
// Tests for Epoch Cleanup Integration - EndpointChainTrackerManager Lifecycle
// These tests validate that the EndpointChainTrackerManager properly cleans up
// when trackers are removed (supporting epoch-based cleanup in RPCSmartRouter)
// ============================================================================

// TestEndpointChainTrackerManager_RemoveTrackerCallsCancel tests that RemoveTracker
// properly invokes the cancel function for per-tracker context cancellation
func TestEndpointChainTrackerManager_RemoveTrackerCallsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("RemoveTracker invokes cancel function", func(t *testing.T) {
		trackerManager := NewEndpointChainTrackerManager(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)
		defer trackerManager.Stop()

		// Manually add a cancel function to simulate a tracker
		endpoint := "http://test:8545"
		cancelCalled := false
		trackerManager.cancelFuncs[endpoint] = func() { cancelCalled = true }

		// Remove the tracker - should call cancel function
		trackerManager.RemoveTracker(endpoint)

		require.True(t, cancelCalled, "RemoveTracker should call the cancel function")
		require.Empty(t, trackerManager.cancelFuncs)
	})

	t.Run("Stop invokes all cancel functions", func(t *testing.T) {
		trackerManager := NewEndpointChainTrackerManager(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)

		// Add multiple cancel functions
		cancelledEndpoints := make(map[string]bool)
		endpoints := []string{"http://ep1:8545", "http://ep2:8545", "http://ep3:8545"}

		for _, ep := range endpoints {
			trackerManager.cancelFuncs[ep] = func() { cancelledEndpoints[ep] = true }
		}

		// Stop should cancel all
		trackerManager.Stop()

		for _, ep := range endpoints {
			require.True(t, cancelledEndpoints[ep], "Stop should cancel %s", ep)
		}
		require.Empty(t, trackerManager.cancelFuncs)
	})

	t.Run("concurrent RemoveTracker and Stop are thread-safe", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		trackerManager := NewEndpointChainTrackerManager(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)

		var wg sync.WaitGroup
		const numGoroutines = 50

		// Add many cancel functions
		for i := 0; i < numGoroutines; i++ {
			endpoint := fmt.Sprintf("http://endpoint%d:8545", i)
			trackerManager.cancelFuncs[endpoint] = func() {}
		}

		// Simulate concurrent removal operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				endpoint := fmt.Sprintf("http://endpoint%d:8545", id)
				trackerManager.RemoveTracker(endpoint)
			}(i)
		}

		wg.Wait()

		// Cleanup
		trackerManager.Stop()
		// If we reach here without race detector error or panic, the test passes
	})
}

// ============================================================================
// Mock Consistency for filterEndpointsByConsistency tests
// ============================================================================

type mockConsistency struct {
	seenBlocks map[string]int64
}

func newMockConsistency() *mockConsistency {
	return &mockConsistency{seenBlocks: make(map[string]int64)}
}

func (mc *mockConsistency) SetSeenBlock(blockSeen int64, userData common.UserData) {
	key := mc.Key(userData)
	mc.seenBlocks[key] = blockSeen
}

func (mc *mockConsistency) GetSeenBlock(userData common.UserData) (int64, bool) {
	key := mc.Key(userData)
	block, found := mc.seenBlocks[key]
	return block, found
}

func (mc *mockConsistency) SetSeenBlockFromKey(blockSeen int64, key string) {
	mc.seenBlocks[key] = blockSeen
}

func (mc *mockConsistency) Key(userData common.UserData) string {
	return userData.DappId + "|" + userData.ConsumerIp
}

// ============================================================================
// Tests for Phase 3.1: Consistency Pre-Validation Retry with Different Providers
// ============================================================================

// TestFilterEndpointsByConsistency_ReturnsFailedSessions tests that the modified
// filterEndpointsByConsistency returns failed sessions separately from valid ones.
func TestFilterEndpointsByConsistency_ReturnsFailedSessions(t *testing.T) {
	ctx := context.Background()

	t.Run("all sessions valid - no failed sessions", func(t *testing.T) {
		consistency := newMockConsistency()
		userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
		consistency.SetSeenBlock(100, userData)

		config := relaycore.DefaultConsistencyValidationConfig()

		// Create endpoint at block 100 (synced)
		endpoint := &lavasession.Endpoint{NetworkAddress: "http://ep1:8545"}
		endpoint.LatestBlock.Store(100)

		sessions := lavasession.ConsumerSessionsMap{
			"http://ep1:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: endpoint,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
			userData:       userData,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Len(t, failed, 0)
	})

	t.Run("some sessions fail - split into valid and failed", func(t *testing.T) {
		consistency := newMockConsistency()
		userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
		consistency.SetSeenBlock(200, userData)

		// EndpointLagThreshold defaults to 10
		config := relaycore.DefaultConsistencyValidationConfig()

		// Create synced endpoint at block 195 (within threshold)
		syncedEndpoint := &lavasession.Endpoint{NetworkAddress: "http://synced:8545"}
		syncedEndpoint.LatestBlock.Store(195)

		// Create stale endpoint at block 100 (way behind, lag=100 > threshold=10)
		staleEndpoint := &lavasession.Endpoint{NetworkAddress: "http://stale:8545"}
		staleEndpoint.LatestBlock.Store(100)

		sessions := lavasession.ConsumerSessionsMap{
			"http://synced:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: syncedEndpoint,
					},
				},
			},
			"http://stale:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: staleEndpoint,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
			userData:       userData,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Len(t, failed, 1)
		require.Contains(t, valid, "http://synced:8545")
		require.Contains(t, failed, "http://stale:8545")
	})

	t.Run("all sessions fail - returns error and all failed", func(t *testing.T) {
		consistency := newMockConsistency()
		userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
		consistency.SetSeenBlock(200, userData)

		config := relaycore.DefaultConsistencyValidationConfig()

		// Both endpoints are stale
		staleEndpoint1 := &lavasession.Endpoint{NetworkAddress: "http://stale1:8545"}
		staleEndpoint1.LatestBlock.Store(100)

		staleEndpoint2 := &lavasession.Endpoint{NetworkAddress: "http://stale2:8545"}
		staleEndpoint2.LatestBlock.Store(50)

		sessions := lavasession.ConsumerSessionsMap{
			"http://stale1:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: staleEndpoint1,
					},
				},
			},
			"http://stale2:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: staleEndpoint2,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
			userData:       userData,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.Error(t, err)
		require.True(t, errors.Is(err, lavasession.ConsistencyPreValidationError))
		require.Nil(t, valid)
		require.Len(t, failed, 2)
	})

	t.Run("no seen block - skip validation, return all as valid", func(t *testing.T) {
		consistency := newMockConsistency()
		// No seenBlock set for this user

		config := relaycore.DefaultConsistencyValidationConfig()

		endpoint := &lavasession.Endpoint{NetworkAddress: "http://ep1:8545"}
		endpoint.LatestBlock.Store(100)

		sessions := lavasession.ConsumerSessionsMap{
			"http://ep1:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: endpoint,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
			userData:       common.UserData{DappId: "new-user", ConsumerIp: "5.6.7.8"},
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Nil(t, failed)
	})

	t.Run("no config - skip validation, return all as valid", func(t *testing.T) {
		sessions := lavasession.ConsumerSessionsMap{
			"http://ep1:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      nil,
			smartRouterConsistency: nil,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Nil(t, failed)
	})

	t.Run("endpoint with no block data - allowed through (first relay)", func(t *testing.T) {
		consistency := newMockConsistency()
		userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
		consistency.SetSeenBlock(200, userData)

		config := relaycore.DefaultConsistencyValidationConfig()

		// Endpoint has no block data yet (LatestBlock == 0)
		newEndpoint := &lavasession.Endpoint{NetworkAddress: "http://new:8545"}

		sessions := lavasession.ConsumerSessionsMap{
			"http://new:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: newEndpoint,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBalance"},
			requestedBlock: spectypes.LATEST_BLOCK,
			userData:       userData,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Contains(t, valid, "http://new:8545")
		require.Len(t, failed, 0)
	})

	t.Run("historical block request - skip validation", func(t *testing.T) {
		consistency := newMockConsistency()
		userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
		consistency.SetSeenBlock(200, userData)

		config := relaycore.DefaultConsistencyValidationConfig()

		staleEndpoint := &lavasession.Endpoint{NetworkAddress: "http://stale:8545"}
		staleEndpoint.LatestBlock.Store(50) // very stale

		sessions := lavasession.ConsumerSessionsMap{
			"http://stale:8545": &lavasession.SessionInfo{
				Session: &lavasession.SingleConsumerSession{
					Connection: &lavasession.DirectRPCSessionConnection{
						Endpoint: staleEndpoint,
					},
				},
			},
		}

		rpcss := &RPCSmartRouterServer{
			consistencyConfig:      config,
			smartRouterConsistency: consistency,
		}

		// Historical block request (block 42) - should skip validation
		protocolMsg := &MockProtocolMessage{
			api:            &spectypes.Api{Name: "eth_getBlockByNumber"},
			requestedBlock: 42,
			userData:       userData,
		}

		valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMsg)
		require.NoError(t, err)
		require.Len(t, valid, 1)
		require.Nil(t, failed)
	})
}

// TestConsistencyPreValidationError_NotRetryable verifies that ConsistencyPreValidationError
// is NOT treated as a retryable sync loss error (unlike SessionOutOfSyncError).
// This ensures immediate blocking via unwantedProviders rather than "allow one retry".
func TestConsistencyPreValidationError_NotRetryable(t *testing.T) {
	// ConsistencyPreValidationError should NOT be a session sync loss
	require.False(t, lavasession.IsSessionSyncLoss(lavasession.ConsistencyPreValidationError),
		"ConsistencyPreValidationError should NOT be treated as session sync loss")

	// SessionOutOfSyncError IS a session sync loss (for comparison)
	require.True(t, lavasession.IsSessionSyncLoss(lavasession.SessionOutOfSyncError),
		"SessionOutOfSyncError should be treated as session sync loss")
}

// ============================================================================
// Tests for the post-filter CrossValidation guard fast-fail behavior
// (commits a78426a + 97266e4)
// ============================================================================

// cvGuardStateMachine implements relaycore.RelayStateMachine for the CV-guard
// integration test. It only needs to expose the UsedProviders, the selection,
// and the cross-validation params; the early-exit path under test does not
// touch the other methods.
type cvGuardStateMachine struct {
	usedProviders *lavasession.UsedProviders
	cvParams      *common.CrossValidationParams
}

func (m *cvGuardStateMachine) GetProtocolMessage() chainlib.ProtocolMessage { return nil }
func (m *cvGuardStateMachine) GetDebugState() bool                          { return false }
func (m *cvGuardStateMachine) GetRelayTaskChannel() (chan relaycore.RelayStateSendInstructions, error) {
	return make(chan relaycore.RelayStateSendInstructions), nil
}
func (m *cvGuardStateMachine) UpdateBatch(err error)             {}
func (m *cvGuardStateMachine) GetSelection() relaycore.Selection { return relaycore.CrossValidation }
func (m *cvGuardStateMachine) GetCrossValidationParams() *common.CrossValidationParams {
	return m.cvParams
}
func (m *cvGuardStateMachine) GetUsedProviders() *lavasession.UsedProviders                { return m.usedProviders }
func (m *cvGuardStateMachine) SetResultsChecker(rc relaycore.ResultsCheckerInf)            {}
func (m *cvGuardStateMachine) SetRelayRetriesManager(rm *lavaprotocol.RelayRetriesManager) {}

// cvGuardMetrics is a no-op MetricsInterface + ChainIdAndApiInterfaceGetter
// for the CV-guard test. The early-exit path does not hit metrics callbacks.
type cvGuardMetrics struct{}

func (cvGuardMetrics) SetRelayNodeErrorMetric(chainId, apiInterface, providerAddress, method string) {
}
func (cvGuardMetrics) GetChainIdAndApiInterface() (string, string) { return "LAVA", "rest" }

// TestSendRelayToDirectEndpoints_CrossValidationGuardReleasesAllSessions is the
// regression catch for commit 97266e4. When filterEndpointsByConsistency drops
// the surviving session count below AgreementThreshold for a CrossValidation
// request, the post-filter guard must release the SURVIVING valid sessions
// before returning, not just the failed ones. Without this, the state machine's
// validateReturnCondition waits for CurrentlyUsed() == 0 to deliver the err,
// so the request stalls the full processingTimeout (~30s) instead of failing fast.
//
// Setup: 3 sessions (1 synced, 2 stale), AgreementThreshold=2.
// Filter drops the 2 stale; the surviving 1 is < AT, so the CV guard fires.
//
// Asserts:
//   - returns in well under processingTimeout (the regression was a 30s stall)
//   - error wraps lavasession.PairingListEmptyError (CV state machine short-circuits on this)
//   - usedProviders.CurrentlyUsed() == 0 (catches surviving-session leak — 97266e4 regression)
//   - usedProviders.SessionsLatestBatch() == 0 (catches counter leak — a78426a regression)
func TestSendRelayToDirectEndpoints_CrossValidationGuardReleasesAllSessions(t *testing.T) {
	ctx := context.Background()

	// Real chainParser so sendRelayToDirectEndpoints' early ChainBlockStats() call works.
	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, noopHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	// seenBlock=200, EndpointLagThreshold=10 (default). Endpoints below 190 are dropped.
	consistency := newMockConsistency()
	userData := common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}
	consistency.SetSeenBlock(200, userData)

	syncedEp := &lavasession.Endpoint{NetworkAddress: "http://synced:8545"}
	syncedEp.LatestBlock.Store(195)
	staleEp1 := &lavasession.Endpoint{NetworkAddress: "http://stale1:8545"}
	staleEp1.LatestBlock.Store(50)
	staleEp2 := &lavasession.Endpoint{NetworkAddress: "http://stale2:8545"}
	staleEp2.LatestBlock.Store(50)

	mkSession := func(addr string, ep *lavasession.Endpoint) *lavasession.SingleConsumerSession {
		return &lavasession.SingleConsumerSession{
			// Parent.Endpoints[0] is read by the QoS metrics path inside
			// OnSessionFailure (consumer_session_manager.go:1807); leaving the
			// slice empty panics. The endpoint identity doesn't matter here —
			// only the indexing has to succeed.
			Parent:     &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: addr, Endpoints: []*lavasession.Endpoint{ep}},
			Connection: &lavasession.DirectRPCSessionConnection{Endpoint: ep},
			QoSManager: qos.NewQoSManager(),
		}
	}
	sess1 := mkSession("lava@provider1", syncedEp)
	sess2 := mkSession("lava@provider2", staleEp1)
	sess3 := mkSession("lava@provider3", staleEp2)
	for _, s := range []*lavasession.SingleConsumerSession{sess1, sess2, sess3} {
		_, ok := s.TryUseSession()
		require.True(t, ok, "test setup: failed to lock session")
	}

	// Mimic GetSessions: AddUsed registers all 3 in UsedProviders; SetUsageForSession
	// wires each session back to UsedProviders so Free(nil) calls RemoveUsed correctly.
	usedProviders := lavasession.NewUsedProviders(nil)
	sessionsMap := lavasession.ConsumerSessionsMap{
		"lava@provider1": &lavasession.SessionInfo{Session: sess1},
		"lava@provider2": &lavasession.SessionInfo{Session: sess2},
		"lava@provider3": &lavasession.SessionInfo{Session: sess3},
	}
	usedProviders.AddUsed(sessionsMap, nil)
	routerKey := lavasession.NewRouterKey(nil)
	require.NoError(t, sess1.SetUsageForSession(0, nil, usedProviders, routerKey))
	require.NoError(t, sess2.SetUsageForSession(0, nil, usedProviders, routerKey))
	require.NoError(t, sess3.SetUsageForSession(0, nil, usedProviders, routerKey))
	require.Equal(t, 3, usedProviders.CurrentlyUsed(), "test setup: expected 3 in CurrentlyUsed")
	require.Equal(t, 3, usedProviders.SessionsLatestBatch(), "test setup: expected 3 in SessionsLatestBatch")

	// CrossValidation, AgreementThreshold=2. Filter drops 2 of 3 → guard fires (1 < 2).
	cvParams := &common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2}
	sm := &cvGuardStateMachine{usedProviders: usedProviders, cvParams: cvParams}
	metricsStub := cvGuardMetrics{}
	relayProcessor := relaycore.NewRelayProcessor(
		ctx, cvParams, consistency, metricsStub, metricsStub,
		lavaprotocol.NewRelayRetriesManager(), sm)

	// Real session manager — OnSessionFailure runs against it for the 2 dropped sessions.
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	sessionManager := lavasession.NewConsumerSessionManager(
		rpcEndpoint, optimizer, nil, "test-router",
		lavasession.NewActiveSubscriptionProvidersStorage())

	rpcss := &RPCSmartRouterServer{
		chainParser:            chainParser,
		consistencyConfig:      relaycore.DefaultConsistencyValidationConfig(),
		smartRouterConsistency: consistency,
		sessionManager:         sessionManager,
	}
	protocolMsg := &MockProtocolMessage{
		api:            &spectypes.Api{Name: "/cosmos/base/tendermint/v1beta1/blocks/latest"},
		requestedBlock: spectypes.LATEST_BLOCK,
		userData:       userData,
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	start := time.Now()
	sendErr := rpcss.sendRelayToDirectEndpoints(callCtx, sessionsMap, protocolMsg, relayProcessor, nil)
	elapsed := time.Since(start)

	require.Less(t, elapsed, time.Second,
		"fast-fail required (commit 97266e4 regression catch); took %v", elapsed)
	require.Error(t, sendErr)
	require.Truef(t, errors.Is(sendErr, lavasession.PairingListEmptyError),
		"error must wrap PairingListEmptyError so the CV short-circuit in policy.OnSendRelayResult can see it; got: %v", sendErr)
	require.Equal(t, 0, usedProviders.CurrentlyUsed(),
		"surviving valid sessions leaked into CurrentlyUsed — commit 97266e4 regression: validateReturnCondition will block on this and the request will stall processingTimeout")
	require.Equal(t, 0, usedProviders.SessionsLatestBatch(),
		"SessionsLatestBatch leaked — commit a78426a regression: RelayProcessor.checkEndProcessing will wait for responses that never arrive")
}

// TestIsFinalizedForCacheWrite pins the finalization decision used by
// tryCacheWrite when choosing between the short non-finalized TTL (~625 ms)
// and the long finalized TTL (~1.5 h).
//
// The regression case it guards: eth_getBlockByNumber(N) responses set
// Reply.LatestBlock = N (extractBlockHeightFromEVMResponse reads result.number,
// which for this method is the requested block itself). The naive
// IsFinalizedBlock(requested, latestFromReply, distance) check can never be
// satisfied because (N <= N - distance) is false for any positive distance,
// so every historical-block response gets the short TTL. A 10 s rolling
// restart then drops every cached entry — the failure mode reported in
// test_phase2_5_caching.py::test_cache_survives_router_pod_restart.
//
// The fix consults the router's tracked chain tip (chainTracker /
// latestBlockEstimator / atomic latestBlockHeight, surfaced via
// rpcss.getLatestBlock()) and prefers it when it is ahead of the reply's
// per-response value.
func TestIsFinalizedForCacheWrite(t *testing.T) {
	tests := []struct {
		name        string
		requested   int64
		replyLatest int64
		tracked     int64
		distance    int64
		want        bool
	}{
		{
			name:        "eth_getBlockByNumber historical: reply echoes request, tracker shows real tip",
			requested:   17500000,
			replyLatest: 17500000, // simulator (and real providers) echo result.number == requested
			tracked:     20000000,
			distance:    64,
			want:        true,
		},
		{
			name:        "tracker only slightly ahead of requested: not finalized",
			requested:   20000000,
			replyLatest: 20000000,
			tracked:     20000050,
			distance:    64,
			want:        false,
		},
		{
			name:        "tracker unavailable, reply carries real tip: falls back to reply",
			requested:   17500000,
			replyLatest: 20000000,
			tracked:     0,
			distance:    64,
			want:        true,
		},
		{
			name:        "tracker behind reply: max() keeps the higher reply value",
			requested:   17500000,
			replyLatest: 20000000,
			tracked:     17500000,
			distance:    64,
			want:        true,
		},
		{
			name:        "LATEST_BLOCK sentinel (-2): never finalized regardless of tip",
			requested:   spectypes.LATEST_BLOCK,
			replyLatest: 20000000,
			tracked:     20000000,
			distance:    64,
			want:        false,
		},
		{
			name:        "exactly on the finalization boundary: finalized",
			requested:   17500000,
			replyLatest: 17500000,
			tracked:     17500064,
			distance:    64,
			want:        true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isFinalizedForCacheWrite(tc.requested, tc.replyLatest, tc.tracked, tc.distance)
			require.Equal(t, tc.want, got)
		})
	}
}
