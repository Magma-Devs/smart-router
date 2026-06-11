package relaycore

import (
	"context"
	"crypto/sha256"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestResponsesCrossValidation_GroupDiversity covers the Phase 1.2c diversity gate: a quorum that meets
// the count threshold but spans too few distinct groups is a failure, not a success. The empty group
// label folds into "default", and MinGroups <= 1 disables the gate (pre-1.2 behavior).
func TestResponsesCrossValidation_GroupDiversity(t *testing.T) {
	mkResult := func(data, group string) common.RelayResult {
		return common.RelayResult{
			Reply:        &pairingtypes.RelayReply{Data: []byte(data)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: "p-" + group, ProviderGroup: group},
		}
	}

	cases := []struct {
		name      string
		minGroups int
		threshold int
		results   []common.RelayResult
		wantOK    bool
	}{
		{
			name: "count met and diversity met -> success", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g2")},
			wantOK:  true,
		},
		{
			// The case that proves the gate does something the old code didn't: enough matching
			// responses, but all from one group.
			name: "count met but all one group -> diversity-unmet failure", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g1")},
			wantOK:  false,
		},
		{
			name: "empty labels fold to a single default group -> fail", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", ""), mkResult("A", "")},
			wantOK:  false,
		},
		{
			name: "min-groups 1 disables the gate -> count alone suffices", minGroups: 1, threshold: 2,
			results: []common.RelayResult{mkResult("A", ""), mkResult("A", "")},
			wantOK:  true,
		},
		{
			name: "three groups, need two -> success", minGroups: 2, threshold: 3,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g2"), mkResult("A", "g3")},
			wantOK:  true,
		},
		{
			// P1: a larger single-group hash (A:3) must NOT shadow a smaller diverse hash (B:2 across
			// 2 groups). The diverse quorum B is valid and must be chosen.
			name: "diverse lower-count quorum chosen over larger single-group", minGroups: 2, threshold: 2,
			results: []common.RelayResult{
				mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g1"),
				mkResult("B", "g2"), mkResult("B", "g3"),
			},
			wantOK: true,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rp := &RelayProcessor{
				crossValidationParams: &common.CrossValidationParams{AgreementThreshold: tc.threshold, MaxParticipants: 5, MinGroups: tc.minGroups},
				selection:             CrossValidation,
			}
			result, reason, err := rp.responsesCrossValidation(tc.results, tc.threshold)
			if tc.wantOK {
				require.NoError(t, err, "tc #%d, i #%d", i, i)
				require.NotNil(t, result, "tc #%d, i #%d", i, i)
				require.Empty(t, reason, "tc #%d, i #%d", i, i)
			} else {
				require.Error(t, err, "tc #%d, i #%d", i, i)
				require.Equal(t, common.CrossValidationReasonDiversityUnmet, reason, "tc #%d, i #%d", i, i)
			}
		})
	}
}

// TestRelayProcessor_CrossValidationOutlierRealPath drives a real RelayProcessor (Finding 1): two
// agreeing successful responses plus one successful divergent response. It proves the stored success
// results and the returned consensus carry real (non-zero) SHA256 hashes, so the divergent response is
// detected as an outlier instead of every hash collapsing to zero and looking like agreement.
func TestRelayProcessor_CrossValidationOutlierRealPath(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")
	usedProviders := lavasession.NewUsedProviders(nil)
	sm := newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, CrossValidation) // threshold 2, minGroups 1
	rp := NewRelayProcessor(ctx, sm.crossValidationParams, NewConsistency("LAVA"), RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, sm)

	mkResp := func(provider, group, data string) *RelayResponse {
		return &RelayResponse{
			RelayResult: common.RelayResult{
				Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
				Reply:        &pairingtypes.RelayReply{Data: []byte(data), LatestBlock: 1},
				ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: group},
				StatusCode:   200,
			},
		}
	}
	// Drive handleResponse directly: deterministic, avoids the CV early-exit race in WaitForResults.
	rp.handleResponse(mkResp("p1", "g1", "A"))
	rp.handleResponse(mkResp("p2", "g2", "A"))
	rp.handleResponse(mkResp("p3", "g3", "B")) // divergent successful outlier

	result, err := rp.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, "A", string(result.Reply.Data), "consensus is the agreed-on A")
	require.Equal(t, 2, result.CrossValidation)
	wantHash := sha256.Sum256([]byte("A"))
	require.Equal(t, wantHash, result.ResponseHash, "consensus result must carry the consensus hash, not zero")

	successResults, _, _ := rp.GetResultsData()
	require.Len(t, successResults, 3)
	outlierGroups := map[string]struct{}{}
	for _, r := range successResults {
		require.NotEqual(t, [32]byte{}, r.ResponseHash, "stored success result must carry a real hash, not zero")
		if r.ResponseHash != wantHash {
			outlierGroups[r.ProviderInfo.ProviderGroup] = struct{}{}
		}
	}
	require.Equal(t, map[string]struct{}{"g3": {}}, outlierGroups, "only the divergent g3 is an outlier; hashes do not collapse to agreement")
}

// TestResponsesCrossValidation_PerGroupQuorum covers the 2.3 per-group quorum rule: each of MinGroups
// groups must independently reach AgreementThreshold matching responses for the SAME hash. It is strictly
// stronger than the MinGroups diversity gate (which only needs threshold total across the groups).
func TestResponsesCrossValidation_PerGroupQuorum(t *testing.T) {
	mkResult := func(data, group string) common.RelayResult {
		return common.RelayResult{
			Reply:        &pairingtypes.RelayReply{Data: []byte(data)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: "p-" + group + "-" + data, ProviderGroup: group},
		}
	}
	cases := []struct {
		name       string
		minGroups  int
		threshold  int
		results    []common.RelayResult
		wantOK     bool
		wantData   string
		wantReason string
	}{
		{
			name: "each group reaches internal quorum and winners agree -> success", minGroups: 2, threshold: 2,
			results:  []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g2"), mkResult("A", "g2")},
			wantOK:   true,
			wantData: "A",
		},
		{
			// g2 has only one matching response (< threshold) -> only one group corroborates -> fail.
			name: "a group cannot reach its internal quorum -> group-quorum-unmet", minGroups: 2, threshold: 2,
			results:    []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g2")},
			wantOK:     false,
			wantReason: common.CrossValidationReasonGroupQuorumUnmet,
		},
		{
			// Each group reaches its own internal quorum but on a DIFFERENT hash -> winners disagree -> fail.
			name: "per-group winners disagree across groups -> group-quorum-unmet", minGroups: 2, threshold: 2,
			results:    []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("B", "g2"), mkResult("B", "g2")},
			wantOK:     false,
			wantReason: common.CrossValidationReasonGroupQuorumUnmet,
		},
		{
			name: "three groups corroborate, need two -> success", minGroups: 2, threshold: 2,
			results:  []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g2"), mkResult("A", "g2"), mkResult("B", "g3"), mkResult("B", "g3")},
			wantOK:   true,
			wantData: "A",
		},
		{
			// MinGroups diversity (1.2) would PASS this (2 matching A across 2 groups), but per-group quorum
			// must FAIL it: neither group reached threshold=2 internally.
			name: "diversity would pass but per-group fails (one-each)", minGroups: 2, threshold: 2,
			results:    []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g2")},
			wantOK:     false,
			wantReason: common.CrossValidationReasonGroupQuorumUnmet,
		},
		{
			// Nil/empty replies are not an independent corroboration of a value -> no per-group winner.
			name: "all-nil groups do not corroborate -> group-quorum-unmet", minGroups: 2, threshold: 2,
			results:    []common.RelayResult{mkResult("", "g1"), mkResult("", "g1"), mkResult("", "g2"), mkResult("", "g2")},
			wantOK:     false,
			wantReason: common.CrossValidationReasonGroupQuorumUnmet,
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rp := &RelayProcessor{
				crossValidationParams: &common.CrossValidationParams{AgreementThreshold: tc.threshold, MaxParticipants: 6, MinGroups: tc.minGroups, PerGroupQuorum: true},
				selection:             CrossValidation,
			}
			result, reason, err := rp.responsesCrossValidation(tc.results, tc.threshold)
			if tc.wantOK {
				require.NoError(t, err, "tc #%d", i)
				require.NotNil(t, result, "tc #%d", i)
				require.Empty(t, reason, "tc #%d", i)
				require.Equal(t, tc.wantData, string(result.Reply.Data), "tc #%d", i)
			} else {
				require.Error(t, err, "tc #%d", i)
				require.Equal(t, tc.wantReason, reason, "tc #%d", i)
			}
		})
	}
}

// TestRelayProcessor_CrossValidationFailureRealPath is the failure-path counterpart to the outlier real
// path: it drives a real RelayProcessor to a quorum FAILURE (two successful responses with different data,
// so no hash reaches the threshold) and proves ProcessingResult returns a NON-NIL minimal result carrying
// the failure reason alongside the error. This is the link the client-facing headers depend on — if a
// refactor returned nil here, appendHeadersToRelayResult would have nothing to write and the
// failure-reason / disagreeing-providers headers would silently die on the exact path they exist for.
func TestRelayProcessor_CrossValidationFailureRealPath(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")
	usedProviders := lavasession.NewUsedProviders(nil)
	sm := newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, CrossValidation) // threshold 2, minGroups 1
	rp := NewRelayProcessor(ctx, sm.crossValidationParams, NewConsistency("LAVA"), RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, sm)

	mkResp := func(provider, group, data string) *RelayResponse {
		return &RelayResponse{
			RelayResult: common.RelayResult{
				Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
				Reply:        &pairingtypes.RelayReply{Data: []byte(data), LatestBlock: 1},
				ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: group},
				StatusCode:   200,
			},
		}
	}
	// Two successful responses that disagree -> no hash reaches the threshold of 2 -> no-agreement.
	rp.handleResponse(mkResp("p1", "g1", "A"))
	rp.handleResponse(mkResp("p2", "g2", "B"))

	result, err := rp.ProcessingResult()
	require.Error(t, err, "a quorum failure must surface an error")
	require.NotNil(t, result, "failure must still return a minimal result so headers can be attached")
	require.Equal(t, common.CrossValidationReasonNoAgreement, result.CrossValidationFailureReason)
	require.Equal(t, http.StatusInternalServerError, result.StatusCode)

	// Both successful-but-disagreeing providers are available to the header path as dissenters.
	successResults, _, _ := rp.GetResultsData()
	require.Len(t, successResults, 2)
}

// TestResponsesCrossValidation_FailureReasons covers the distinguishable failure reasons surfaced to the
// client: no-agreement (count threshold never reached) vs diversity-unmet (count met but too few groups).
func TestResponsesCrossValidation_FailureReasons(t *testing.T) {
	mkResult := func(data, group string) common.RelayResult {
		return common.RelayResult{
			Reply:        &pairingtypes.RelayReply{Data: []byte(data)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: "p-" + group, ProviderGroup: group},
		}
	}

	t.Run("no hash reaches threshold -> no-agreement", func(t *testing.T) {
		rp := &RelayProcessor{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 3, MaxParticipants: 5, MinGroups: 2},
			selection:             CrossValidation,
		}
		_, reason, err := rp.responsesCrossValidation([]common.RelayResult{mkResult("A", "g1"), mkResult("B", "g2")}, 3)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonNoAgreement, reason)
	})

	t.Run("count met but one group -> diversity-unmet", func(t *testing.T) {
		rp := &RelayProcessor{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 2},
			selection:             CrossValidation,
		}
		_, reason, err := rp.responsesCrossValidation([]common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1")}, 2)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonDiversityUnmet, reason)
	})

	t.Run("plentiful nil replies that cannot span groups -> no-agreement, not diversity-unmet", func(t *testing.T) {
		// Regression: nil replies must not inflate the count that drives the no-agreement/diversity-unmet
		// split. No REAL hash reaches the threshold (one real response, count 1), and the nil replies all
		// sit in one group so they cannot form a diverse nil-reply quorum. Pre-fix, nilReplies inflated
		// maxCount >= threshold and the failure was mislabelled diversity-unmet (implying a quorum agreed).
		mkNil := func(group string) common.RelayResult {
			return common.RelayResult{
				Reply:        &pairingtypes.RelayReply{Data: []byte{}},
				ProviderInfo: common.ProviderInfo{ProviderAddress: "nil-" + group, ProviderGroup: group},
			}
		}
		rp := &RelayProcessor{
			crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 2},
			selection:             CrossValidation,
		}
		results := []common.RelayResult{mkResult("A", "g1"), mkNil("g1"), mkNil("g1")}
		_, reason, err := rp.responsesCrossValidation(results, 2)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonNoAgreement, reason)
	})
}

// TestCrossValidationQuorumReached_Diversity covers the Phase 1.2b diversity-aware early-exit predicate:
// it must NOT report quorum on count alone when the matching responses span too few groups, so processing
// continues until a later same-hash response from a new group arrives.
func TestCrossValidationQuorumReached_Diversity(t *testing.T) {
	hash := [32]byte{0x01}
	rp := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 2},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			hash: {count: 2, groupCounts: map[string]int{"g1": 2}}, // count met, only one group
		},
	}
	require.False(t, rp.crossValidationQuorumReached(), "count met but diversity unmet must NOT early-exit")

	// A later response with the same hash from a new group satisfies diversity.
	rp.quorumMap[hash].count++
	rp.quorumMap[hash].groupCounts["g2"]++
	require.True(t, rp.crossValidationQuorumReached(), "count + diversity met must early-exit")

	// With MinGroups <= 1 the predicate reduces to count alone (pre-1.2 behavior).
	rpNoGroups := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 1},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			hash: {count: 2, groupCounts: map[string]int{"g1": 2}},
		},
	}
	require.True(t, rpNoGroups.crossValidationQuorumReached(), "MinGroups<=1 must reach quorum on count alone")
}

// TestCrossValidationQuorumReached_PerGroupIgnoresNilReplies covers the nil-reply early-exit rule: empty/nil
// successful replies accumulate under the zero hash, but a nil/empty consensus is a FALLBACK that
// responsesCrossValidation accepts only when no real hash formed a quorum. The early-exit must therefore
// ignore the zero-hash bucket in ALL modes — committing to the nil fallback before real responses (still in
// flight) could form a preferred real quorum would be premature. The nil fallback is resolved at final eval.
func TestCrossValidationQuorumReached_PerGroupIgnoresNilReplies(t *testing.T) {
	zero := [32]byte{}     // nil/empty replies bucket here
	real := [32]byte{0xAA} // a real response hash

	// Per-group: a zero-hash bucket that WOULD satisfy per-group (2 groups, 2 each) must NOT early-exit.
	rp := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 6, MinGroups: 2, PerGroupQuorum: true},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			zero: {count: 4, groupCounts: map[string]int{"g1": 2, "g2": 2}},
		},
	}
	require.False(t, rp.crossValidationQuorumReached(), "nil/empty replies must not satisfy per-group early-exit")

	// A later run of real replies forming a genuine per-group quorum DOES early-exit.
	rp.quorumMap[real] = &quorumStat{count: 4, groupCounts: map[string]int{"g1": 2, "g2": 2}}
	require.True(t, rp.crossValidationQuorumReached(), "real per-group quorum must early-exit even with nil replies present")

	// Default (non-per-group) mode now ALSO ignores the zero-hash bucket: a nil/empty consensus is a
	// fallback resolved at final eval, so the early-exit must not commit to it before a preferred real
	// quorum could form. A zero bucket that would otherwise "reach" the count+diversity rule does NOT exit.
	rpDefault := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 6, MinGroups: 2},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			zero: {count: 4, groupCounts: map[string]int{"g1": 2, "g2": 2}},
		},
	}
	require.False(t, rpDefault.crossValidationQuorumReached(), "default mode must NOT early-exit on the nil-reply fallback bucket")

	// But a real-hash diverse quorum in default mode DOES early-exit — the fix changes only nil handling.
	rpDefault.quorumMap[real] = &quorumStat{count: 2, groupCounts: map[string]int{"g1": 1, "g2": 1}}
	require.True(t, rpDefault.crossValidationQuorumReached(), "default mode must early-exit on a real diverse quorum")
}

// TestRelayProcessor_PerGroupNilReplyRealPath drives a real RelayProcessor through handleResponse: empty
// successful replies arrive first (accumulating under the zero hash), then real replies form a per-group
// quorum. It proves the live path does not prematurely report group-quorum-unmet on the nil replies and
// still reaches consensus on the real data.
func TestRelayProcessor_PerGroupNilReplyRealPath(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")
	usedProviders := lavasession.NewUsedProviders(nil)
	sm := &mockRelayStateMachine{
		protocolMessage:       protocolMessage,
		usedProviders:         usedProviders,
		selection:             CrossValidation,
		crossValidationParams: &common.CrossValidationParams{MaxParticipants: 8, AgreementThreshold: 2, MinGroups: 2, PerGroupQuorum: true},
	}
	rp := NewRelayProcessor(ctx, sm.crossValidationParams, NewConsistency("LAVA"), RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, sm)

	mkResp := func(provider, group, data string) *RelayResponse {
		return &RelayResponse{
			RelayResult: common.RelayResult{
				Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
				Reply:        &pairingtypes.RelayReply{Data: []byte(data), LatestBlock: 1},
				ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: group},
				StatusCode:   200,
			},
		}
	}

	// Empty successful replies first (4, spanning two groups, 2 each) — would satisfy per-group IF counted.
	rp.handleResponse(mkResp("e1", "g1", ""))
	rp.handleResponse(mkResp("e2", "g1", ""))
	rp.handleResponse(mkResp("e3", "g2", ""))
	rp.handleResponse(mkResp("e4", "g2", ""))
	rp.lock.RLock()
	reachedOnNil := rp.crossValidationQuorumReached()
	rp.lock.RUnlock()
	require.False(t, reachedOnNil, "nil replies must not trip the per-group early-exit")

	// Real replies forming a genuine per-group quorum on "A".
	rp.handleResponse(mkResp("p1", "g1", "A"))
	rp.handleResponse(mkResp("p2", "g1", "A"))
	rp.handleResponse(mkResp("p3", "g2", "A"))
	rp.handleResponse(mkResp("p4", "g2", "A"))

	result, err := rp.ProcessingResult()
	require.NoError(t, err, "real per-group quorum must succeed despite earlier nil replies")
	require.Equal(t, "A", string(result.Reply.Data))
}
