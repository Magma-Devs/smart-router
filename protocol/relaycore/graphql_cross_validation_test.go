package relaycore

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestGraphQLCrossValidationBuckets pins how GraphQL answers vote under cross-validation (MAG-2863).
// A GraphQL node answers almost everything inside an HTTP 200, so two things decide a vote: the
// interface's own CheckResponseError decides whether a reply votes at all, and the canonical hash
// of the whole body decides which answer it votes for. The message comes from the real GraphQL
// parser, so the vote rule is the one a live relay meets.
//
// The bodies are Sui's. The out-of-range body is the one mainnet returned on 2026-10-11 (Foundation
// and Triton alike) and again on 2026-10-13; the not-indexed body was read from mainnet on
// 2026-10-13; the pruned body is derived from the server's source, because no public operator
// prunes checkpoints (see the constants). Each case's replies arrive in the order listed. The
// processor stops reading at quorum, so a reply the tally must include comes before the reply that
// completes the quorum.
func TestGraphQLCrossValidationBuckets(t *testing.T) {
	ctx := context.Background()
	emptyBlockParsing := spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{Name: "checkpoint", Enabled: true, ComputeUnits: 20, Category: spectypes.SpecCategory{Deterministic: true}, BlockParsing: emptyBlockParsing},
				{Name: "address", Enabled: true, ComputeUnits: 20, Category: spectypes.SpecCategory{Deterministic: true}, BlockParsing: emptyBlockParsing},
			},
		}},
	})

	const (
		checkpointRead = `{"query":"{ checkpoint(sequenceNumber: 1000) { sequenceNumber digest } }"}`
		balanceRead    = `{"query":"{ address(address: \"0x0000000000000000000000000000000000000000000000000000000000000005\", atCheckpoint: 250000000) { balances(first: 1) { nodes { totalBalance } } } }"}`

		checkpoint          = `{"data":{"checkpoint":{"sequenceNumber":1000,"digest":"BGbVv2N"}}}`
		checkpointReordered = `{"data": {"checkpoint": {"digest": "BGbVv2N", "sequenceNumber": 1000}}}`
		otherDigest         = `{"data":{"checkpoint":{"sequenceNumber":1000,"digest":"9yQ3rTf"}}}`
		// A checkpoint past the provider's own head is a bare null: the resolver returns nothing for a
		// sequence number above the head it serves (graphql.mainnet.sui.io, 2026-10-13).
		notIndexed = `{"data":{"checkpoint":null}}`
		// A checkpoint the provider has pruned keeps its object and loses its digest: the resolver
		// builds the checkpoint for any sequence number up to its head without a store lookup, and only
		// the contents lookup misses (sui-indexer-alt-graphql, src/api/types/checkpoint/mod.rs,
		// with_sequence_number and digest; sui-indexer-alt-reader maps the store's NotFound to None).
		// Derived from the source: no public operator prunes checkpoints, so it cannot be observed.
		pruned     = `{"data":{"checkpoint":{"sequenceNumber":1000,"digest":null}}}`
		timedOut   = `{"data":null,"errors":[{"message":"Request timed out","extensions":{"code":"REQUEST_TIMEOUT"}}]}`
		outOfRange = `{"data":{"address":{"balances":null}},"errors":[{"message":"Request is outside consistent range","locations":[{"line":1,"column":117}],"path":["address","balances"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
	)

	tests := []struct {
		name           string
		request        string   // checkpointRead when empty
		replies        []string // one per provider, in arrival order
		threshold      int
		wantVotes      []int  // the size of each answer's bucket, largest first
		wantNodeErrors int    // replies filed as node errors, which never vote
		wantAgreed     string // the body the caller gets; empty when cross-validation fails
		wantReason     string // the failure reason when it does
	}{
		{
			name:       "the same answer with its keys in another order is one answer",
			replies:    []string{checkpoint, checkpointReordered},
			threshold:  2,
			wantVotes:  []int{2},
			wantAgreed: checkpoint,
		},
		{
			name:       "a different digest is a different answer",
			replies:    []string{checkpoint, otherDigest},
			threshold:  2,
			wantVotes:  []int{1, 1},
			wantReason: common.CrossValidationReasonNoAgreement,
		},
		{
			// A null with no errors is an answer, not a node error, so it votes against the data.
			name:       "a provider that has not indexed the checkpoint yet disagrees with one that has it",
			replies:    []string{notIndexed, checkpoint},
			threshold:  2,
			wantVotes:  []int{1, 1},
			wantReason: common.CrossValidationReasonNoAgreement,
		},
		{
			name:       "the data outvotes one pruned provider",
			replies:    []string{pruned, checkpoint, checkpoint},
			threshold:  2,
			wantVotes:  []int{2, 1},
			wantAgreed: checkpoint,
		},
		{
			// Current behavior, not a goal. The router does not read the document, so a pruned
			// provider's null digest is an answer like any other, and two of them outvote the one that
			// still holds the data. Until MAG-2863's retention-window slice decides otherwise, every
			// provider in a cross-validation group has to keep the same history.
			name:       "a pruned majority outvotes the data (current behavior, not a goal)",
			replies:    []string{checkpoint, pruned, pruned},
			threshold:  2,
			wantVotes:  []int{2, 1},
			wantAgreed: pruned,
		},
		{
			// Current behavior, not a goal. Sui files "outside consistent range" under BAD_USER_INPUT,
			// so the router reads it as the caller's answer: it votes, and two of them are the agreed
			// reply. Retention is per operator (about an hour for balances), so an honest provider that
			// still holds the data is outvoted by two that no longer do. MAG-2863 keeps retention-window
			// handling as an open slice, and this row changes with it.
			name:       "an error that is the caller's votes like any answer (current behavior, not a goal)",
			request:    balanceRead,
			replies:    []string{outOfRange, outOfRange},
			threshold:  2,
			wantVotes:  []int{2},
			wantAgreed: outOfRange,
		},
		{
			// A server-fault code inside the 200 is the node's error.
			name:           "a timeout inside a 200 does not vote",
			replies:        []string{timedOut, checkpoint, checkpoint},
			threshold:      2,
			wantVotes:      []int{2},
			wantNodeErrors: 1,
			wantAgreed:     checkpoint,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := test.request
			if request == "" {
				request = checkpointRead
			}
			chainMessage, err := chainParser.ParseMsg("", []byte(request), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, nil, "", "")

			// Registering the whole batch keeps the processor reading past the first reply.
			providers := lavasession.ConsumerSessionsMap{}
			for i := range test.replies {
				providers[fmt.Sprintf("provider%d", i+1)] = &lavasession.SessionInfo{}
			}
			usedProviders := lavasession.NewUsedProviders(nil)
			usedProviders.AddUsed(providers, nil)
			relayProcessor := NewRelayProcessor(ctx,
				&common.CrossValidationParams{AgreementThreshold: test.threshold, MaxParticipants: len(test.replies)},
				RelayProcessorMetrics, RelayProcessorMetrics,
				newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, CrossValidation))

			for i, reply := range test.replies {
				relayProcessor.SetResponse(&RelayResponse{RelayResult: common.RelayResult{
					Reply:        &pairingtypes.RelayReply{Data: []byte(reply)},
					ProviderInfo: common.ProviderInfo{ProviderAddress: fmt.Sprintf("provider%d", i+1)},
					StatusCode:   http.StatusOK,
				}})
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			require.NoError(t, relayProcessor.WaitForResults(waitCtx), "the processor must stop on its replies, not on the timeout")

			require.Equal(t, test.wantVotes, graphQLVotes(t, relayProcessor))
			_, nodeErrors, _ := relayProcessor.GetResultsData()
			require.Len(t, nodeErrors, test.wantNodeErrors)

			result, err := relayProcessor.ProcessingResult()
			if test.wantAgreed != "" {
				require.NoError(t, err)
				require.Equal(t, test.wantAgreed, string(result.Reply.Data))
				require.Equal(t, test.wantVotes[0], result.CrossValidation)
				return
			}
			require.Error(t, err)
			require.Equal(t, test.wantReason, result.CrossValidationFailureReason)
		})
	}
}

// graphQLVotes returns the size of each answer's bucket, largest first. A GraphQL reply always has
// a body, so none may land in the zero-hash bucket, which is the empty-reply fallback.
func graphQLVotes(t *testing.T, rp *RelayProcessor) []int {
	t.Helper()
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	require.NotContains(t, rp.quorumMap, [32]byte{}, "a GraphQL body was tallied as an empty reply")
	votes := make([]int, 0, len(rp.quorumMap))
	for _, stat := range rp.quorumMap {
		votes = append(votes, stat.count)
	}
	slices.Sort(votes)
	slices.Reverse(votes)
	return votes
}
