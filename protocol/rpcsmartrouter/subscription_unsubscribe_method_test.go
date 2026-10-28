package rpcsmartrouter

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestResolveTeardownUnsubscribeMethod drives the resolver with the directive shapes the
// lava-specs catalog actually declares. Each case names the chains it stands for, so a future
// edit can tell a real spec shape from a synthetic one.
//
// The rows that matter for MAG-4278 item 1 are the Solana and Substrate ones: the string
// derivation these paths used before returns the literal "unsubscribe" for every one of them,
// which is a method only Tendermint has.
func TestResolveTeardownUnsubscribeMethod(t *testing.T) {
	solana := solanaSubscriptionDirectives()

	// The Substrate collection every Polkadot-family spec declares (ACA, ASTAR, KUSAMA,
	// POLKADOT, …): all the subscribes, then all the unsubscribes, and three pairs that no
	// string rule reaches.
	substrate := subscriptionDirectives(
		[2]string{"author_submitAndWatchExtrinsic", "author_unwatchExtrinsic"},
		[2]string{"chainHead_v1_follow", "chainHead_v1_unfollow"},
		[2]string{"chain_subscribeAllHeads", "chain_unsubscribeAllHeads"},
		[2]string{"chain_subscribeFinalisedHeads", "chain_unsubscribeFinalisedHeads"},
		[2]string{"chain_subscribeNewHead", "chain_unsubscribeNewHead"},
		[2]string{"chain_subscribeNewHeads", "chain_unsubscribeNewHeads"},
		[2]string{"chain_subscribeRuntimeVersion", "chain_unsubscribeRuntimeVersion"},
		[2]string{"state_subscribeRuntimeVersion", "state_unsubscribeRuntimeVersion"},
		[2]string{"state_subscribeStorage", "state_unsubscribeStorage"},
		[2]string{"subscribe_newHead", "unsubscribe_newHead"},
		[2]string{"transactionWatch_v1_submitAndWatch", "transactionWatch_v1_unwatch"},
	)

	for _, tc := range []struct {
		name       string
		directives []*spectypes.ParseDirective
		subscribe  string
		want       string
		wantOK     bool
	}{
		{
			name:       "ETH1: one pair, the token rule",
			directives: subscriptionDirectives([2]string{"eth_subscribe", "eth_unsubscribe"}),
			subscribe:  "eth_subscribe", want: "eth_unsubscribe", wantOK: true,
		},
		{
			name:       "TENDERMINT: the bare pair",
			directives: subscriptionDirectives([2]string{"subscribe", "unsubscribe"}),
			subscribe:  "subscribe", want: "unsubscribe", wantOK: true,
		},
		{
			name:       "SOLANA: nine pairs, each client's own",
			directives: solana,
			subscribe:  "accountSubscribe", want: "accountUnsubscribe", wantOK: true,
		},
		{
			name:       "SOLANA: the pair whose name the derivation gets closest to",
			directives: solana,
			subscribe:  "slotsUpdatesSubscribe", want: "slotsUpdatesUnsubscribe", wantOK: true,
		},
		{
			name:       "Substrate: the token rule across a split list",
			directives: substrate,
			subscribe:  "chain_subscribeNewHeads", want: "chain_unsubscribeNewHeads", wantOK: true,
		},
		{
			name:       "Substrate: a subscribe-prefixed name",
			directives: substrate,
			subscribe:  "subscribe_newHead", want: "unsubscribe_newHead", wantOK: true,
		},
		{
			name:       "Substrate: watch pairs with no subscribe token, by namespace",
			directives: substrate,
			subscribe:  "author_submitAndWatchExtrinsic", want: "author_unwatchExtrinsic", wantOK: true,
		},
		{
			name:       "Substrate: follow/unfollow, by namespace",
			directives: substrate,
			subscribe:  "chainHead_v1_follow", want: "chainHead_v1_unfollow", wantOK: true,
		},
		{
			name:       "Substrate: submitAndWatch/unwatch, by namespace",
			directives: substrate,
			subscribe:  "transactionWatch_v1_submitAndWatch", want: "transactionWatch_v1_unwatch", wantOK: true,
		},
		{
			name: "FVM: a subscribe the spec declares no unsubscribe for keeps its hands off the other pair's",
			directives: subscriptionDirectives(
				[2]string{"Filecoin.EthSubscribe", "Filecoin.EthUnsubscribe"},
				[2]string{"Filecoin.ChainNotify", ""},
			),
			subscribe: "Filecoin.ChainNotify", want: "", wantOK: false,
		},
		{
			name: "ENJ: two archive subscribes share a namespace with one unclaimed unsubscribe, so neither may take it",
			directives: subscriptionDirectives(
				[2]string{"archive_v1_storage", "archive_v1_stopStorage"},
				[2]string{"archive_v1_storageDiff", ""},
			),
			subscribe: "archive_v1_storage", want: "", wantOK: false,
		},
		{
			name: "a namespace with the subscribe's own unsubscribe already claimed by the token rule",
			directives: subscriptionDirectives(
				[2]string{"chain_subscribeNewHeads", "chain_unsubscribeNewHeads"},
				[2]string{"chain_follow", ""},
			),
			subscribe: "chain_follow", want: "", wantOK: false,
		},
		{
			name:       "a collection that declares no unsubscribe falls back to the derivation",
			directives: subscriptionDirectives([2]string{"eth_subscribe", ""}),
			subscribe:  "eth_subscribe", want: "eth_unsubscribe", wantOK: true,
		},
		{
			name:       "no collection at all falls back to the derivation",
			directives: nil,
			subscribe:  "chain_subscribe", want: "chain_unsubscribe", wantOK: true,
		},
		{
			name:       "the derivation refuses a name it cannot reach, rather than returning the literal unsubscribe",
			directives: nil,
			subscribe:  "accountSubscribe", want: "", wantOK: false,
		},
		{
			name:       "an empty subscribe method resolves to nothing",
			directives: solana,
			subscribe:  "", want: "", wantOK: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collection := &spectypes.ApiCollection{ParseDirectives: tc.directives}
			got, ok := resolveTeardownUnsubscribeMethod(collection, tc.subscribe)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("a nil collection resolves through the derivation", func(t *testing.T) {
		got, ok := resolveTeardownUnsubscribeMethod(nil, "eth_subscribe")
		require.True(t, ok)
		require.Equal(t, "eth_unsubscribe", got)
	})

	// Every Solana pair, because the manager picks one per subscription and a rule that
	// resolved only the first would look right in the row above.
	t.Run("every SOLANA pair resolves to its own unsubscribe", func(t *testing.T) {
		for _, pair := range [][2]string{
			{"accountSubscribe", "accountUnsubscribe"},
			{"blockSubscribe", "blockUnsubscribe"},
			{"logsSubscribe", "logsUnsubscribe"},
			{"programSubscribe", "programUnsubscribe"},
			{"rootSubscribe", "rootUnsubscribe"},
			{"signatureSubscribe", "signatureUnsubscribe"},
			{"slotSubscribe", "slotUnsubscribe"},
			{"slotsUpdatesSubscribe", "slotsUpdatesUnsubscribe"},
			{"voteSubscribe", "voteUnsubscribe"},
		} {
			got, ok := resolveTeardownUnsubscribeMethod(&spectypes.ApiCollection{ParseDirectives: solana}, pair[0])
			require.True(t, ok, pair[0])
			require.Equal(t, pair[1], got)
			// What the derivation these paths used would have sent instead.
			derived, derivedOK := derivedUnsubscribeMethod(pair[0])
			require.False(t, derivedOK, "the derivation cannot reach %s", pair[1])
			require.Empty(t, derived)
		}
	})
}

// TestDerivedUnsubscribeMethod_RefusesTheBareLiteral pins the one change to the legacy
// derivation: it used to answer every name it could not reach with the literal "unsubscribe",
// which only Tendermint has. That answer is what the teardown paths sent to Solana and
// Substrate nodes (MAG-4278 item 1). It now says it has no answer, and the caller sends
// nothing.
func TestDerivedUnsubscribeMethod_RefusesTheBareLiteral(t *testing.T) {
	for _, subscribe := range []string{
		"accountSubscribe",
		"chain_subscribeNewHeads",
		"chainHead_v1_follow",
		"author_submitAndWatchExtrinsic",
		"subscribe_newHead",
		"",
	} {
		got, ok := derivedUnsubscribeMethod(subscribe)
		require.False(t, ok, subscribe)
		require.Empty(t, got, subscribe)
	}

	for _, tc := range [][2]string{
		{"eth_subscribe", "eth_unsubscribe"},
		{"subscribe", "unsubscribe"},
		{"chain_subscribe", "chain_unsubscribe"},
	} {
		got, ok := derivedUnsubscribeMethod(tc[0])
		require.True(t, ok, tc[0])
		require.Equal(t, tc[1], got)
	}
}

func TestApiNamespaceOf(t *testing.T) {
	for _, tc := range [][2]string{
		{"chainHead_v1_follow", "chainHead_v1_"},
		{"author_submitAndWatchExtrinsic", "author_"},
		{"Filecoin.ChainNotify", "Filecoin."},
		{"accountSubscribe", ""},
		{"", ""},
	} {
		require.Equal(t, tc[1], apiNamespaceOf(tc[0]), tc[0])
	}
}
