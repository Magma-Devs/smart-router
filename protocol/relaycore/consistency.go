package relaycore

import (
	"github.com/magma-Devs/smart-router/protocol/common"
)

// The per-user seen-block consistency store used to live here: a ristretto cache keyed by
// dappId__consumerIp, holding a monotonic-max "highest block this user has seen", fed from the
// served provider's Reply.LatestBlock, with a 5-minute TTL plus generation/tombstone machinery so
// the /debug/reset-* endpoints could flush it.
//
// It was RETIRED by Topic C C-G. It was the reference consistency validation measured every
// endpoint against, and it had only a monotonic-increase guard — no anti-lie cross-check. A
// provider reporting a fake-high block poisoned it; honest providers then measured as "behind" and
// were filtered out, handing the liar all traffic on a multi-provider pod and driving a
// single-provider pod to "No pairings". Because the value was monotonic-max and its TTL was kept
// alive by ongoing traffic, it never recovered on its own — hence the reset endpoints and the
// gen/tombstone machinery that existed only to make those resets safe. All of that is gone with it.
//
// Consistency now compares each endpoint's own latest block against the anti-lie-guarded chain tip
// (chainstate.ChainState.GetLatestBlock) — see ValidateEndpointCapability.
//
// Only the key derivation survives, because the shared-state cache path still identifies a caller
// by dapp+IP. Rebuilding that path on a chain-level key is task T10 in
// agent_docs/bug-reports/chainTracker-architecture/topic-C-action-plan.md; once it lands, this
// function has no callers either.

// UserDataKey derives the per-caller identity string used as the shared-state cache id. It is the
// same dappId__consumerIp form the retired consistency cache used for its keys, kept byte-identical
// so shared-state entries written by an older build still resolve.
func UserDataKey(userData common.UserData) string {
	return userData.DappId + "__" + userData.ConsumerIp
}
