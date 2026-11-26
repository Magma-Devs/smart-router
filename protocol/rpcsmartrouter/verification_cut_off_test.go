package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// shortenVerifyDeadlines makes a verification pass run out after d instead of 30s.
func shortenVerifyDeadlines(t *testing.T, d time.Duration) {
	t.Helper()
	prevAttempt, prevBoot := SpecReVerifyAttemptTimeout, BootValidateTimeout
	SpecReVerifyAttemptTimeout, BootValidateTimeout = d, d
	t.Cleanup(func() { SpecReVerifyAttemptTimeout, BootValidateTimeout = prevAttempt, prevBoot })
}

var debugCheck = chainlib.CheckID{Name: "enabled", Addon: "debug"}

// MAG-3950 scenario 6, the hang variant: at an epoch pass the debug check of a provider
// that serves holds until the pass runs out. Only debug is switched off. The url's own
// checks run first, so chain-id is asked and passes; the retries re-run the debug check
// alone, and the provider keeps its place through them until debug answers again. On
// main the debug check ran first, chain-id was cut off, and the provider was demoted at
// the first retry.
func TestVerificationRetries_AnAddonCheckThatHangsCostsOnlyItsAddon(t *testing.T) {
	shortenRetryIntervals(t)
	shortenVerifyDeadlines(t, time.Second)
	up := newFakeUpstreams(t)
	const path = "/dh/a/node"
	up.become(path, upDebug)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(path), Addons: []string{"debug"}})
	provider.Name = "debug-hangs"
	steady := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/dh/b/" + upArchive)})
	steady.Name = "steady"
	rig := newPairedRetryRig(t, provider, steady)
	require.False(t, rig.admissions.admissionFor(provider).Any(), "boot admitted debug")

	up.become(path, upDebugHang)
	asked := len(up.methods("http", path))
	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	admission := rig.admissions.admissionFor(provider)
	require.NotNil(t, rig.session(t, provider.Name), "the provider keeps its place")
	require.False(t, admission.URLRefused(0), "its url is not refused")
	require.False(t, rig.admissions.striking(provider), "and carries no strike")
	require.Equal(t, []chainlib.RefusedService{{Service: "debug", Check: "enabled"}}, admission.Refused(),
		"debug alone is switched off, for its own check")
	require.Contains(t, up.methods("http", path)[asked:], "eth_chainId", "the url's own checks ran before the debug check")

	runRetryLoop(t, rig.rpsr, rig.chainKey, rig.endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool { return attemptsOf(rig.inputs.retries, provider.Name, debugCheck) >= 3 },
		20*time.Second, 10*time.Millisecond, "two retries hang as well")
	rig.rpsr.mu.Lock()
	failed := len(rig.rpsr.failedStaticProviders[rig.chainKey])
	rig.rpsr.mu.Unlock()
	require.Zero(t, failed, "nothing is demoted")
	require.NotNil(t, rig.session(t, provider.Name))
	require.False(t, rig.admissions.admissionFor(provider).URLRefused(0))
	require.Equal(t, 1, count(up.methods("http", path)[asked:], "eth_chainId"), "the retries re-ran only the debug check")

	before := rig.session(t, provider.Name)
	up.become(path, upDebug)
	require.Eventually(t, func() bool {
		return !rig.admissions.admissionFor(provider).Any() && rig.session(t, provider.Name) != before && !rig.pending()
	}, 20*time.Second, 10*time.Millisecond, "debug is back at the first retry that passes, the session rebuilt with it")
}

// Two entries of one url share its connection and are asked in turn. When the first
// one's debug check holds until the pass runs out, the second gets no turn: the deadline
// cuts off its checks. That decides nothing about it, so it is no strike, and passes in a
// row that cut it off never refuse it (MAG-3950).
func TestApplyReverification_AUrlTheDeadlineCutOffIsNoStrike(t *testing.T) {
	shortenRetryIntervals(t)
	shortenVerifyDeadlines(t, time.Second)
	up := newFakeUpstreams(t)
	const path = "/cn/a/node"
	up.become(path, upDebug)
	url := up.httpURL(path)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: url, Addons: []string{"debug"}}, commonlib.NodeUrl{Url: url})
	rig := newPairedRetryRig(t, provider)
	var counted []string
	rig.inputs.countFailedCheck = func(_ reverifyTier, _ string, failure metrics.ProviderVerificationFailure) {
		counted = append(counted, failure.Check)
	}

	up.become(path, upDebugHang)
	for pass := 1; pass <= 2; pass++ {
		rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
		require.False(t, rig.admissions.admissionFor(provider).URLRefused(1), "pass %d: the entry the deadline cut off keeps its place", pass)
		require.False(t, rig.admissions.striking(provider), "pass %d: and carries no strike", pass)
		require.NotNil(t, rig.session(t, provider.Name), "pass %d: the provider serves", pass)
	}
	require.Contains(t, counted, "enabled", "the debug check that ran and failed is a failed run")
	require.NotContains(t, counted, chainlib.CheckNotRun, "the checks the deadline cut off are not")
}

// A check the deadline cut off ran nothing: it is no failed run, and the check the
// provider last failed stands, as after a rate limit.
func TestNoteFailure_ACheckTheDeadlineCutOffIsNoFailedRun(t *testing.T) {
	var counted []string
	inputs := &chainReverifyInputs{countFailedCheck: func(_ reverifyTier, _ string, failure metrics.ProviderVerificationFailure) {
		counted = append(counted, failure.Check)
	}}
	cutOff := &chainlib.VerificationFailure{Check: chainlib.CheckNotRun, Err: errors.New("not run")}
	inputs.noteFailure(reverifyTierStatic, "a", &chainlib.VerificationFailure{Check: "chain-id", Err: errors.New("wrong chain")})
	inputs.noteFailure(reverifyTierStatic, "a", cutOff)
	require.Equal(t, []string{"chain-id"}, counted, "only the check that ran and failed is counted")
	require.Equal(t, "chain-id", inputs.lastFailure[retryKey(reverifyTierStatic, "a")], "and it stays the check the provider last failed")

	inputs.noteFailure(reverifyTierStatic, "b", cutOff)
	require.Equal(t, chainlib.CheckNotRun, inputs.lastFailure[retryKey(reverifyTierStatic, "b")], "one that failed nothing else reads as not-run")
}

// A head read that holds until the pass runs out is the url's own failure: the checks
// measured from the head fail as latest-block, which is a strike, and the url is refused
// under it, not under the checks the deadline then cut off.
func TestValidateProviderCollections_AHeadReadThatHangsIsAFailure(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/hh/a/" + upHeadHang)})
	_, err := validateProviderCollections(t.Context(), provider, liveParser(t, "ETH1", true), time.Second)
	require.Error(t, err)
	require.Equal(t, chainlib.CheckLatestBlock, chainlib.FailedCheck(err))
	pass, ok := chainlib.FailedPass(err)
	require.True(t, ok)
	require.True(t, pass.URLRefusalRan(0), "a strike")
}
