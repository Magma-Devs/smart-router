package rpcsmartrouter

import (
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// withFreshRelayHoldoff swaps the package's hold-off registry for a fresh one for the
// duration of a test, restoring the shared instance on cleanup.
func withFreshRelayHoldoff(t *testing.T) *holdoff.Registry {
	t.Helper()
	restore := relayHoldoff
	reg := holdoff.NewRegistry()
	relayHoldoff = reg
	t.Cleanup(func() { relayHoldoff = restore })
	return reg
}

// The session-release decision keys on this: a rate-limit in either arriving shape must
// be neither failure nor success.
func TestIsRateLimitedRelayOutcome(t *testing.T) {
	t.Run("typed 429 error", func(t *testing.T) {
		err := httpStatusRelayError(429, nil)
		require.True(t, isRateLimitedRelayOutcome(err, &common.RelayResult{}))
	})
	t.Run("body-shape classification flag", func(t *testing.T) {
		require.True(t, isRateLimitedRelayOutcome(nil, &common.RelayResult{IsRateLimited: true}))
	})
	t.Run("5xx is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(httpStatusRelayError(503, nil), &common.RelayResult{}))
	})
	t.Run("plain transport error is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(errors.New("connection refused"), &common.RelayResult{}))
	})
	t.Run("clean result is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(nil, &common.RelayResult{StatusCode: 200}))
	})
}

// A 429 replay records the endpoint into the hold-off registry, honouring the upstream's
// Retry-After as the floor.
func TestReplayFailingRelay_RecordsRateLimitIntoHoldoff(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	rpcss := replayServer()
	conn := &fakeDirectConn{err: &routersession.HTTPStatusError{StatusCode: 429, Status: "429", RetryAfter: 5 * time.Minute}}
	epc := &routersession.EndpointWithDirectConnection{
		Endpoint:         &routersession.Endpoint{NetworkAddress: "http://fake:8545"},
		DirectConnection: conn,
		ProviderAddress:  "provider1",
	}

	verdict := rpcss.replayFailingRelay(epc, "eth_call", testEvidence, testEvidenceTimeout)
	require.Equal(t, relayProbeInconclusive, verdict)
	require.True(t, reg.HeldOff("provider1", "http://fake:8545"))
	// The floor: at least the vendor's ask (jitter may extend it).
	require.False(t, reg.ReadyAt("provider1", "http://fake:8545").Before(time.Now().Add(5*time.Minute-time.Second)))
}

// While the endpoint is held off, the replay is skipped entirely — inconclusive verdict,
// no request spent.
func TestReplayFailingRelay_SkipsWhileHeldOff(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	reg.RecordRateLimit("provider1", "http://fake:8545", time.Minute)

	rpcss := replayServer()
	conn := &fakeDirectConn{resp: &routersession.DirectRPCResponse{StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}}
	epc := &routersession.EndpointWithDirectConnection{
		Endpoint:         &routersession.Endpoint{NetworkAddress: "http://fake:8545"},
		DirectConnection: conn,
		ProviderAddress:  "provider1",
	}

	verdict := rpcss.replayFailingRelay(epc, "eth_call", testEvidence, testEvidenceTimeout)
	require.Equal(t, relayProbeInconclusive, verdict)
	require.Nil(t, conn.gotPayload, "a held-off endpoint must not be probed")
}

// A replay that recovers clears the endpoint's hold-off state — the upstream answered,
// so it is no longer refusing us for load and its strikes reset.
func TestReplayFailingRelay_RecoveredClearsHoldoff(t *testing.T) {
	restore := relayHoldoff
	t.Cleanup(func() { relayHoldoff = restore })
	now := time.Now()
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })
	relayHoldoff = reg

	// Two strikes, then let the hold-off expire.
	reg.RecordRateLimit("provider1", "http://fake:8545", 0)
	reg.RecordRateLimit("provider1", "http://fake:8545", 0)
	now = now.Add(2 * time.Minute)
	require.False(t, reg.HeldOff("provider1", "http://fake:8545"))

	rpcss := replayServer()
	conn := &fakeDirectConn{resp: &routersession.DirectRPCResponse{StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}}
	epc := &routersession.EndpointWithDirectConnection{
		Endpoint:         &routersession.Endpoint{NetworkAddress: "http://fake:8545"},
		DirectConnection: conn,
		ProviderAddress:  "provider1",
	}

	verdict := rpcss.replayFailingRelay(epc, "eth_call", testEvidence, testEvidenceTimeout)
	require.Equal(t, relayProbeRecovered, verdict)

	// The recovery cleared the strikes: a fresh 429 starts from the initial penalty.
	reg.RecordRateLimit("provider1", "http://fake:8545", 0)
	require.Equal(t, now.Add(holdoff.InitialHoldoff), reg.ReadyAt("provider1", "http://fake:8545"))
}
