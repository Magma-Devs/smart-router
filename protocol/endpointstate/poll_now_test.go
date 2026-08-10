package endpointstate

// End-to-end tests for the MAG-2649 poll-now trigger, driven through a REAL ChainTracker built by
// GetOrCreateTracker — so the production poll callbacks (observation record, endpoint tip,
// tracker-state write) are all wired exactly as in the router.
//
// Two of these are load-bearing beyond their assertions:
//   - the block ADVANCES between polls, which is what makes the tracker fire newLatestCallback →
//     setTrackerState → m.mu.Lock() inside the poll cycle. PollNow must therefore have released the
//     monitor lock before waiting on the tracker; a version that holds it deadlocks here and would
//     pass against a mock returning a constant block.
//   - the failure case asserts the observation's ConsecutivePollFailures, which is the ticket's
//     "running count of failed polls" — recorded by the shared production path, not by poll-now.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// pollNowConn answers the two ETH poll requests (eth_blockNumber and eth_getBlockByNumber) from a
// live block counter, so a test can move the chain forward between polls. Hashes are derived from
// the REQUESTED block, not the current head, so re-reading an older block returns the same hash it
// had (a head-derived hash would look like a fork on every advance). fail breaks the transport.
// delayNanos, when set, makes every request take that long — the knob the deadline-split tests use
// to build a poll cycle that outlives a short deadline.
type pollNowConn struct {
	url        string
	block      atomic.Int64
	fail       atomic.Bool
	delayNanos atomic.Int64
}

func (c *pollNowConn) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	if delay := time.Duration(c.delayNanos.Load()); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.fail.Load() {
		return nil, errors.New("pollNowConn: upstream unreachable")
	}
	request := string(data)
	if strings.Contains(request, "eth_getBlockByNumber") {
		return &lavasession.DirectRPCResponse{
			Data:       []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"hash":"0x%064x"}}`, requestedBlockOf(request))),
			StatusCode: 200,
		}, nil
	}
	return &lavasession.DirectRPCResponse{
		Data:       []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, c.block.Load())),
		StatusCode: 200,
	}, nil
}

func (c *pollNowConn) GetProtocol() lavasession.DirectRPCProtocol {
	return lavasession.DirectRPCProtocolHTTP
}
func (c *pollNowConn) Close() error                { return nil }
func (c *pollNowConn) GetURL() string              { return c.url }
func (c *pollNowConn) GetNodeUrl() *common.NodeUrl { return nil }

// requestedBlockOf pulls the block number out of an eth_getBlockByNumber body
// ({"params":["0x3e8", false]}). Returns 0 when it cannot be read, which still yields a stable
// hash — the tests never depend on the value itself, only on it being per-block.
func requestedBlockOf(request string) int64 {
	var body struct {
		Params []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal([]byte(request), &body); err != nil || len(body.Params) == 0 {
		return 0
	}
	var blockHex string
	if err := json.Unmarshal(body.Params[0], &blockHex); err != nil {
		return 0
	}
	block, err := strconv.ParseInt(strings.TrimPrefix(blockHex, "0x"), 16, 64)
	if err != nil {
		return 0
	}
	return block
}

// newPollNowMonitor wires a monitor + one live ETH tracker whose cadence (avgBlockTime/2 = 30 s) is
// far beyond the test's lifetime, so nothing the test observes can come from the timer. It returns
// once the tracker is actually polling, i.e. its init succeeded and the poll goroutine exists.
func newPollNowMonitor(t *testing.T, ctx context.Context, url string, startBlock int64) (*EndpointMonitor, *pollNowConn) {
	t.Helper()
	ensureRandSeeded()

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)

	conn := &pollNowConn{url: url}
	conn.block.Store(startBlock)
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		state, _, exists := m.GetTrackerState(url)
		return exists && state == EndpointChainTrackerPolling
	}, 10*time.Second, 20*time.Millisecond, "the tracker must reach its poll loop before poll-now is triggered")

	return m, conn
}

// TestEndpointMonitor_PollNow_RecordsAdvancedBlockImmediately is the MAG-2649 end-to-end proof: a
// block that appears after the last poll is observed and recorded the moment poll-now is called,
// with the tracker's next scheduled poll still ~30 s away. It also proves the trigger records what
// the ticket lists — block, source, and the successful-poll stamp — via the production path.
func TestEndpointMonitor_PollNow_RecordsAdvancedBlockImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const url = "http://eth-pollnow:8545"
	m, conn := newPollNowMonitor(t, ctx, url, 1000)
	defer m.Stop()

	before, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(1000), before.LatestBlock, "the init poll seeds the record")

	conn.block.Store(1042) // a new head arrives; nothing has polled it yet

	obs, polled, err := m.PollNow(ctx, url)
	require.NoError(t, err)
	require.True(t, polled)
	require.Equal(t, int64(1042), obs.LatestBlock, "poll-now observed the new head without waiting for the timer")
	require.Equal(t, ObservationSourcePoll, obs.Source)
	require.True(t, obs.LastSuccessfulPoll.After(before.LastSuccessfulPoll), "a successful poll re-stamps LastSuccessfulPoll")
	require.Equal(t, 0, obs.ConsecutivePollFailures)
	require.Empty(t, obs.LastPollError)
}

// TestEndpointMonitor_PollNow_FailedPollIsRecordedAsFailure covers the outcome a test asserting on
// the failure streak needs: the poll RAN (polled=true) and failed, so the streak advances while the
// last-good block and success stamp are left alone. This is the ticket's "running count of failed
// polls", written by recordPollObservation exactly as on a timer-driven failure.
func TestEndpointMonitor_PollNow_FailedPollIsRecordedAsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const url = "http://eth-pollnow-fail:8545"
	m, conn := newPollNowMonitor(t, ctx, url, 2000)
	defer m.Stop()

	good, ok := m.GetObservation(url)
	require.True(t, ok)

	conn.fail.Store(true)
	obs, polled, err := m.PollNow(ctx, url)
	require.True(t, polled, "the cycle ran and reached upstream — it just failed there")
	require.Error(t, err)
	require.Equal(t, 1, obs.ConsecutivePollFailures, "the failure streak advances")
	require.NotEmpty(t, obs.LastPollError)
	require.Equal(t, good.LatestBlock, obs.LatestBlock, "a failed poll observes no new block")
	require.Equal(t, good.LastSuccessfulPoll, obs.LastSuccessfulPoll, "a failed poll does not stamp success")

	obs, polled, err = m.PollNow(ctx, url)
	require.True(t, polled)
	require.Error(t, err)
	require.Equal(t, 2, obs.ConsecutivePollFailures, "consecutive forced failures keep accumulating")

	// And recovery clears it, through the same record path.
	conn.fail.Store(false)
	obs, polled, err = m.PollNow(ctx, url)
	require.NoError(t, err)
	require.True(t, polled)
	require.Equal(t, 0, obs.ConsecutivePollFailures, "a successful poll resets the streak")
}

// TestEndpointMonitor_PollNow_ForcesPastAFreshRelayTip composes the two halves the pytest suite
// actually uses in sequence: serve a relay (which harvests a tip and arms the traffic gate), then
// trigger a poll. The ordinary cycle would SKIP here — a relay-sourced tip younger than
// relayGateFreshness suppresses it — so without the force this endpoint would return the pre-relay
// record and the trigger would look broken. The forced poll goes upstream anyway and records.
func TestEndpointMonitor_PollNow_ForcesPastAFreshRelayTip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const url = "http://eth-pollnow-gated:8545"
	m, conn := newPollNowMonitor(t, ctx, url, 3000)
	defer m.Stop()

	// A served relay harvests the current tip — exactly what the router's relay path does.
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)
	require.True(t, m.RecordRelayObservation(url, gen, 3000, time.Now()))
	require.Condition(t, func() bool {
		_, fresh := m.freshRelayTip(url, time.Now())
		return fresh
	}, "the fixture must actually arm the traffic gate, or this test proves nothing")

	conn.block.Store(3050)

	obs, polled, err := m.PollNow(ctx, url)
	require.NoError(t, err)
	require.True(t, polled, "a fresh relay tip must not turn the trigger into a silent skip")
	require.Equal(t, int64(3050), obs.LatestBlock)
	require.Equal(t, ObservationSourcePoll, obs.Source, "the forced poll is the source of the new tip")
}

// TestEndpointMonitor_PollNow_UnknownEndpoint: an endpoint with no ChainTracker reports "nothing
// polled" rather than an empty success, so the debug handler can answer 404/504 honestly instead of
// handing back a zero record that reads like a fresh poll.
func TestEndpointMonitor_PollNow_UnknownEndpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
	})
	defer m.Stop()

	obs, polled, err := m.PollNow(ctx, "http://never-registered:8545")
	require.Error(t, err)
	require.False(t, polled)
	require.Equal(t, EndpointObservation{}, obs)
}

// TestEndpointMonitor_PollNow_TrackerNotPolling_NamesTheState covers the endpoint that is
// registered but cannot poll yet (its init keeps failing, so it sits in startTrackerWithRetry). The
// caller must learn WHY nothing ran — the tracker's lifecycle state is named in the error — instead
// of being handed a bare timeout.
func TestEndpointMonitor_PollNow_TrackerNotPolling_NamesTheState(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond, // fail init fast rather than spacing retries out
		BlocksToSave:     1,
	})
	defer m.Stop()

	const url = "http://eth-pollnow-dead:8545"
	conn := &pollNowConn{url: url}
	conn.fail.Store(true) // every fetch fails → init never completes → no poll goroutine
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	triggerCtx, triggerCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer triggerCancel()

	_, polled, err := m.PollNow(triggerCtx, url)
	require.False(t, polled, "no poll can run while the tracker is still trying to start")
	require.Error(t, err)
	require.ErrorIs(t, err, chaintracker.ErrorPollNowNotDelivered, "nothing was taken, so nothing ran")
	require.Contains(t, err.Error(), "trackerState", "the error names the tracker's lifecycle state")
}

// TestEndpointMonitor_PollNow_LaggingTrackerStateDoesNotTruncateThePoll is the MAG-2649 review
// regression, and it covers precisely the window the unstarted-grace comment claims is safe.
//
// The grace fires whenever trackerStates says anything but Polling. That includes the moment right
// after StartAndServe returns, when the poll goroutine is already at its select but the state write
// has not landed — so the send is taken instantly and a full poll cycle then runs under the grace.
// While one context bounded both waits, that cycle was abandoned at 2 s even though nothing was
// wrong with it, the error carried no sentinel, and PollNow reported polled=true over the PRE-poll
// record: a stale block and a stale failure streak, both labelled fresh. Worse than the refusal the
// grace exists to avoid.
//
// The fixture forces the lagging state deliberately rather than trying to hit the real race, which
// is microseconds wide. What it reproduces is the same code path with the same inputs.
func TestEndpointMonitor_PollNow_LaggingTrackerStateDoesNotTruncateThePoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const url = "http://eth-pollnow-lagging:8545"
	m, conn := newPollNowMonitor(t, ctx, url, 7000)
	defer m.Stop()

	// A cycle that comfortably outlives the 2 s grace: two fetches at 1.5 s each.
	conn.delayNanos.Store(int64(1500 * time.Millisecond))
	require.Greater(t, 2*1500*time.Millisecond, pollNowUnstartedGrace,
		"the fixture only proves anything while the poll outlasts the grace")

	// The lagging window: the goroutine is polling, the recorded state has not caught up.
	m.mu.Lock()
	m.trackerStates[url] = EndpointChainTrackerStarting
	m.mu.Unlock()

	conn.block.Store(7042)

	obs, polled, err := m.PollNow(ctx, url)
	require.NoError(t, err, "a healthy poll must not be cut short by a deadline meant for delivery")
	require.True(t, polled)
	require.Equal(t, int64(7042), obs.LatestBlock,
		"polled=true must mean the returned record is THIS poll's, never the one from before it")
	require.Equal(t, ObservationSourcePoll, obs.Source)
}

// TestEndpointMonitor_PollNow_ResultNotAwaited_ReportsNotPolled covers the outcome that survives the
// deadline split: the caller's own budget really is too short for the cycle. The poll is under way
// and will record, but this call cannot say what it recorded — so it must report polled=false.
//
// The alternative is the bug this pair of tests exists to prevent. polled=true alongside a PollError
// means, per the handler's documented vocabulary, "a poll reached upstream and failed, and
// ConsecutivePollFailures went up". A harness reading that asserts on a failure streak that was
// never recorded, off a record that predates the call entirely.
func TestEndpointMonitor_PollNow_ResultNotAwaited_ReportsNotPolled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const url = "http://eth-pollnow-slow:8545"
	m, conn := newPollNowMonitor(t, ctx, url, 8000)
	defer m.Stop()

	before, ok := m.GetObservation(url)
	require.True(t, ok)

	conn.delayNanos.Store(int64(2 * time.Second))
	conn.block.Store(8080)

	// Delivery is instant (the tracker is Polling); it is the cycle that outlives this budget.
	shortCtx, shortCancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer shortCancel()

	obs, polled, err := m.PollNow(shortCtx, url)
	require.False(t, polled, "an unwitnessed poll must never be reported as a completed one")
	require.ErrorIs(t, err, chaintracker.ErrorPollNowResultNotAwaited)
	require.NotErrorIs(t, err, chaintracker.ErrorPollNowNotDelivered, "the trigger WAS delivered")
	require.Equal(t, before.LatestBlock, obs.LatestBlock, "the record handed back is the pre-poll one")
	require.Equal(t, before.ConsecutivePollFailures, obs.ConsecutivePollFailures,
		"and its failure streak is the pre-poll one too — the field a harness would have misread")

	// The abandoned poll is bounded by the tracker, not the caller: it finishes and records. Proving
	// that is what makes polled=false the honest answer rather than an under-report.
	conn.delayNanos.Store(0)
	require.Eventually(t, func() bool {
		latest, found := m.GetObservation(url)
		return found && latest.LatestBlock == 8080
	}, 15*time.Second, 20*time.Millisecond, "the poll the caller stopped waiting for still completed")
}
