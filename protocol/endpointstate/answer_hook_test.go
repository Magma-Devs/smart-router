package endpointstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// OnAnswer hands the per-chain tip every block a url answered this pod with first-hand (MAG-5080): every
// answered poll and every relay past the generation gate, whatever the url's tip store did with the block,
// after OnTipObservation for the same block. A peer pod's observation is not an answer.

// hookEvents records the OnTipObservation and OnAnswer calls in the order they came.
type hookEvents struct {
	mu     sync.Mutex
	events []string
}

func (h *hookEvents) add(kind string, block int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, fmt.Sprintf("%s %d", kind, block))
}

func (h *hookEvents) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...)
}

func TestOnAnswer_ReportsEveryFirstHandAnswerAfterTheTipFeed(t *testing.T) {
	const url = "http://answer-hook:8545"
	events := &hookEvents{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnTipObservation: func(_ string, block int64, _ bool) { events.add("tip", block) },
		OnAnswer:         func(_ string, block int64) { events.add("answer", block) },
	})
	t.Cleanup(m.Stop)
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(0))   // the store takes it
	m.recordPollObservation(url, gen, 90, time.Millisecond, nil, at(1))    // a straggler the store drops
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(2))      // a failed poll
	require.True(t, m.RecordRelayObservation(url, gen, 101, at(3)))        // the store takes it
	require.False(t, m.RecordRelayObservation(url, gen, 95, at(4)))        // a straggler the store drops
	require.False(t, m.RecordRelayObservation(url, gen+1, 500, at(5)))     // a replaced tracker's relay
	require.True(t, m.recordPeerObservation(url, gen, 102, at(6)))         // another pod's poll
	m.recordPollObservation(url, gen+1, 103, time.Millisecond, nil, at(7)) // a replaced tracker's poll

	require.Equal(t, []string{
		"tip 100", "answer 100",
		"answer 90",
		"tip 101", "answer 101",
		"answer 95",
		"tip 102", // a peer's block feeds the tip, but it is not an answer
	}, events.snapshot())

	m.Stop()
	m.RecordRelayObservation(url, gen, 104, at(8))
	require.Len(t, events.snapshot(), 7, "nothing is reported after Stop")
}

// TestOnAnswer_KeepsTheTipFreshPastABadBackendsStoredBlock wires a real ChainState the way the router
// does and plays a url behind a load balancer whose bad backend answers an absurd block on every other
// relay. The url's tip store keeps the absurd block and drops every good answer as a straggler, so
// OnTipObservation alone never refreshes the tip; OnAnswer does, and the absurd block is never taken.
func TestOnAnswer_KeepsTheTipFreshPastABadBackendsStoredBlock(t *testing.T) {
	const (
		url    = "http://answer-hook-lb:8545"
		honest = int64(5_000_000)
		absurd = int64(1) << 50
	)
	start := time.Unix(1_758_784_000, 0)
	clock := start
	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth: 5, OutlierThreshold: 100, StalenessWindow: 10 * time.Second, TTL: 10 * time.Second,
	}, func() time.Time { return clock })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var storeTook int
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Second, // the url's tip store goes stale after 10 s, like the chain tip
		BlocksToSave:     1,
		OnTipObservation: func(_ string, block int64, _ bool) {
			storeTook++
			cs.SetLatestBlock(block)
		},
		OnAnswer: func(endpointURL string, block int64) { cs.NoteAnswer(endpointURL, block) },
	})
	t.Cleanup(m.Stop)
	gen := m.registerGenForTest(url)

	m.RecordRelayObservation(url, gen, honest, clock)
	for i := 1; i <= 60; i++ {
		clock = start.Add(time.Duration(i) * time.Second)
		block := honest + int64(i)
		if i%2 == 1 {
			block = absurd
		}
		m.RecordRelayObservation(url, gen, block, clock)
		require.NotEqual(t, absurd, cs.GetLatestBlockAllowStale(), "relay %d: the bad backend's block became the tip", i)
	}
	tip, fresh := cs.GetLatestBlock()
	require.True(t, fresh, "the good backend's answers keep the tip fresh")
	require.Equal(t, honest+60, tip)
	require.Equal(t, 1+30, storeTook, "the store took only the first block and the absurd ones")
}
