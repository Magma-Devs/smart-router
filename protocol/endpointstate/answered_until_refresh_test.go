package endpointstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// stragglerConn answers every latest-block poll with one block, and counts the polls.
type stragglerConn struct {
	url   string
	block atomic.Int64
	polls atomic.Int32
}

func (c *stragglerConn) SendRequest(ctx context.Context, data []byte, _ map[string]string) (*lavasession.DirectRPCResponse, error) {
	if strings.Contains(string(data), "eth_blockNumber") {
		c.polls.Add(1)
	}
	return &lavasession.DirectRPCResponse{
		Data:       []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, c.block.Load())),
		StatusCode: 200,
	}, nil
}

func (c *stragglerConn) GetProtocol() lavasession.DirectRPCProtocol {
	return lavasession.DirectRPCProtocolHTTP
}
func (c *stragglerConn) Close() error                { return nil }
func (c *stragglerConn) GetURL() string              { return c.url }
func (c *stragglerConn) GetNodeUrl() *common.NodeUrl { return nil }

// TestAnsweredPollBelowAFreshPeerTipStillReportsTheAnswer is MAG-4347, from the repro Anna Rayer
// attached to the ticket.
//
// A block below a still-fresh stored tip is a straggler, and the tip store drops it. The poll-block
// hook used to fire only when the store accepted the block, so a straggler wrote nothing for the
// url — and that write is the url's per-url latest-block series and the answered-until time the
// per-url stuck alerts read.
//
// The fleet gate turns that from an occasional event into a steady state: each peer skip adopts the
// peer's block again, which keeps the stored tip fresh, so every forced poll that answers lower than
// the peer is a straggler. Solana reaches it by design, because the tracker polls at finalized
// commitment while relays harvest the slot at the client's, which is newer; so does the non-relayed
// provider of a url two providers share.
//
// Here a peer reports one block above what this pod's polls answer. Every poll is answered, and
// every answer must be reported.
func TestAnsweredPollBelowAFreshPeerTipStillReportsTheAnswer(t *testing.T) {
	const url = "http://straggler-poll:8545"
	ensureRandSeeded()
	sink := &pollBlockSink{}
	peers := &fakePeerStore{}
	peers.set(257, "other-pod/abcd", 0) // the peer saw one block more than this pod's polls get

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 400 * time.Millisecond,
		BlocksToSave:     1,
		OnPollBlock:      sink.record,
		PeerObservations: peers,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)

	conn := &stragglerConn{url: url}
	conn.block.Store(256)
	_, err := m.GetOrCreateTracker("straggler-provider", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	time.Sleep(3200 * time.Millisecond)

	polls := conn.polls.Load()
	reported := sink.snapshot()
	t.Logf("upstream eth_blockNumber requests=%d (all answered 256), reported=%d", polls, len(reported))
	require.GreaterOrEqual(t, polls, int32(4), "init plus at least three forced polls, all answered")

	obs, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Zero(t, obs.ConsecutivePollFailures, "every poll counted as a success")
	require.Greater(t, len(reported), 1,
		"answered polls after init must keep the url's series answering, or it reads as down while it answers")
	for _, r := range reported {
		require.Equal(t, url, r.url)
		require.Equal(t, int64(256), r.block,
			"the block reported is the one the url answered, not the peer's")
	}
}

// TestAnsweredUntilStopsWhenTheUrlStopsAnswering is the other half of the ticket's acceptance: the
// fix must not make a dead url look alive. A failed poll reports nothing, so the url's series stops
// moving and its answered-until time passes — which is what the stuck alerts read as down.
func TestAnsweredUntilStopsWhenTheUrlStopsAnswering(t *testing.T) {
	const url = "http://stops-answering:8545"
	sink := &pollBlockSink{}
	m := newPollBlockMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(0))
	require.Len(t, sink.snapshot(), 1)

	for i := 1; i <= 5; i++ {
		m.recordPollObservation(url, gen, 0, 0, errors.New("connection refused"), at(i))
	}
	require.Len(t, sink.snapshot(), 1, "a url that stopped answering writes nothing more")

	obs, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, 5, obs.ConsecutivePollFailures)
}

// TestStuckUrlKeepsAnsweringWithItsOldBlock: a url frozen below the rest of the fleet reports its
// own frozen block on every poll. Its series stands still while its answered-until time keeps
// moving, which is the shape the stuck alerts look for — and the reason the hook reports the
// answered block rather than the stored tip.
func TestStuckUrlKeepsAnsweringWithItsOldBlock(t *testing.T) {
	const url = "http://stuck-below-the-fleet:8545"
	sink := &pollBlockSink{}
	m := newPollBlockMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }

	// A relay from elsewhere puts a much newer tip in the store for this url, so every poll
	// below it is a straggler for as long as that tip stays fresh.
	require.True(t, m.RecordRelayObservation(url, gen, 9000, at(0)))

	for i := 1; i <= 4; i++ {
		m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(i))
	}

	reported := sink.snapshot()
	require.Len(t, reported, 4, "every answered poll is reported, straggler or not")
	for _, r := range reported {
		require.Equal(t, int64(100), r.block, "the url's own frozen block, not the stored tip")
	}
}
