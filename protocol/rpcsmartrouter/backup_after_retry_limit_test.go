package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// upstreamBehaviour is what a test upstream does with every request.
type upstreamBehaviour int

const (
	servesTheRequest     upstreamBehaviour = iota
	refusesTheConnection                   // the port is closed, so every dial is refused
	answersUnavailable                     // HTTP 503 at once: the provider simulator's "down"
)

// tierUpstream is one local upstream and the number of requests that reached it.
type tierUpstream struct {
	name      string
	behaviour upstreamBehaviour
	hits      int64
}

func (u *tierUpstream) requests() int64 { return atomic.LoadInt64(&u.hits) }

const tierTestAnswer = `{"jsonrpc":"2.0","id":1,"result":"0x10"}`

// newTieredTestServer builds a router over local upstreams, primaries first, then backups. A
// refusing upstream is a server that was closed once its connection existed, so its port refuses
// every dial from then on, which is the "port closed" shape of the ticket.
func newTieredTestServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, primaries, backups []*tierUpstream) *RPCSmartRouterServer {
	t.Helper()
	ctx := context.Background()
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	buildTier := func(upstreams []*tierUpstream) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		tier := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
		for i, upstream := range upstreams {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt64(&upstream.hits, 1)
				w.Header().Set("Content-Type", "application/json")
				if upstream.behaviour == answersUnavailable {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`{"error":"down"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tierTestAnswer))
			}))
			connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL}, 5, "")
			require.NoError(t, err)
			if upstream.behaviour == refusesTheConnection {
				server.Close()
			} else {
				t.Cleanup(server.Close)
			}
			endpoint := &lavasession.Endpoint{NetworkAddress: server.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
			provider := lavasession.NewConsumerSessionWithProvider(upstream.name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
			provider.StaticProvider = true
			tier[uint64(i)] = provider
		}
		return tier
	}
	require.NoError(t, csm.UpdateAllProviders(1, buildTier(primaries), buildTier(backups)))

	return &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

// dispatchOrder reads the provider-address header, which the router fills with every provider the
// request was sent to, in dispatch order, the one that served it last.
func dispatchOrder(result *common.RelayResult) []string {
	return strings.Split(refusalReplyHeader(result, common.PROVIDER_ADDRESS_HEADER_NAME), ",")
}

func failingUpstreams(behaviour upstreamBehaviour, names ...string) []*tierUpstream {
	upstreams := make([]*tierUpstream, 0, len(names))
	for _, name := range names {
		upstreams = append(upstreams, &tierUpstream{name: name, behaviour: behaviour})
	}
	return upstreams
}

// TestBackupServesWhenEveryPrimaryFailsAtTheRetryLimit_MAG3978 sends one request through
// SendParsedRelay to local upstreams at the default retry limit of 2.
//
// Three primaries that fail fast are three errors, one past the limit, and the policy used to stop
// the request there, one dispatch before selection would have found the primary pool empty and
// handed the request to the backup tier. The caller got HTTP 500 in about 20 ms while a healthy
// backup sat unused. Now the tolerance waits while a backup that could serve the request has not
// been asked, so the request runs down the tier order and the backup answers. Both failure shapes
// the ticket measured are sent: a closed port and a 503 answered at once.
func TestBackupServesWhenEveryPrimaryFailsAtTheRetryLimit_MAG3978(t *testing.T) {
	rand.InitRandomSeed()
	require.Equal(t, 2, relaycore.RelayRetryLimit, "the defect needs the default limit: three primaries are one error past it")
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	shapes := []struct {
		name      string
		behaviour upstreamBehaviour
	}{
		{"the port is closed", refusesTheConnection},
		{"HTTP 503 at once", answersUnavailable},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			t.Run("three primaries fail, the one backup serves", func(t *testing.T) {
				backup := &tierUpstream{name: "backup-1"}
				server := newTieredTestServer(t, chainParser, logs, failingUpstreams(shape.behaviour, "primary-a", "primary-b", "primary-c"), []*tierUpstream{backup})

				result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
				require.NoError(t, sendErr, "the backup serves, so the caller must get its answer")
				require.NotNil(t, result.Reply)
				require.Contains(t, string(result.Reply.Data), `"result":"0x10"`, "the caller gets the backup's answer")
				order := dispatchOrder(result)
				require.Len(t, order, 4, "three primaries, then the backup: %v", order)
				require.ElementsMatch(t, []string{"primary-a", "primary-b", "primary-c"}, order[:3], "every primary is asked before the backup")
				require.Equal(t, "backup-1", order[3], "the backup serves last")
				require.Equal(t, "3", refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME), "three primaries, then the backup")
				require.Equal(t, int64(1), backup.requests(), "the backup is asked once")
			})

			t.Run("three primaries and two of three backups fail: the healthy backup serves", func(t *testing.T) {
				backups := failingUpstreams(shape.behaviour, "backup-1", "backup-2")
				healthy := &tierUpstream{name: "backup-3"}
				server := newTieredTestServer(t, chainParser, logs, failingUpstreams(shape.behaviour, "primary-a", "primary-b", "primary-c"), append(backups, healthy))

				result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
				require.NoError(t, sendErr, "the healthy backup serves")
				// The optimizer picks among the backups, so the healthy one may be asked first,
				// second or third. What is fixed: every primary came before any backup, only
				// failing backups sit between them and the one that served, and nothing stopped
				// the request on the way. The exhaustive case below pins that every backup is
				// asked when none can serve.
				order := dispatchOrder(result)
				require.GreaterOrEqual(t, len(order), 4, "three primaries, then the backups: %v", order)
				require.ElementsMatch(t, []string{"primary-a", "primary-b", "primary-c"}, order[:3], "every primary is asked before any backup")
				require.Subset(t, []string{"backup-1", "backup-2"}, order[3:len(order)-1], "only failing backups sit between the primaries and the one that served")
				require.Equal(t, "backup-3", order[len(order)-1], "the healthy backup serves last")
				require.Equal(t, strconv.Itoa(len(order)-1), refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME), "one retry per failed provider")
				require.Equal(t, int64(1), healthy.requests())
			})

			t.Run("four primaries fail, one backup: the limit waits for the backup", func(t *testing.T) {
				// The rule's reach, recorded on purpose: while a backup is untried the limit does
				// not bound the primaries either, so a pool larger than the limit is tried in full
				// before the backup. The alternative, skipping the untried primaries, would change
				// the documented order (backups once the primary pool is exhausted).
				backup := &tierUpstream{name: "backup-1"}
				server := newTieredTestServer(t, chainParser, logs, failingUpstreams(shape.behaviour, "primary-a", "primary-b", "primary-c", "primary-d"), []*tierUpstream{backup})

				result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
				require.NoError(t, sendErr)
				order := dispatchOrder(result)
				require.Len(t, order, 5, "four primaries, then the backup: %v", order)
				require.ElementsMatch(t, []string{"primary-a", "primary-b", "primary-c", "primary-d"}, order[:4], "every primary is asked before the backup")
				require.Equal(t, "backup-1", order[4])
				require.Equal(t, "4", refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME), "every primary, then the backup")
			})
		})
	}

	t.Run("three primaries and three backups fail: every backup is asked before the request fails", func(t *testing.T) {
		// 503 upstreams, so every attempt is countable. Six providers, six attempts: the limit
		// waits for each untried backup and ends the request only once none is left.
		primaries := failingUpstreams(answersUnavailable, "primary-a", "primary-b", "primary-c")
		backups := failingUpstreams(answersUnavailable, "backup-1", "backup-2", "backup-3")
		server := newTieredTestServer(t, chainParser, logs, primaries, backups)

		_, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
		require.Error(t, sendErr, "nobody can serve")
		for _, upstream := range append(primaries, backups...) {
			require.Equal(t, int64(1), upstream.requests(), "%s is asked exactly once", upstream.name)
		}
	})

	t.Run("control: with no backup tier the limit still stops the request after three primaries", func(t *testing.T) {
		// Four 503 primaries, so a fourth attempt would have somewhere to go and the limit is the
		// only thing that can stop the request. Hits are countable for this shape.
		primaries := failingUpstreams(answersUnavailable, "primary-a", "primary-b", "primary-c", "primary-d")
		server := newTieredTestServer(t, chainParser, logs, primaries, nil)

		_, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
		require.Error(t, sendErr, "nobody can serve, and the limit ends the request")
		var asked int64
		for _, primary := range primaries {
			asked += primary.requests()
		}
		require.Equal(t, int64(3), asked, "the limit of 2 allows three attempts; the fourth primary is never asked")
	})
}
