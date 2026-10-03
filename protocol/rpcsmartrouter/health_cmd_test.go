package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthy/unhealthy row builders for the rollup tests.
func okRow() healthEndpointResult {
	return healthEndpointResult{
		Name: "p", ChainID: "ETH1", APIInterface: "jsonrpc", SpecValid: true, Ok: true,
		Verifications: []healthVerification{{Name: "chain-id", Ok: true}},
	}
}

func failRow() healthEndpointResult {
	return healthEndpointResult{
		Name: "p", ChainID: "ETH1", APIInterface: "jsonrpc", SpecValid: true, Ok: false,
		Verifications: []healthVerification{{Name: "archive", Extension: "archive", Ok: false, Error: "block not found"}},
	}
}

func TestBuildHealthReport_AllHealthy(t *testing.T) {
	report := buildHealthReport([]healthEndpointResult{okRow(), okRow()}, nil)
	assert.True(t, report.Ok)
	assert.Nil(t, report.Error)
	assert.Len(t, report.Results, 2)
}

func TestBuildHealthReport_OneUnhealthyFlipsTopLevel(t *testing.T) {
	// A single failed endpoint makes the top-level ok false, but the run still completed
	// (error stays nil — the failure is data, not a fatal setup error).
	report := buildHealthReport([]healthEndpointResult{okRow(), failRow()}, nil)
	assert.False(t, report.Ok)
	assert.Nil(t, report.Error)
	assert.Len(t, report.Results, 2)
}

func TestBuildHealthReport_FatalError(t *testing.T) {
	report := buildHealthReport(nil, errors.New("config file not found"))
	assert.False(t, report.Ok)
	require.NotNil(t, report.Error)
	assert.Equal(t, "config file not found", *report.Error)
	// Results is always a non-nil array so consumers can iterate unconditionally.
	assert.NotNil(t, report.Results)
	assert.Len(t, report.Results, 0)
}

func TestBuildHealthReport_EmptyResultsIsHealthyArray(t *testing.T) {
	report := buildHealthReport([]healthEndpointResult{}, nil)
	assert.True(t, report.Ok)
	assert.NotNil(t, report.Results)
	assert.Len(t, report.Results, 0)
}

// applyValidation maps one node url's own results into its row: a url that was not
// reached is not ok even with no check failed, and any failed check — Warning
// severity included — makes the row not ok.
func TestApplyValidation(t *testing.T) {
	t.Run("every check passed", func(t *testing.T) {
		row := healthEndpointResult{Verifications: []healthVerification{}}
		applyValidation(&row, chainlib.NodeURLValidation{
			LatestBlock: 5324700,
			Verifications: []chainlib.VerificationResult{
				{Name: "chain-id", Severity: "Fail", Ok: true},
				{Name: "pruning", Extension: "archive", Severity: "Fail", Ok: true},
			},
		})
		assert.True(t, row.Ok)
		assert.Equal(t, int64(5324700), row.LatestBlock)
		assert.Equal(t, []string{"archive"}, row.Extensions)
		assert.Equal(t, "Fail", row.Verifications[1].Severity)
	})
	t.Run("a failed warning still fails the row", func(t *testing.T) {
		row := healthEndpointResult{Verifications: []healthVerification{}}
		applyValidation(&row, chainlib.NodeURLValidation{
			Verifications: []chainlib.VerificationResult{{Name: "tokens-owner-indexed", Severity: "Warning", Ok: false, Error: "unavailable"}},
		})
		assert.False(t, row.Ok)
	})
	t.Run("an unreachable url is not ok with no check run", func(t *testing.T) {
		row := healthEndpointResult{Verifications: []healthVerification{}}
		applyValidation(&row, chainlib.NodeURLValidation{Error: "connect: connection refused", LatestBlock: -1})
		assert.False(t, row.Ok)
		assert.Equal(t, "connect: connection refused", row.Error)
	})
	t.Run("a failed height read is reported", func(t *testing.T) {
		row := healthEndpointResult{Verifications: []healthVerification{}}
		applyValidation(&row, chainlib.NodeURLValidation{LatestBlock: -1, LatestBlockError: "head unavailable"})
		assert.Equal(t, int64(-1), row.LatestBlock)
		assert.Equal(t, "head unavailable", row.LatestBlockError)
	})
}

func TestRouterVerdict(t *testing.T) {
	admitted := routerVerdict(chainlib.NodeURLValidation{RefusedServices: []string{"archive"}}, nil)
	assert.Equal(t, healthRouterVerdict{Verdict: routerAdmitted, RefusedServices: []string{"archive"}}, admitted)

	throttled := routerVerdict(chainlib.NodeURLValidation{Throttled: "429 Too Many Requests"}, nil)
	assert.Equal(t, routerUnknown, throttled.Verdict, "a rate limit decides nothing")

	partly := routerVerdict(chainlib.NodeURLValidation{ThrottledServices: []string{"trace"}}, nil)
	assert.Equal(t, routerAdmitted, partly.Verdict)
	assert.Contains(t, partly.Reason, "trace")

	onlyThrottled := routerVerdict(chainlib.NodeURLValidation{}, fmt.Errorf("no node url verified serving the root path: %w", commonlib.StatusCodeError429))
	assert.Equal(t, routerUnknown, onlyThrottled.Verdict)

	refused := routerVerdict(chainlib.NodeURLValidation{Refused: true, Refusal: "chain-id verification failed: 401"}, nil)
	assert.Equal(t, routerRefused, refused.Verdict)
	assert.Equal(t, "chain-id verification failed: 401", refused.Reason)

	excluded := routerVerdict(chainlib.NodeURLValidation{}, errors.New("no node url left serving the root path: 401"))
	assert.Equal(t, routerExcluded, excluded.Verdict)
	assert.NotNil(t, excluded.RefusedServices, "always an array")
}

func TestTransportForURL(t *testing.T) {
	cases := map[string]string{
		"https://ethereum-rpc.publicnode.com": "http",
		"http://127.0.0.1:8545":               "http",
		"wss://ethereum-rpc.publicnode.com":   "ws",
		"ws://localhost:8546":                 "ws",
		"WSS://EthNode/Path":                  "ws",
		"grpc.example.com:443":                "other",
		"127.0.0.1:9090":                      "other",
	}
	for url, want := range cases {
		assert.Equalf(t, want, transportForURL(url), "transport for %q", url)
	}
}

// Stdout must be a single, parseable JSON document with a stable shape.
func TestHealthReport_JSONShape(t *testing.T) {
	report := buildHealthReport([]healthEndpointResult{
		{
			Name: "eth-publicnode", ChainID: "ETH1", APIInterface: "jsonrpc",
			URL: "wss://ethereum-rpc.publicnode.com", Transport: "ws",
			Addons: []string{}, Extensions: []string{"archive"},
			SpecValid: true, LatestBlock: 2030011, Ok: false,
			Verifications: []healthVerification{
				{Name: "chain-id", Severity: "Fail", Ok: true},
				{Name: "pruning", Severity: "Fail", Ok: false, Error: "block not found"},
			},
			Router: healthRouterVerdict{Verdict: routerRefused, RefusedServices: []string{}, Reason: "pruning verification failed"},
		},
	}, nil)

	raw, err := json.Marshal(report)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	// Envelope keys present and typed as the consumer expects.
	assert.Contains(t, decoded, "ok")
	assert.Contains(t, decoded, "error")
	assert.Contains(t, decoded, "results")
	assert.Nil(t, decoded["error"]) // null, not omitted — uniform envelope

	results, ok := decoded["results"].([]any)
	require.Truef(t, ok, "results must be an array, got %T", decoded["results"])
	require.Len(t, results, 1)
	row, ok := results[0].(map[string]any)
	require.Truef(t, ok, "result row must be an object, got %T", results[0])
	for _, key := range []string{"name", "chainId", "apiInterface", "url", "transport", "addons", "extensions", "specValid", "latestBlock", "ok", "verifications", "router"} {
		assert.Containsf(t, row, key, "result row must carry %q", key)
	}
	assert.Equal(t, "ws", row["transport"])
	assert.Equal(t, false, row["ok"])
	verdict, ok := row["router"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, routerRefused, verdict["verdict"])
	assert.Equal(t, []any{}, verdict["refusedServices"])
	verifications, ok := row["verifications"].([]any)
	require.True(t, ok)
	verification, ok := verifications[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Fail", verification["severity"])
}

func TestNonNilStrings(t *testing.T) {
	assert.Equal(t, []string{}, nonNilStrings(nil))
	assert.Equal(t, []string{"a"}, nonNilStrings([]string{"a"}))
}

func TestSortedKeys(t *testing.T) {
	got := sortedKeys(map[string]struct{}{"archive": {}, "debug": {}, "": {}})
	assert.Equal(t, []string{"", "archive", "debug"}, got)
}

// The inline gRPC flags reach the inline url only when given: unset, the dial tries
// plaintext first and upgrades to TLS when that fails.
func TestInlineAuthFrom(t *testing.T) {
	flags := CreateHealthCobraCommand().Flags()
	assert.Equal(t, commonlib.AuthConfig{}, inlineAuthFrom(flags))

	require.NoError(t, flags.Set("use-tls", "true"))
	require.NoError(t, flags.Set("allow-insecure-connection", "true"))
	assert.Equal(t, commonlib.AuthConfig{UseTLS: true, AllowInsecure: true}, inlineAuthFrom(flags))
}

// An inline probe has no provider name, and the url stands in for it. It is printed
// redacted like the url column: vendors put the key in the path or the query.
func TestCollectHealthProviders_InlineNameIsRedacted(t *testing.T) {
	const keyed = "https://eth.example.com/v2/0123456789abcdef0123456789abcdef"
	providers, err := collectHealthProviders([]string{keyed, "ETH1", "jsonrpc"}, false, commonlib.AuthConfig{UseTLS: true, AllowInsecure: true})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	assert.NotContains(t, providers[0].name, "0123456789abcdef")
	assert.Equal(t, keyed, providers[0].nodeUrls[0].Url, "the url itself is dialed as given")
	assert.True(t, providers[0].nodeUrls[0].AuthConfig.UseTLS, "inline grpc flags reach the node url")
	assert.True(t, providers[0].nodeUrls[0].AuthConfig.AllowInsecure)
}

// --- the per-url probe, against local upstreams ---------------------------------

func healthProbe(t *testing.T, chainID string, skipWebsocket bool, timeout time.Duration, urls ...commonlib.NodeUrl) []healthEndpointResult {
	t.Helper()
	provider := healthProvider{name: "p", chainID: chainID, apiInterface: "jsonrpc", nodeUrls: urls}
	return probeProvider(context.Background(), provider, healthOptions{
		staticSpecPaths: []string{"../../specs/"},
		timeout:         timeout,
		skipWebsocket:   skipWebsocket,
		concurrency:     1,
	})
}

// A dead https url beside a working socket: its checks go to the https url, so the
// https row fails and the socket row stands for the socket alone. The router drops
// the provider — a socket serves no unary traffic.
func TestHealth_EachUrlAnswersForItself(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "ETH1", false, 20*time.Second,
		commonlib.NodeUrl{Url: up.httpURL("/a/" + upDead)},
		commonlib.NodeUrl{Url: up.wsURL("/a/" + upArchive)},
	)
	require.Len(t, rows, 2)

	httpRow, wsRow := rows[0], rows[1]
	assert.False(t, httpRow.Ok, "the dead https url fails its own checks")
	require.NotEmpty(t, httpRow.Verifications)
	for _, v := range httpRow.Verifications {
		assert.Falsef(t, v.Ok, "%s ran against the dead url", v.Name)
	}
	assert.True(t, wsRow.Ok, "the socket passes its own checks")
	assert.Positive(t, wsRow.LatestBlock, "the socket's height is its own")
	assert.Contains(t, up.methods("http", "/a/"+upDead), "eth_chainId", "chain-id reached the https url")
	assert.Contains(t, up.methods("ws", "/a/"+upArchive), "eth_chainId", "chain-id reached the socket too")

	for _, row := range rows {
		assert.Equal(t, routerExcluded, row.Router.Verdict)
		assert.Contains(t, row.Router.Reason, "no node url left serving the root path")
	}
}

// A socket that answers only subscriptions fails the checks sent over it, and the
// router refuses that socket alone; with --skip-websocket-verification it is only dialed.
func TestHealth_SubscriptionOnlySocket(t *testing.T) {
	up := newFakeUpstreams(t)
	urls := []commonlib.NodeUrl{{Url: up.httpURL("/b/" + upArchive)}, {Url: up.wsURL("/b/" + upSubOnly)}}

	rows := healthProbe(t, "ETH1", false, 20*time.Second, urls...)
	require.Len(t, rows, 2)
	assert.True(t, rows[0].Ok)
	assert.Equal(t, routerAdmitted, rows[0].Router.Verdict)
	assert.False(t, rows[1].Ok)
	assert.Equal(t, routerRefused, rows[1].Router.Verdict)

	rows = healthProbe(t, "ETH1", true, 20*time.Second, urls...)
	require.Len(t, rows, 2)
	assert.True(t, rows[1].Ok, "dialed only: the handshake is the whole check")
	assert.Empty(t, rows[1].Verifications)
	assert.Equal(t, routerAdmitted, rows[1].Router.Verdict)
}

// base + archive + socket, all healthy: the archive check runs on the archive url over
// http. Routed by extension it asked for {archive, websocket}, which no url serves.
func TestHealth_ArchiveBesideASocketIsCheckedOverHTTP(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "ETH1", false, 20*time.Second,
		commonlib.NodeUrl{Url: up.httpURL("/c/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL("/c/" + upArchive), Addons: []string{"archive"}},
		commonlib.NodeUrl{Url: up.wsURL("/c/" + upArchive)},
	)
	require.Len(t, rows, 3)
	for _, row := range rows {
		assert.Truef(t, row.Ok, "%s %v: %+v", row.Transport, row.Addons, row.Verifications)
		assert.Equal(t, routerAdmitted, row.Router.Verdict)
	}
	assert.Equal(t, []string{"archive"}, rows[1].Extensions)
}

// Two plain https urls are two rows with two verdicts, whichever order they come in.
func TestHealth_TwoPlainUrlsAreCheckedApart(t *testing.T) {
	for _, deadFirst := range []bool{true, false} {
		up := newFakeUpstreams(t)
		dead := commonlib.NodeUrl{Url: up.httpURL("/d/" + upDead)}
		good := commonlib.NodeUrl{Url: up.httpURL("/d/" + upArchive)}
		urls := []commonlib.NodeUrl{good, dead}
		if deadFirst {
			urls = []commonlib.NodeUrl{dead, good}
		}
		rows := healthProbe(t, "ETH1", true, 20*time.Second, urls...)
		byURL := map[string]healthEndpointResult{}
		for i, row := range rows {
			byURL[urls[i].Url] = row
		}
		assert.False(t, byURL[dead.Url].Ok)
		assert.Equal(t, routerRefused, byURL[dead.Url].Router.Verdict)
		assert.True(t, byURL[good.Url].Ok)
		assert.Equal(t, routerAdmitted, byURL[good.Url].Router.Verdict)
		assert.Contains(t, up.methods("http", "/d/"+upDead), "eth_chainId", "the dead url was asked")
	}
}

// A failed head read fails the checks measured against the head — they are not run
// against a height that was never read — and leaves the others to answer.
func TestHealth_HeadFailureFailsTheChecksThatNeedIt(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "ETH1", true, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/e/" + upNoHead)})
	require.Len(t, rows, 1)
	row := rows[0]
	assert.False(t, row.Ok)
	assert.Equal(t, int64(-1), row.LatestBlock)
	assert.Contains(t, row.LatestBlockError, "Failed")
	checks := map[string]healthVerification{}
	for _, v := range row.Verifications {
		checks[v.Name] = v
	}
	assert.True(t, checks["chain-id"].Ok)
	assert.False(t, checks["pruning"].Ok)
	assert.Contains(t, checks["pruning"].Error, "latest block unavailable")
	assert.Equal(t, routerExcluded, row.Router.Verdict)
}

// A failed Warning check fails the row and carries its severity, and the router
// admits the url anyway.
func TestHealth_WarningSeverityIsCarried(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "SOLANA", true, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/f/" + upSolana)})
	require.Len(t, rows, 1)
	row := rows[0]
	assert.False(t, row.Ok)
	var warned bool
	for _, v := range row.Verifications {
		if !v.Ok {
			assert.Equal(t, "Warning", v.Severity, v.Name)
			warned = true
		}
	}
	assert.True(t, warned)
	assert.Equal(t, routerAdmitted, row.Router.Verdict)
}

// A check the deadline cut off says so, and so does every check after it: none reads
// as the connector's own "connector is closed".
func TestHealth_DeadlineNamesItself(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "ETH1", true, 2*time.Second, commonlib.NodeUrl{Url: up.httpURL("/g/" + upSlowChain)})
	require.Len(t, rows, 1)
	require.NotEmpty(t, rows[0].Verifications)
	for _, v := range rows[0].Verifications {
		assert.Falsef(t, v.Ok, v.Name)
		assert.Containsf(t, v.Error, "deadline exceeded", "%s: %s", v.Name, v.Error)
		assert.NotContains(t, v.Error, "connector is closed")
	}
}

// --- bounding the run ------------------------------------------------------------

func TestRunHealthProbes_CapsConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int32
	restore := stubProbe(func(context.Context, healthProvider, healthOptions) []healthEndpointResult {
		now := inFlight.Add(1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return []healthEndpointResult{okRow()}
	})
	defer restore()

	providers := make([]healthProvider, 9)
	results := runHealthProbes(context.Background(), providers, healthOptions{concurrency: 3, timeout: time.Second})
	assert.Len(t, results, 9)
	assert.LessOrEqual(t, peak.Load(), int32(3))
}

// A provider that never reports is timed out on its own deadline, however long the
// providers ahead of it in the queue took.
func TestRunHealthProbes_WedgedProviderTimesOut(t *testing.T) {
	release := make(chan struct{})
	restore := stubProbe(func(_ context.Context, p healthProvider, _ healthOptions) []healthEndpointResult {
		if p.name == "wedged" {
			<-release
		}
		return []healthEndpointResult{okRow()}
	})
	defer restore()
	defer close(release)

	providers := []healthProvider{
		{name: "fine"},
		{name: "wedged", nodeUrls: []commonlib.NodeUrl{{Url: "https://wedged.example.com"}}},
	}
	start := time.Now()
	results := runHealthProbes(context.Background(), providers, healthOptions{concurrency: 1, timeout: 10 * time.Millisecond})
	assert.Less(t, time.Since(start), 2*time.Second)
	require.Len(t, results, 2)
	assert.True(t, results[0].Ok)
	assert.False(t, results[1].Ok)
	assert.Contains(t, results[1].Error, "probe timed out")
	assert.Equal(t, routerUnknown, results[1].Router.Verdict)
}

func stubProbe(fn func(context.Context, healthProvider, healthOptions) []healthEndpointResult) (restore func()) {
	savedProbe, savedGrace := healthProbeFn, healthDeadlineGrace
	healthProbeFn, healthDeadlineGrace = fn, 10*time.Millisecond
	return func() { healthProbeFn, healthDeadlineGrace = savedProbe, savedGrace }
}
