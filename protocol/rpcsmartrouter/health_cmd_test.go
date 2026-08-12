package rpcsmartrouter

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
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

// The endpoint `ok` is specValid AND every verification ok, regardless of "severity"
// (which we deliberately don't surface). A failed verification at any level drags ok down.
func TestEndpointOk_AnyFailedVerificationDragsDown(t *testing.T) {
	// Mirror the rollup probeProvider applies to a node-url's verification set.
	verifications := []healthVerification{
		{Name: "chain-id", Ok: true},
		{Name: "archive", Extension: "archive", Ok: false},
	}
	allOk := true
	for _, v := range verifications {
		if !v.Ok {
			allOk = false
		}
	}
	assert.False(t, allOk, "one failed verification must make the endpoint not ok")
}

// applyValidation maps one node-URL's spec results into its row. This guards the
// regression where two node-urls sharing the same URL string (a base URL and an
// addons:[archive] URL) were matched by url-string into a map and collided — the archive
// row must carry its OWN block + verifications, not the base row's (or vice versa).
func TestApplyValidation_DistinctPerNodeURL(t *testing.T) {
	baseRow := healthEndpointResult{URL: "https://x", Addons: []string{}, Verifications: []healthVerification{}}
	archiveRow := healthEndpointResult{URL: "https://x", Addons: []string{"archive"}, Verifications: []healthVerification{}}

	baseV := chainlib.NodeURLValidation{
		URL:         "https://x",
		LatestBlock: 5324700,
		Verifications: []chainlib.VerificationResult{
			{Name: "chain-id", Ok: true},
			{Name: "pruning", Ok: true},
		},
	}
	archiveV := chainlib.NodeURLValidation{
		URL:         "https://x", // same UrlStr as base — would collide in a url-keyed map
		LatestBlock: 0,
		Verifications: []chainlib.VerificationResult{
			{Name: "pruning", Extension: "archive", Ok: true},
		},
	}

	applyValidation(&baseRow, baseV)
	applyValidation(&archiveRow, archiveV)

	// Each row reflects its own validation, not the other's.
	assert.Equal(t, int64(5324700), baseRow.LatestBlock)
	assert.Len(t, baseRow.Verifications, 2)
	assert.Empty(t, baseRow.Extensions)

	assert.Equal(t, int64(0), archiveRow.LatestBlock)
	assert.Len(t, archiveRow.Verifications, 1)
	assert.Equal(t, []string{"archive"}, archiveRow.Extensions)
	assert.True(t, archiveRow.Ok)
}

func TestApplyValidation_FailedCheckSetsNotOk(t *testing.T) {
	row := healthEndpointResult{Verifications: []healthVerification{}}
	applyValidation(&row, chainlib.NodeURLValidation{
		Verifications: []chainlib.VerificationResult{
			{Name: "pruning", Extension: "archive", Ok: false, Error: "expected and received are different"},
		},
	})
	assert.False(t, row.Ok)
	assert.Equal(t, []string{"archive"}, row.Extensions)
	assert.Equal(t, "expected and received are different", row.Verifications[0].Error)
}

// MAG-2333: legs that passed every check reported latestBlock 0 whenever the spec had no
// verification needing a LatestDistance, which reads as a dead node in the JSON report.
func TestBackfillLatestBlock_FillsOnlyMissingAndFetchesOnce(t *testing.T) {
	rows := []healthEndpointResult{
		{URL: "https://a", LatestBlock: 0, Ok: true},
		{URL: "https://b", LatestBlock: 5324700, Ok: true}, // already has a height — must not be touched
		{URL: "https://c", LatestBlock: 0, Ok: true},
	}

	calls := 0
	backfillLatestBlock(rows, func() (int64, error) {
		calls++
		return 9000001, nil
	})

	assert.Equal(t, 1, calls, "the block must be fetched once per endpoint, not once per row")
	assert.Equal(t, int64(9000001), rows[0].LatestBlock)
	assert.Equal(t, int64(5324700), rows[1].LatestBlock, "a row that already reported a height must keep it")
	assert.Equal(t, int64(9000001), rows[2].LatestBlock)
}

func TestBackfillLatestBlock_NoFetchWhenNothingMissing(t *testing.T) {
	rows := []healthEndpointResult{{URL: "https://a", LatestBlock: 42, Ok: true}}

	calls := 0
	backfillLatestBlock(rows, func() (int64, error) {
		calls++
		return 9000001, nil
	})

	assert.Zero(t, calls, "a spec that already reports a height must not cost an extra relay")
	assert.Equal(t, int64(42), rows[0].LatestBlock)
}

// A leg that failed its checks must not advertise a height. fetch routes through the
// router spanning every probed URL, so the value it returns says nothing about the URL
// on a failed row — printing it there reads as if that URL were reachable.
func TestBackfillLatestBlock_SkipsFailedRows(t *testing.T) {
	rows := []healthEndpointResult{
		{URL: "https://dead", LatestBlock: 0, Ok: false},
		{URL: "https://live", LatestBlock: 0, Ok: true},
	}

	backfillLatestBlock(rows, func() (int64, error) { return 9000001, nil })

	assert.Zero(t, rows[0].LatestBlock, "a failed leg must not report a height fetched via another URL")
	assert.Equal(t, int64(9000001), rows[1].LatestBlock, "a passing leg is still backfilled")
}

// The fetch is skipped entirely when every row that lacks a height also failed, so a
// fully-dead endpoint costs no extra relay.
func TestBackfillLatestBlock_NoFetchWhenOnlyFailedRowsAreMissing(t *testing.T) {
	rows := []healthEndpointResult{{URL: "https://dead", LatestBlock: 0, Ok: false}}

	calls := 0
	backfillLatestBlock(rows, func() (int64, error) {
		calls++
		return 9000001, nil
	})

	assert.Zero(t, calls, "no passing row needs a height, so nothing should be fetched")
	assert.Zero(t, rows[0].LatestBlock)
}

// A failed fetch must stay invisible: latestBlock is a reporting field and cannot be
// allowed to flip any leg's ok verdict.
func TestBackfillLatestBlock_FetchFailureLeavesRowsAlone(t *testing.T) {
	rows := []healthEndpointResult{{URL: "https://a", LatestBlock: 0, Ok: true}}

	backfillLatestBlock(rows, func() (int64, error) {
		return 0, assert.AnError
	})

	assert.Equal(t, int64(0), rows[0].LatestBlock)
	assert.True(t, rows[0].Ok, "a reporting-only fetch failure must not change the verdict")
}

func TestTransportForURL(t *testing.T) {
	cases := map[string]string{
		"https://eth1.lava.build":         "http",
		"http://127.0.0.1:8545":           "http",
		"wss://eth1.lava.build/websocket": "ws",
		"ws://localhost:8546":             "ws",
		"WSS://EthNode/Path":              "ws",
		"grpc.example.com:443":            "other",
		"127.0.0.1:9090":                  "other",
	}
	for url, want := range cases {
		assert.Equalf(t, want, transportForURL(url), "transport for %q", url)
	}
}

// Stdout must be a single, parseable JSON document with a stable shape.
func TestHealthReport_JSONShape(t *testing.T) {
	report := buildHealthReport([]healthEndpointResult{
		{
			Name: "eth-lava", ChainID: "ETH1", APIInterface: "jsonrpc",
			URL: "wss://eth1.lava.build/websocket", Transport: "ws",
			Addons: []string{}, Extensions: []string{"archive"},
			SpecValid: true, LatestBlock: 2030011, Ok: false,
			Verifications: []healthVerification{
				{Name: "chain-id", Ok: true},
				{Name: "subscribe", Extension: "websocket", Ok: false, Error: "ws handshake failed"},
			},
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

	results := decoded["results"].([]any)
	require.Len(t, results, 1)
	row := results[0].(map[string]any)
	for _, key := range []string{"name", "chainId", "apiInterface", "url", "transport", "addons", "extensions", "specValid", "latestBlock", "ok", "verifications"} {
		assert.Containsf(t, row, key, "result row must carry %q", key)
	}
	assert.Equal(t, "ws", row["transport"])
	assert.Equal(t, false, row["ok"])
}

func TestProviderHasWebSocketURL(t *testing.T) {
	nu := func(u string) commonlib.NodeUrl { return commonlib.NodeUrl{Url: u} }
	assert.True(t, providerHasWebSocketURL([]commonlib.NodeUrl{nu("https://x"), nu("wss://x/ws")}))
	assert.False(t, providerHasWebSocketURL([]commonlib.NodeUrl{nu("https://x")}))
	assert.False(t, providerHasWebSocketURL(nil))
}

func TestAllURLsAreWebSocket(t *testing.T) {
	nu := func(u string) commonlib.NodeUrl { return commonlib.NodeUrl{Url: u} }
	// ws-only -> true (cannot construct a router: no http base collection)
	assert.True(t, allURLsAreWebSocket([]commonlib.NodeUrl{nu("wss://x/ws"), nu("ws://y")}))
	// any http present -> false
	assert.False(t, allURLsAreWebSocket([]commonlib.NodeUrl{nu("https://x"), nu("wss://x/ws")}))
	assert.False(t, allURLsAreWebSocket([]commonlib.NodeUrl{nu("https://x")}))
	// empty -> false (nothing to flag)
	assert.False(t, allURLsAreWebSocket(nil))
}

func TestNonNilStrings(t *testing.T) {
	assert.Equal(t, []string{}, nonNilStrings(nil))
	assert.Equal(t, []string{"a"}, nonNilStrings([]string{"a"}))
}

func TestSortedKeys(t *testing.T) {
	got := sortedKeys(map[string]struct{}{"archive": {}, "debug": {}, "": {}})
	assert.Equal(t, []string{"", "archive", "debug"}, got)
}
