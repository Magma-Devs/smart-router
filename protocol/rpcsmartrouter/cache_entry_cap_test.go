package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	capTestTip = int64(20000000)
	// How long a skipped write is watched for, to show that no late async write lands.
	capTestNoWriteWindow = 300 * time.Millisecond
)

// recordingCacheBackend records every SetEntry, so a test can tell "the router never issued
// the write" apart from "the backend dropped it".
type recordingCacheBackend struct {
	mu   sync.Mutex
	sets []*pairingtypes.RelayCacheSet
}

func (r *recordingCacheBackend) CacheActive() bool { return true }

func (r *recordingCacheBackend) GetEntry(context.Context, *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	return nil, errors.New("recordingCacheBackend serves no lookups")
}

func (r *recordingCacheBackend) SetEntry(_ context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sets = append(r.sets, cacheSet)
	return nil
}

func (r *recordingCacheBackend) Flush(context.Context) error { return nil }
func (r *recordingCacheBackend) Close() error                { return nil }

func (r *recordingCacheBackend) recorded() []*pairingtypes.RelayCacheSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*pairingtypes.RelayCacheSet(nil), r.sets...)
}

// capFixture is an ETH1 router writing eth_blockNumber replies into backend under a cap.
type capFixture struct {
	rpcss   *RPCSmartRouterServer
	msg     chainlib.ProtocolMessage
	hashKey []byte
}

func newCapFixture(t *testing.T, backend performance.CacheBackend, maxEntryBytes int64) capFixture {
	t.Helper()
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, nil)
	rpcss.cache = backend
	rpcss.cacheMaxEntryBytes = maxEntryBytes
	rpcss.smartRouterEndpointMetrics = metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, capTestTip)
	hashKey, _, err := msg.HashCacheRequest("ETH1")
	require.NoError(t, err)
	return capFixture{rpcss: rpcss, msg: msg, hashKey: hashKey}
}

func (f capFixture) write(body []byte) {
	f.rpcss.tryCacheWrite(context.Background(), f.msg, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: body, LatestBlock: capTestTip},
		StatusCode: http.StatusOK,
	})
}

// replyOfSize is an eth_blockNumber reply padded to exactly size bytes.
func replyOfSize(t *testing.T, size int) []byte {
	t.Helper()
	const head, tail = `{"jsonrpc":"2.0","id":1,"result":"0x1312d00","pad":"`, `"}`
	require.Greater(t, size, len(head)+len(tail))
	return []byte(head + strings.Repeat("a", size-len(head)-len(tail)) + tail)
}

// cacheWriteSeries reads the ETH1 eth_blockNumber cache-write series from the default
// registry: the size-skip counter and the entry-size histogram's count and sum.
func cacheWriteSeries(t *testing.T) (skipped float64, written uint64, writtenBytes float64) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["spec"] != "ETH1" || labels["apiInterface"] != spectypes.APIInterfaceJsonRPC || labels["method"] != "eth_blockNumber" {
				continue
			}
			switch family.GetName() {
			case "smartrouter_cache_write_skipped_total":
				if labels["reason"] == metrics.CacheWriteSkipReasonSize {
					skipped = metric.GetCounter().GetValue()
				}
			case "smartrouter_cache_entry_bytes":
				written = metric.GetHistogram().GetSampleCount()
				writtenBytes = metric.GetHistogram().GetSampleSum()
			}
		}
	}
	return skipped, written, writtenBytes
}

// (a) A reply over the cap is never handed to the backend, and is counted once.
func TestCacheWriteSkipsReplyOverEntryCap(t *testing.T) {
	backend := &recordingCacheBackend{}
	f := newCapFixture(t, backend, common.DefaultCacheMaxEntryBytes)
	skippedBefore, writtenBefore, _ := cacheWriteSeries(t)

	f.write(replyOfSize(t, 2<<20))

	skipped, written, _ := cacheWriteSeries(t)
	require.Equal(t, skippedBefore+1, skipped, "the skip is counted once, with reason=size")
	require.Equal(t, writtenBefore, written, "a skipped entry is not observed as written")
	require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
		"no SetEntry reaches the backend")
}

// The cap is inclusive: a body of exactly the cap is written, one byte more is not.
func TestCacheWriteEntryCapBoundary(t *testing.T) {
	const maxEntryBytes = 4096
	backend := &recordingCacheBackend{}
	f := newCapFixture(t, backend, maxEntryBytes)
	skippedBefore, _, _ := cacheWriteSeries(t)

	f.write(replyOfSize(t, maxEntryBytes+1))
	f.write(replyOfSize(t, maxEntryBytes))

	require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond)
	require.Len(t, backend.recorded()[0].GetResponse().GetData(), maxEntryBytes)
	skipped, _, _ := cacheWriteSeries(t)
	require.Equal(t, skippedBefore+1, skipped)
}

// (b) A reply under the cap is written exactly as before, through a real cache server.
func TestCacheWriteUnderEntryCapLandsInCache(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	f := newCapFixture(t, primary, common.DefaultCacheMaxEntryBytes)
	body := replyOfSize(t, 100<<10)
	skippedBefore, writtenBefore, bytesBefore := cacheWriteSeries(t)

	f.write(body)

	skipped, written, writtenBytes := cacheWriteSeries(t)
	require.Equal(t, skippedBefore, skipped)
	require.Equal(t, writtenBefore+1, written)
	require.Equal(t, bytesBefore+float64(len(body)), writtenBytes, "the histogram observes the body size")
	var cached *pairingtypes.RelayReply
	require.Eventually(t, func() bool {
		cached = directGetOn(rcs, "ETH1", f.hashKey, capTestTip, capTestTip).GetReply()
		return cached != nil
	}, 3*time.Second, 20*time.Millisecond, "the entry is written")
	require.Equal(t, body, cached.Data)
}

// (c) A cap of 0 is no cap: a reply of any size is handed to the backend.
func TestCacheWriteCapZeroWritesLargeReply(t *testing.T) {
	backend := &recordingCacheBackend{}
	f := newCapFixture(t, backend, 0)
	body := replyOfSize(t, 2<<20)
	skippedBefore, _, _ := cacheWriteSeries(t)

	f.write(body)

	require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, body, backend.recorded()[0].GetResponse().GetData())
	skipped, _, _ := cacheWriteSeries(t)
	require.Equal(t, skippedBefore, skipped)
}

// (d) The RESP backend honours the cap. The cap-0 case stores the same 2 MiB entry, so the
// skip is the cap's doing and not a backend limit.
func TestCacheWriteEntryCapOnRespBackend(t *testing.T) {
	cases := []struct {
		name          string
		maxEntryBytes int64
		size          int
		wantWritten   bool
	}{
		{name: "over the cap is skipped", maxEntryBytes: common.DefaultCacheMaxEntryBytes, size: 2 << 20, wantWritten: false},
		{name: "under the cap is written", maxEntryBytes: common.DefaultCacheMaxEntryBytes, size: 100 << 10, wantWritten: true},
		{name: "cap 0 writes a large reply", maxEntryBytes: 0, size: 2 << 20, wantWritten: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store, err := redisstore.New(redisstore.Config{Addresses: []string{server.Addr()}})
			require.NoError(t, err)
			respCache := performance.NewRespCache(store, core.DefaultPolicy())
			t.Cleanup(func() { _ = respCache.Close() })

			f := newCapFixture(t, respCache, tc.maxEntryBytes)
			body := replyOfSize(t, tc.size)
			skippedBefore, _, _ := cacheWriteSeries(t)

			f.write(body)

			skipped, _, _ := cacheWriteSeries(t)
			if !tc.wantWritten {
				require.Equal(t, skippedBefore+1, skipped)
				require.Never(t, func() bool { return len(server.Keys()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
					"nothing reaches the RESP backend, not even the chain tip")
				return
			}
			require.Equal(t, skippedBefore, skipped)
			var cached []byte
			require.Eventually(t, func() bool {
				reply, getErr := respCache.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
					RequestHash:    f.hashKey,
					ChainId:        "ETH1",
					RequestedBlock: capTestTip,
					SeenBlock:      capTestTip,
				})
				cached = reply.GetReply().GetData()
				return getErr == nil && cached != nil
			}, 3*time.Second, 20*time.Millisecond, "the entry is written")
			require.Equal(t, body, cached)
		})
	}
}

// The skip path allocates a small constant per call, whatever the body size: nothing copies
// or encodes the body before the check.
func TestCacheWriteSkipCopiesNoBody(t *testing.T) {
	backend := &recordingCacheBackend{}
	f := newCapFixture(t, backend, common.DefaultCacheMaxEntryBytes)
	const calls = 50
	for _, size := range []int{2 << 20, 8 << 20} {
		body := replyOfSize(t, size)
		f.write(body) // warm up label caches and lazily built state

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for i := 0; i < calls; i++ {
			f.write(body)
		}
		runtime.ReadMemStats(&after)

		perCall := (after.TotalAlloc - before.TotalAlloc) / calls
		require.Less(t, perCall, uint64(64<<10), "a %d-byte body allocated %d bytes per skipped write", size, perCall)
	}
	require.Empty(t, backend.recorded())
}

// The flag is registered on the shipped command with its default, reads from the command
// line and the config file, and refuses what is not a byte count.
func TestCacheMaxEntryBytesFlag(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		yaml    string
		want    int64
		wantErr bool
	}{
		{name: "default", want: 1 << 20},
		{name: "flag", args: []string{"--cache-max-entry-bytes=4194304"}, want: 4 << 20},
		{name: "flag zero is no cap", args: []string{"--cache-max-entry-bytes=0"}, want: 0},
		{name: "config file", yaml: "cache-max-entry-bytes: 262144\n", want: 256 << 10},
		{name: "negative", args: []string{"--cache-max-entry-bytes=-1"}, wantErr: true},
		{name: "config file unit suffix", yaml: "cache-max-entry-bytes: 1MiB\n", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			cmd := CreateRPCSmartRouterCobraCommand()
			flag := cmd.Flags().Lookup(common.CacheMaxEntryBytesFlagName)
			require.NotNil(t, flag, "the shipped command registers --%s", common.CacheMaxEntryBytesFlagName)
			require.Equal(t, "1048576", flag.DefValue)

			require.NoError(t, cmd.Flags().Parse(tc.args))
			require.NoError(t, viper.BindPFlags(cmd.Flags()))
			if tc.yaml != "" {
				path := filepath.Join(t.TempDir(), "smartrouter.yml")
				require.NoError(t, os.WriteFile(path, []byte(tc.yaml), 0o600))
				viper.SetConfigFile(path)
				require.NoError(t, viper.ReadInConfig())
			}

			got, err := cacheMaxEntryBytesFrom(viper.GetViper())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
