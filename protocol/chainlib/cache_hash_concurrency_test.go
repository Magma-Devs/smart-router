package chainlib

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/cacheformat"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils/sigs"
	"github.com/stretchr/testify/require"
)

// cacheKeyFixture is a request with every field the key clears or keeps populated.
func cacheKeyFixture() *pairingtypes.RelayPrivateData {
	return &pairingtypes.RelayPrivateData{
		ConnectionType: "POST",
		Data:           []byte(`{"jsonrpc":"2.0","id":"abc","method":"eth_getBalance","params":["0x0123","0x10"]}`),
		RequestBlock:   16,
		ApiInterface:   "jsonrpc",
		Salt:           []byte{9, 8, 7},
		Metadata: []pairingtypes.Metadata{
			{Name: "x-second", Value: "2"},
			{Name: "content-type", Value: "application/json"},
			{Name: "x-first", Value: "1"},
		},
		Extensions: []string{"archive"},
		SeenBlock:  20,
		RequestId:  "req-1",
		XTaskId:    &pairingtypes.RelayPrivateData_TaskId{TaskId: "task-1"},
	}
}

// historicalCacheKey is the key as hashCacheRequest computed it before MAG-4242 item 3: mutate the
// request in place, marshal, restore on defer. It is kept so the copy-based computation is proven
// to yield the same bytes, which keeps every cache entry written before the change findable.
func historicalCacheKey(t *testing.T, relayData *pairingtypes.RelayPrivateData, chainId, explicitExtensionDirective string) []byte {
	t.Helper()
	originalData, originalSalt, originalRequestBlock, originalSeenBlock := relayData.Data, relayData.Salt, relayData.RequestBlock, relayData.SeenBlock
	originalRequestId, originalTaskId, originalTxId, originalMetadata := relayData.RequestId, relayData.XTaskId, relayData.XTxId, relayData.Metadata
	defer func() {
		relayData.Data, relayData.Salt, relayData.RequestBlock, relayData.SeenBlock = originalData, originalSalt, originalRequestBlock, originalSeenBlock
		relayData.RequestId, relayData.XTaskId, relayData.XTxId, relayData.Metadata = originalRequestId, originalTaskId, originalTxId, originalMetadata
	}()
	inputFormatter, _ := cacheformat.FormatterForRelayRequestAndResponse(relayData.ApiInterface)
	relayData.Data = inputFormatter(relayData.Data)
	relayData.Salt = nil
	relayData.SeenBlock = 0
	relayData.RequestId = ""
	relayData.XTaskId = nil
	relayData.XTxId = nil
	relayData.Metadata = metadataForCacheKey(relayData.Metadata)
	relayData.RequestBlock = 0
	raw, err := json.Marshal(&pairingtypes.CacheHash{Request: relayData, ChainId: chainId})
	require.NoError(t, err)
	if explicitExtensionDirective != "" {
		raw = append(raw, []byte("\x00lava-extension="+explicitExtensionDirective)...)
	}
	return sigs.HashMsg(raw)
}

// TestHashCacheRequest_KeyMatchesTheHistoricalAlgorithm pins that computing the key on a copy
// (MAG-4242 item 3) changes no key, with and without an explicit extension directive.
func TestHashCacheRequest_KeyMatchesTheHistoricalAlgorithm(t *testing.T) {
	for name, directive := range map[string]string{"no directive": "", "explicit archive": "archive"} {
		t.Run(name, func(t *testing.T) {
			want := historicalCacheKey(t, cacheKeyFixture(), "ETH1", directive)
			got, _, err := hashCacheRequest(cacheKeyFixture(), "ETH1", directive)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestHashCacheRequest_LeavesTheRequestUntouched: the key is computed on a copy, so the request is
// the same object before and after, field for field, not merely restored after the fact.
func TestHashCacheRequest_LeavesTheRequestUntouched(t *testing.T) {
	relayData := cacheKeyFixture()
	_, _, err := hashCacheRequest(relayData, "ETH1", "archive")
	require.NoError(t, err)
	require.Equal(t, cacheKeyFixture(), relayData)
}

// TestHashCacheRequest_IsSafeForConcurrentCallers reproduces MAG-4242 item 3 under -race: the cache
// lookup (state-machine goroutine) and the cache write (attempt goroutine) hash the same request,
// and the mutate-and-restore computation wrote to it from both. The key is computed on a copy now,
// so the callers only read the request, and every caller gets the same key.
func TestHashCacheRequest_IsSafeForConcurrentCallers(t *testing.T) {
	relayData := cacheKeyFixture()
	want, _, err := hashCacheRequest(relayData, "ETH1", "")
	require.NoError(t, err)

	const callers = 8
	keys := make([][]byte, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys[i], _, errs[i] = hashCacheRequest(relayData, "ETH1", "")
		}()
	}
	wg.Wait()
	for i := range callers {
		require.NoError(t, errs[i])
		require.Equal(t, want, keys[i])
	}
	require.Equal(t, cacheKeyFixture(), relayData, "a reader must leave the request as it found it")
}

// TestProtocolMessage_HashCacheRequestIsComputedOnce: the lookup and the write of one relay must
// agree on the key, so the protocol message computes it once and hands that key to every caller,
// however many goroutines ask. A different chain id is answered fresh, never with another chain's key.
func TestProtocolMessage_HashCacheRequestIsComputedOnce(t *testing.T) {
	protocolMessage := NewProtocolMessage(nil, map[string]string{common.EXTENSION_OVERRIDE_HEADER_NAME: "archive"}, cacheKeyFixture(), "dapp", "127.0.0.1")
	first, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	require.NotEmpty(t, first)

	const callers = 8
	keys := make([][]byte, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys[i], _, _ = protocolMessage.HashCacheRequest("ETH1")
		}()
	}
	wg.Wait()
	for i := range callers {
		require.Equal(t, first, keys[i])
		require.True(t, &keys[i][0] == &first[0], "the key is computed once and shared, not recomputed per caller")
	}

	other, _, err := protocolMessage.HashCacheRequest("OTHER")
	require.NoError(t, err)
	require.NotEqual(t, first, other)
}
