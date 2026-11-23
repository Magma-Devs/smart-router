package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// TestCacheWriteSkipsANotFoundAnswer (MAG-5121 P6): a record no provider held is served as an
// answer, but never written to the cache, so a provider that holds it later is asked again. The
// same reply without the mark is written as before.
func TestCacheWriteSkipsANotFoundAnswer(t *testing.T) {
	const block = int64(20000000)
	request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}`, block)
	reply := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x%x","transactions":[]}}`, block)

	for _, notFound := range []bool{false, true} {
		t.Run(fmt.Sprintf("not-found answer %v", notFound), func(t *testing.T) {
			chainParser := ethJsonRPCParser(t)
			backend := &recordingCacheBackend{}
			rpcss := ethCacheTestServer(chainParser, nil)
			rpcss.cache = backend
			msg := ethProtocolMessage(t, chainParser, request, 0)

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:            &pairingtypes.RelayReply{Data: []byte(reply), LatestBlock: block},
				StatusCode:       http.StatusOK,
				IsNotFoundAnswer: notFound,
			})
			if !notFound {
				require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond, "the control is written")
				return
			}
			require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
				"a not-found answer must not reach the cache")
		})
	}
}
