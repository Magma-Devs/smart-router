package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/assert"
)

// MAG-4448. ShouldRetryError re-reads a wrapped error's text, which carries the verdict's
// Description, so a Description holding a phrase a text row matches can flip the retry. Each
// verdict the bitcoind -8 and -5 rows give, wrapped around a 500 as the JSON-RPC sender wraps it,
// keeps the retry its code says: another provider is asked.
func TestBitcoindRowVerdicts_KeepTheirRetryOnceWrapped(t *testing.T) {
	for _, verdict := range []*common.RouterError{
		common.RouterErrorChainBlockNotFound,
		common.RouterErrorChainBitcoinInvalidParameter,
		common.RouterErrorNodeDataNotHeld,
		common.RouterErrorChainTxNotFound,
		common.RouterErrorChainBitcoinInvalidAddressOrKey,
	} {
		t.Run(verdict.Name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 500, Status: "500 Internal Server Error", Body: []byte(`{"result":null,"error":{"code":-5,"message":"x"},"id":7}`)}
			wrapped := common.NewRouterErrorWrapping(verdict, httpErr)

			assert.True(t, verdict.Retryable)
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay goes on to another provider")
			assert.False(t, chainlib.IsUnsupportedMethodError(wrapped))
		})
	}
}
