package chainlib

import (
	"testing"

	"github.com/stretchr/testify/require"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// testing hash cache request doesn't change the fields it resets to calculate hash.
func TestHashCacheRequest(t *testing.T) {
	var initSeenBlock int64 = 99
	var initRequestBlock int64 = 100
	initData := []byte{1, 2, 3, 4}
	initSalt := []byte{1, 3, 4, 5}

	relayRequest := &pairingtypes.RelayPrivateData{Data: initData, Salt: initSalt, RequestBlock: initRequestBlock, SeenBlock: initSeenBlock}
	_, _, err := HashCacheRequest(relayRequest, "Best-Chain-AKA-Router-Network")
	require.NoError(t, err)
	require.Equal(t, relayRequest.SeenBlock, initSeenBlock)
	require.Equal(t, relayRequest.RequestBlock, initRequestBlock)
	require.Equal(t, relayRequest.Data, initData)
	require.Equal(t, relayRequest.Salt, initSalt)
}
