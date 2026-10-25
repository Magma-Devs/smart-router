package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3080: lava-providers-block used to silently drop the whole list at three or more names.

func blockedFromHeader(value string) []string {
	return NewProtocolMessage(nil, map[string]string{common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: value}, nil, "", "").GetBlockedProviders()
}

func TestGetBlockedProviders_ThreeOrMoreAreAllHonoured(t *testing.T) {
	require.Equal(t, []string{"a", "b", "c"}, blockedFromHeader("a,b,c"))
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, blockedFromHeader("a,b,c,d,e"))
}

func TestGetBlockedProviders_OneAndTwoUnchanged(t *testing.T) {
	require.Equal(t, []string{"a"}, blockedFromHeader("a"))
	require.Equal(t, []string{"a", "b"}, blockedFromHeader("a,b"))
}

// "a, b" used to exclude "a" and " b" — a name that matches nothing — so b still served.
func TestGetBlockedProviders_EntriesAreTrimmedAndBlanksSkipped(t *testing.T) {
	require.Equal(t, []string{"a", "b", "c"}, blockedFromHeader(" a, b ,c "))
	require.Equal(t, []string{"a", "b"}, blockedFromHeader("a,,b,"))
	require.Empty(t, blockedFromHeader(""))
	require.Empty(t, blockedFromHeader(" , "))
}

func TestGetBlockedProviders_NoHeaderBlocksNothing(t *testing.T) {
	require.Nil(t, NewProtocolMessage(nil, map[string]string{}, nil, "", "").GetBlockedProviders())
	require.Nil(t, NewProtocolMessage(nil, nil, nil, "", "").GetBlockedProviders())
}
