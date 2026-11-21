package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Only a request the client sent as a GET URI is sent to the node as that GET (MAG-5267). The
// router's own crafted requests carry a Path too, and stay JSON-RPC.
func TestTendermintrpcMessage_URIRequest(t *testing.T) {
	uri, ok := (&TendermintrpcMessage{Path: "block?height=5", FromURI: true}).URIRequest()
	require.True(t, ok)
	require.Equal(t, "block?height=5", uri)

	uri, ok = (&TendermintrpcMessage{Path: "status?", FromURI: true}).URIRequest()
	require.True(t, ok)
	require.Equal(t, "status", uri, "the lone ? the listener adds is not the client's")

	_, ok = (&TendermintrpcMessage{Path: "status"}).URIRequest()
	require.False(t, ok, "a crafted request is not a client GET")

	var none *TendermintrpcMessage
	_, ok = none.URIRequest()
	require.False(t, ok)
}
