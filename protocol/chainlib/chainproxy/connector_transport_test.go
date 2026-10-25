package chainproxy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorTransport(t *testing.T) {
	require.Equal(t, "WebSocket", connectorTransport("wss://eth.example.com/ws"))
	require.Equal(t, "WebSocket", connectorTransport("WS://127.0.0.1:8546"))
	require.Equal(t, "HTTP", connectorTransport("https://eth.example.com"))
	require.Equal(t, "HTTP", connectorTransport("http://127.0.0.1:8545"))
}
