package testnet

import (
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// The point of holding the port: a listener started anywhere else cannot take it.
func TestRefusedAddrKeepsThePort(t *testing.T) {
	addr := RefusedAddr(t)
	lis, err := net.Listen("tcp", addr)
	if lis != nil {
		_ = lis.Close()
	}
	require.ErrorIs(t, err, syscall.EADDRINUSE)
}
