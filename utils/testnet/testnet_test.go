//go:build unix

package testnet

import (
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRefusedAddrRefusesTheDial(t *testing.T) {
	addr := RefusedAddr(t)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if conn != nil {
		_ = conn.Close()
	}
	require.ErrorIs(t, err, syscall.ECONNREFUSED)
}

// The point of holding the port: a listener started anywhere else cannot take it.
func TestRefusedAddrKeepsThePort(t *testing.T) {
	addr := RefusedAddr(t)
	lis, err := net.Listen("tcp", addr)
	if lis != nil {
		_ = lis.Close()
	}
	require.ErrorIs(t, err, syscall.EADDRINUSE)
}
