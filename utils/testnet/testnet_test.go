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
