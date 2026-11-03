//go:build !linux

package testnet

import (
	"net"
	"testing"
)

// refusedAddr frees the port: see RefusedAddr for why it cannot be held here.
func refusedAddr(t testing.TB) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testnet: listen: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("testnet: close: %v", err)
	}
	return addr
}
