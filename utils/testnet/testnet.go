//go:build unix

// Package testnet holds loopback-network helpers for tests.
package testnet

import (
	"net"
	"strconv"
	"syscall"
	"testing"
)

// RefusedAddr returns a loopback address that refuses every dial, for the rest of the test.
//
// The port stays bound to a socket that never listens: the kernel answers a connection to it with a
// reset, and nothing else can bind the port. Listening on :0 and closing the listener would free the
// port instead, and any listener on the machine could be handed it before the dial.
func RefusedAddr(t testing.TB) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("testnet: socket: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatalf("testnet: bind: %v", err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatalf("testnet: getsockname: %v", err)
	}
	in4, ok := sa.(*syscall.SockaddrInet4)
	if !ok {
		t.Fatalf("testnet: getsockname returned %T, not an IPv4 address", sa)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(in4.Port))
}
