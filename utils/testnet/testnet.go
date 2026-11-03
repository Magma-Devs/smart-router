// Package testnet holds loopback-network helpers for tests.
package testnet

import "testing"

// RefusedAddr returns a loopback address that refuses every dial with ECONNREFUSED.
//
// On Linux the port stays bound, for the rest of the test, to a socket that never listens: the
// kernel answers a connection to it with a reset, and nothing else can bind the port. Listening on
// :0 and closing the listener would free the port instead, and any listener on the machine could be
// handed it before the dial.
//
// Elsewhere a bound socket that never listens does not refuse: macOS drops the SYN, and the dial
// waits out its timeout. There the port is freed as before, and the race above is accepted. CI runs
// on Linux.
func RefusedAddr(t testing.TB) string {
	t.Helper()
	return refusedAddr(t)
}
