//go:build race

package rpcInterfaceMessages

// raceEnabled reports whether the tests run under the race detector, where sync.Pool drops one Put
// in four: goccy/go-json keeps its input buffer in a pool, so about one reading in four copies the
// whole body again, and a per-reading memory bound no longer measures the reader.
const raceEnabled = true
