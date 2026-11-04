package relaycore

import (
	"sort"
	"sync"
)

// RelayErrorTally is the router's own count of the relay attempts it classified as a node error or
// a protocol error, per chain, interface, provider and method, for GET /debug/relay-errors
// (MAG-4718). It is kept beside the Prometheus counters, not read from them: it exists on a router
// run without --metrics-listen-address, its method is the API name the request was relayed as
// rather than the metric's normalized label (which folds a GraphQL operation's root fields into a
// batch signature), and a test of the route reads what the router decided, not a metric.
//
// The two counts stay apart: a node error is a reply the router read as the node's fault, a
// protocol error an attempt that got no proper answer, and failover treats them differently. Both
// are per attempt and cumulative for the process. Safe for concurrent use; a nil tally records
// nothing and reads as empty.
type RelayErrorTally struct {
	mu   sync.Mutex
	rows map[relayErrorKey]*RelayErrorCount
}

type relayErrorKey struct {
	chainID, apiInterface, providerAddress, method string
}

// RelayErrorCount is one row of the tally.
type RelayErrorCount struct {
	ChainID         string
	ApiInterface    string
	ProviderAddress string
	Method          string
	NodeErrors      uint64
	ProtocolErrors  uint64
}

func NewRelayErrorTally() *RelayErrorTally {
	return &RelayErrorTally{rows: map[relayErrorKey]*RelayErrorCount{}}
}

// RecordNodeError counts one attempt the router read as a node error.
func (t *RelayErrorTally) RecordNodeError(chainID, apiInterface, providerAddress, method string) {
	t.add(relayErrorKey{chainID, apiInterface, providerAddress, method}, func(row *RelayErrorCount) { row.NodeErrors++ })
}

// RecordProtocolError counts one attempt that went out on the wire and got no answer.
func (t *RelayErrorTally) RecordProtocolError(chainID, apiInterface, providerAddress, method string) {
	t.add(relayErrorKey{chainID, apiInterface, providerAddress, method}, func(row *RelayErrorCount) { row.ProtocolErrors++ })
}

func (t *RelayErrorTally) add(key relayErrorKey, count func(*RelayErrorCount)) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	row, ok := t.rows[key]
	if !ok {
		row = &RelayErrorCount{ChainID: key.chainID, ApiInterface: key.apiInterface, ProviderAddress: key.providerAddress, Method: key.method}
		t.rows[key] = row
	}
	count(row)
}

// Counts returns a copy of every row, sorted by chain, interface, provider and method.
func (t *RelayErrorTally) Counts() []RelayErrorCount {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	out := make([]RelayErrorCount, 0, len(t.rows))
	for _, row := range t.rows {
		out = append(out, *row)
	}
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ChainID != b.ChainID {
			return a.ChainID < b.ChainID
		}
		if a.ApiInterface != b.ApiInterface {
			return a.ApiInterface < b.ApiInterface
		}
		if a.ProviderAddress != b.ProviderAddress {
			return a.ProviderAddress < b.ProviderAddress
		}
		return a.Method < b.Method
	})
	return out
}
