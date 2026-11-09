package relaycore

import (
	"sort"
	"strings"
	"sync"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
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
//
// The rows are bounded (see boundedMethodLocked): two kinds of method name are the request's own
// text, and each kind gets a budget per chain and interface, with the method label's numbers.
type RelayErrorTally struct {
	mu   sync.Mutex
	rows map[relayErrorKey]*RelayErrorCount
	// admitted holds, per chain and interface, the unbounded names that have their own row.
	admitted map[tallyScope]*admittedNames
}

type relayErrorKey struct {
	chainID, apiInterface, providerAddress, method string
}

type tallyScope struct {
	chainID, apiInterface string
}

type admittedNames struct {
	defaults, batches map[string]struct{}
}

// maxTallyMethodNameBytes bounds the size of an unbounded name that may keep its own row. A batch
// names every element, so one large batch would otherwise pin a name as long as its element list
// for the life of the process. Declared methods are spec names and never come near it.
const maxTallyMethodNameBytes = 512

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
	return &RelayErrorTally{rows: map[relayErrorKey]*RelayErrorCount{}, admitted: map[tallyScope]*admittedNames{}}
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
	key.method = t.boundedMethodLocked(key.chainID, key.apiInterface, key.method)
	row, ok := t.rows[key]
	if !ok {
		row = &RelayErrorCount{ChainID: key.chainID, ApiInterface: key.apiInterface, ProviderAddress: key.providerAddress, Method: key.method}
		t.rows[key] = row
	}
	count(row)
}

// boundedMethodLocked returns the method a row is kept under. That is the API name as relayed, and
// for two kinds of name it is the request's own text: a "Default-<...>" name the parser mints for an
// api the spec does not declare, and a name joined with chainlib.SEP (a batch's elements, or a
// GraphQL operation's root fields). Left alone, every distinct one adds a row for the life of the
// process (review 626-3: 200 unknown REST paths gave 200 rows). So each kind gets a budget per chain
// and interface, with the method label's numbers (metrics.MaxDefaultMethodsPerSpec,
// MaxBatchSignaturesPerSpec; the label's own budget is per chain, and it counts a path's shape and a
// batch's set of methods rather than the name), and past it, or past maxTallyMethodNameBytes, the
// attempt is counted on that kind's overflow row, under the label's overflow name. Admission is
// monotone, as the label's is: an admitted name keeps its row, and a full budget stays full.
// Declared methods are a spec-sized set and keep their names.
// The caller holds t.mu.
func (t *RelayErrorTally) boundedMethodLocked(chainID, apiInterface, method string) string {
	batch := strings.Contains(method, chainlib.SEP)
	if !batch && !strings.HasPrefix(method, chainlib.DefaultApiName) {
		return method
	}
	budget, overflow := metrics.MaxDefaultMethodsPerSpec, metrics.DefaultMethodOther
	if batch {
		budget, overflow = metrics.MaxBatchSignaturesPerSpec, metrics.BatchMethodOther
	}
	if method == overflow || len(method) > maxTallyMethodNameBytes {
		return overflow
	}
	if t.admitted == nil {
		t.admitted = map[tallyScope]*admittedNames{}
	}
	scope := tallyScope{chainID, apiInterface}
	names := t.admitted[scope]
	if names == nil {
		names = &admittedNames{defaults: map[string]struct{}{}, batches: map[string]struct{}{}}
		t.admitted[scope] = names
	}
	set := names.defaults
	if batch {
		set = names.batches
	}
	if _, ok := set[method]; ok {
		return method
	}
	if len(set) >= budget {
		return overflow
	}
	set[method] = struct{}{}
	return method
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
