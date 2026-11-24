package chainlib

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// errRelayPathNotUsed is what recordingRelaySender answers on a path its test gave it nothing
// for, so a listener that takes a path the test did not expect fails loudly.
var errRelayPathNotUsed = errors.New("not used")

// relayCall is one call a listener made to its RelaySender: the method, its arguments, the
// caller ids on its context when it was made, and what the call answered.
type relayCall struct {
	method         string // "SendRelay", "ParseRelay" or "SendParsedRelay"
	ctx            context.Context
	tracing        tracingIds
	url            string
	req            string
	connectionType string
	dappID         string
	consumerIp     string
	metadata       []pairingtypes.Metadata
	parsed         ProtocolMessage // what SendParsedRelay was handed
	err            error
}

// recordingRelaySender is the RelaySender the listener tests put behind a listener. It records
// every call, in order, and answers each from a hook:
//   - SendRelay with send;
//   - ParseRelay with parse, or else by parsing the request with parser;
//   - SendParsedRelay with sendParsed.
//
// A path with no hook answers errRelayPathNotUsed.
type recordingRelaySender struct {
	send       func(call relayCall) (*common.RelayResult, error)
	parse      func(call relayCall) (ProtocolMessage, error)
	parser     ChainParser
	sendParsed func(call relayCall) (*common.RelayResult, error)

	mu    sync.Mutex
	calls []relayCall
}

// answering is a send or sendParsed hook that answers every relay with 200 and body.
func answering(body string) func(relayCall) (*common.RelayResult, error) {
	return func(relayCall) (*common.RelayResult, error) {
		return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(body)}, StatusCode: http.StatusOK}, nil
	}
}

// answeringWith is a send or sendParsed hook that answers every relay with result and err.
func answeringWith(result *common.RelayResult, err error) func(relayCall) (*common.RelayResult, error) {
	return func(relayCall) (*common.RelayResult, error) { return result, err }
}

// jsonRPCResultReply is a JSON-RPC answer a send hook can hand a listener.
const jsonRPCResultReply = `{"jsonrpc":"2.0","id":1,"result":"0x1"}`

func (r *recordingRelaySender) record(call relayCall) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	return len(r.calls) - 1
}

func (r *recordingRelaySender) answered(i int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[i].err = err
}

func (r *recordingRelaySender) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	call := relayCall{method: "SendRelay", ctx: ctx, tracing: tracingIdsFromContext(ctx), url: url, req: req, connectionType: connectionType, dappID: dappID, consumerIp: consumerIp, metadata: metadataValues}
	i := r.record(call)
	if r.send == nil {
		r.answered(i, errRelayPathNotUsed)
		return nil, errRelayPathNotUsed
	}
	result, err := r.send(call)
	r.answered(i, err)
	return result, err
}

func (r *recordingRelaySender) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	call := relayCall{method: "ParseRelay", ctx: ctx, tracing: tracingIdsFromContext(ctx), url: url, req: req, connectionType: connectionType, dappID: dappID, consumerIp: consumerIp, metadata: metadata}
	i := r.record(call)
	var (
		protocolMessage ProtocolMessage
		err             error
	)
	switch {
	case r.parse != nil:
		protocolMessage, err = r.parse(call)
	case r.parser != nil:
		var chainMessage ChainMessage
		chainMessage, err = r.parser.ParseMsg(url, []byte(req), connectionType, metadata, extensionslib.ExtensionInfo{})
		if err == nil {
			protocolMessage = NewProtocolMessage(chainMessage, nil, nil, dappID, consumerIp)
		}
	default:
		err = errRelayPathNotUsed
	}
	r.answered(i, err)
	return protocolMessage, err
}

func (r *recordingRelaySender) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	call := relayCall{method: "SendParsedRelay", ctx: ctx, tracing: tracingIdsFromContext(ctx), parsed: protocolMessage}
	i := r.record(call)
	if r.sendParsed == nil {
		r.answered(i, errRelayPathNotUsed)
		return nil, errRelayPathNotUsed
	}
	result, err := r.sendParsed(call)
	r.answered(i, err)
	return result, err
}

// sends is every SendRelay call so far, in order.
func (r *recordingRelaySender) sends() []relayCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []relayCall
	for _, call := range r.calls {
		if call.method == "SendRelay" {
			out = append(out, call)
		}
	}
	return out
}

// lastSend is the most recent SendRelay call, or a zero call when there was none.
func (r *recordingRelaySender) lastSend() relayCall {
	sends := r.sends()
	if len(sends) == 0 {
		return relayCall{}
	}
	return sends[len(sends)-1]
}

// sentURLs is the url of every SendRelay call so far, in order.
func (r *recordingRelaySender) sentURLs() []string {
	var urls []string
	for _, call := range r.sends() {
		urls = append(urls, call.url)
	}
	return urls
}

// relayed counts the SendRelay calls answered without an error: the relays that reached a
// provider.
func (r *recordingRelaySender) relayed() int {
	n := 0
	for _, call := range r.sends() {
		if call.err == nil {
			n++
		}
	}
	return n
}

// lastTracing is the caller ids on the most recent call's context, and whether there was a call.
func (r *recordingRelaySender) lastTracing() (tracingIds, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return tracingIds{}, false
	}
	return r.calls[len(r.calls)-1].tracing, true
}

// reset forgets the calls so far.
func (r *recordingRelaySender) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}
