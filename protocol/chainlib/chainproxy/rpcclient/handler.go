// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpcclient

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/ethereum/go-ethereum/log"
	"github.com/magma-Devs/smart-router/utils"
)

// handler handles JSON-RPC messages. There is one handler per connection. Note that
// handler is not safe for concurrent use. Message handling never blocks indefinitely
// because RPCs are processed on background goroutines launched by handler.
//
// The entry points for incoming messages are:
//
//	h.handleMsg(message)
//	h.handleBatch(message)
//
// Outgoing calls use the requestOp struct. Register the request before sending it
// on the connection:
//
//	op := &requestOp{ids: ...}
//	h.addRequestOp(op)
//
// Now send the request, then wait for the reply to be delivered through handleMsg:
//
//	if err := op.wait(...); err != nil {
//	    h.removeRequestOp(op) // timeout, etc.
//	}
type handler struct {
	reg            *serviceRegistry
	unsubscribeCb  *callback
	idgen          func() ID                      // subscription ID generator
	respWait       map[string]*requestOp          // active client requests
	clientSubs     map[string]*ClientSubscription // active client subscriptions
	callWG         sync.WaitGroup                 // pending call goroutines
	rootCtx        context.Context                // canceled by close()
	cancelRoot     func()                         // cancel function for rootCtx
	conn           jsonWriter                     // where responses will be sent
	log            log.Logger
	allowSubscribe bool

	subLock    utils.LavaMutex
	serverSubs map[ID]*Subscription
}

type callProc struct {
	ctx       context.Context
	notifiers []*Notifier
}

func newHandler(connCtx context.Context, conn jsonWriter, idgen func() ID, reg *serviceRegistry) *handler {
	rootCtx, cancelRoot := context.WithCancel(connCtx)
	h := &handler{
		reg:            reg,
		idgen:          idgen,
		conn:           conn,
		respWait:       make(map[string]*requestOp),
		clientSubs:     make(map[string]*ClientSubscription),
		rootCtx:        rootCtx,
		cancelRoot:     cancelRoot,
		allowSubscribe: true,
		serverSubs:     make(map[ID]*Subscription),
		log:            log.Root(),
	}
	if conn.remoteAddr() != "" {
		h.log = h.log.New("conn", conn.remoteAddr())
	}
	h.unsubscribeCb = newCallback(reflect.Value{}, reflect.ValueOf(h.unsubscribe))
	return h
}

// handleBatch executes all messages in a batch and returns the responses.
func (h *handler) handleBatch(msgs []*JsonrpcMessage) {
	// Emit error response for empty batches:
	if len(msgs) == 0 {
		h.startCallProc(func(cp *callProc) {
			h.conn.writeJSON(cp.ctx, errorMessage(&invalidRequestError{"empty batch"}))
		})
		return
	}

	// Handle non-call messages first:
	calls := make([]*JsonrpcMessage, 0, len(msgs))
	for _, msg := range msgs {
		if handled := h.handleImmediate(msg); !handled {
			calls = append(calls, msg)
		}
	}
	if len(calls) == 0 {
		return
	}
	// Process calls on a goroutine because they may block indefinitely:
	h.startCallProc(func(cp *callProc) {
		answers := make([]*JsonrpcMessage, 0, len(msgs))
		for _, msg := range calls {
			if answer := h.handleCallMsg(cp, msg); answer != nil {
				answers = append(answers, answer)
			}
		}
		h.addSubscriptions(cp.notifiers)
		if len(answers) > 0 {
			h.conn.writeJSON(cp.ctx, answers)
		}
		for _, n := range cp.notifiers {
			n.activate()
		}
	})
}

// handleMsg handles a single message.
func (h *handler) handleMsg(msg *JsonrpcMessage) {
	if ok := h.handleImmediate(msg); ok {
		return
	}
	h.startCallProc(func(cp *callProc) {
		answer := h.handleCallMsg(cp, msg)
		h.addSubscriptions(cp.notifiers)
		if answer != nil {
			h.conn.writeJSON(cp.ctx, answer)
		}
		for _, n := range cp.notifiers {
			n.activate()
		}
	})
}

// close cancels all requests except for inflightReq and waits for
// call goroutines to shut down.
func (h *handler) close(err error, inflightReq *requestOp) {
	h.cancelAllRequests(err, inflightReq)
	h.callWG.Wait()
	h.cancelRoot()
	h.cancelServerSubscriptions(err)
}

// addRequestOp registers a request operation.
func (h *handler) addRequestOp(op *requestOp) {
	for _, id := range op.ids {
		h.respWait[string(id)] = op
	}
}

// removeRequestOps stops waiting for the given request IDs.
func (h *handler) removeRequestOp(op *requestOp) {
	for _, id := range op.ids {
		delete(h.respWait, string(id))
	}
}

// cancelAllRequests unblocks and removes pending requests and active subscriptions.
func (h *handler) cancelAllRequests(err error, inflightReq *requestOp) {
	didClose := make(map[*requestOp]bool)
	if inflightReq != nil {
		didClose[inflightReq] = true
	}

	for id, op := range h.respWait {
		// Remove the op so that later calls will not close op.resp again.
		delete(h.respWait, id)

		if !didClose[op] {
			op.err = err
			close(op.resp)
			didClose[op] = true
		}
	}
	for id, sub := range h.clientSubs {
		delete(h.clientSubs, id)
		sub.close(err)
	}
}

func (h *handler) addSubscriptions(nn []*Notifier) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	for _, n := range nn {
		if sub := n.takeSubscription(); sub != nil {
			h.serverSubs[sub.ID] = sub
		}
	}
}

// cancelServerSubscriptions removes all subscriptions and closes their error channels.
func (h *handler) cancelServerSubscriptions(err error) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	for id, s := range h.serverSubs {
		s.err <- err
		close(s.err)
		delete(h.serverSubs, id)
	}
}

// startCallProc runs fn in a new goroutine and starts tracking it in the h.calls wait group.
func (h *handler) startCallProc(fn func(*callProc)) {
	h.callWG.Add(1)
	go func() {
		ctx, cancel := context.WithCancel(h.rootCtx)
		defer h.callWG.Done()
		defer cancel()
		fn(&callProc{ctx: ctx})
	}()
}

// handleImmediate executes non-call messages. It returns false if the message is a
// call or requires a reply.
func (h *handler) handleImmediate(msg *JsonrpcMessage) bool {
	start := time.Now()
	switch {
	case msg.isTendermintNotification():
		h.handleSubscriptionResultTendermint(msg)
		return true
	case msg.isEthereumNotification():
		if strings.HasSuffix(msg.Method, ethereumNotificationMethodSuffix) {
			h.handleSubscriptionResultEthereum(msg)
			return true
		} else if strings.HasSuffix(msg.Method, solanaNotificationMethodSuffix) {
			// Unreachable: isEthereumNotification above already required the
			// "_subscription" suffix, which no *Notification method has. Solana frames
			// are served by the shape-based case below instead — left in place only so
			// removing it is not confused with a behaviour change (MAG-3359).
			h.handleSubscriptionResultSolana(msg)
			return true
		}
		return false
	case msg.Method != "" && msg.Params != nil:
		// Shape-based delivery: a method-bearing frame whose params name a subscription.
		// The method-name cases above keep first claim on anything they already
		// recognise; this catches the rest, which is how Substrate and Solana pushes
		// arrive — see subscriptionIDFromParams.
		//
		// It sits BEFORE the StarkNet case deliberately. That predicate matches any
		// id-less method-bearing frame carrying a result, so a push that also sets a
		// top-level result would be swallowed there and answered with "invalid request"
		// — the very MAG-3345 symptom this exists to remove. StarkNet's own frames put
		// the id in result and carry no params, so they never enter this case.
		//
		// The id is parsed once and handed to the delivery path. The sibling handlers
		// above re-parse inside the handler, which here would mean decoding the whole
		// payload twice for every frame delivered.
		id, named := subscriptionIDFromParams(msg.Params)
		if !named {
			return false
		}
		return h.deliverSubscriptionPush(msg, id)
	case msg.isStarkNetPathfinderNotification():
		if strings.HasSuffix(msg.Method, ethereumNotificationMethodSuffix) {
			h.handleSubscriptionResultStarkNetPathfinder(msg)
			return true
		}
		return false
	case msg.isResponse():
		h.handleResponse(msg)
		h.log.Trace("Handled RPC response", "reqid", idForLog{msg.ID}, "duration", time.Since(start))
		return true
	default:
		return false
	}
}

// deliverSubscriptionPush hands a push frame to the subscription it names, reporting
// whether this handler claimed the frame.
//
// Claiming matters as much as delivering. Before MAG-3345 an unrecognised push fell
// through to handleCallMsg, which had no id and no matching method to serve, so the router
// answered the node with an "invalid request" for every frame it received. But a frame that
// carries a request id is a request the peer expects answered, and swallowing it silently
// would leave the peer waiting on a reply that never comes — so an id-bearing frame that
// matches no subscription is handed back to the call path, while an id-less one (a genuine
// JSON-RPC notification, which nobody is waiting on) is claimed and dropped with a trace.
func (h *handler) deliverSubscriptionPush(msg *JsonrpcMessage, subscriptionID string) bool {
	if sub := h.clientSubs[subscriptionID]; sub != nil {
		sub.deliver(msg)
		return true
	}
	if msg.hasValidID() {
		return false
	}
	// Late frames after an unsubscribe land here routinely, so this is a trace and not a
	// warning — but it is logged, because a subscription registered under a key the push
	// does not name is otherwise indistinguishable from a healthy idle one, which is
	// exactly the failure mode MAG-3345 was.
	utils.LavaFormatTrace("Dropping subscription push with no matching subscription",
		utils.LogAttr("method", msg.Method),
		utils.LogAttr("subscriptionID", subscriptionID),
	)
	return true
}

func (h *handler) handleSubscriptionResultStarkNetPathfinder(msg *JsonrpcMessage) {
	var result integerIdSubscriptionResult
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		utils.LavaFormatTrace("Dropping invalid starknet pathfinder subscription message",
			utils.LogAttr("err", err),
			utils.LogAttr("result", string(msg.Result)),
		)
		h.log.Debug("Dropping invalid subscription message")
		return
	}

	id := strconv.Itoa(result.ID)
	if h.clientSubs[id] != nil {
		h.clientSubs[id].deliver(msg)
	}
}

// handleSubscriptionResult processes subscription notifications.
func (h *handler) handleSubscriptionResultEthereum(msg *JsonrpcMessage) {
	var result ethereumSubscriptionResult
	if err := json.Unmarshal(msg.Params, &result); err != nil {
		utils.LavaFormatTrace("Dropping invalid ethereum subscription message",
			utils.LogAttr("err", err),
			utils.LogAttr("params", string(msg.Params)),
		)
		h.log.Debug("Dropping invalid subscription message")
		return
	}
	if h.clientSubs[result.ID] != nil {
		h.clientSubs[result.ID].deliver(msg)
	}
}

func (h *handler) handleSubscriptionResultSolana(msg *JsonrpcMessage) {
	var result integerIdSubscriptionResult
	if err := json.Unmarshal(msg.Params, &result); err != nil {
		utils.LavaFormatTrace("Dropping invalid solana subscription message",
			utils.LogAttr("err", err),
			utils.LogAttr("params", string(msg.Params)),
		)
		h.log.Debug("Dropping invalid subscription message")
		return
	}
	if h.clientSubs[strconv.Itoa(result.ID)] != nil {
		h.clientSubs[strconv.Itoa(result.ID)].deliver(msg)
	}
}

func (h *handler) handleSubscriptionResultTendermint(msg *JsonrpcMessage) {
	var result tendermintSubscriptionResult
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		utils.LavaFormatTrace("Dropping invalid tendermint subscription message",
			utils.LogAttr("err", err),
			utils.LogAttr("result", string(msg.Result)),
		)
		h.log.Debug("Dropping invalid subscription message")
		return
	}
	if h.clientSubs[result.Query] != nil {
		h.clientSubs[result.Query].deliver(msg)
	}
}

// handleResponse processes method call responses.
func (h *handler) handleResponse(msg *JsonrpcMessage) {
	op := h.respWait[string(msg.ID)]
	if op == nil {
		utils.LavaFormatWarning("Unsolicited RPC response", nil, utils.LogAttr("req-id", idForLog{msg.ID}.String()))
		return
	}
	delete(h.respWait, string(msg.ID))
	if op.sub == nil {
		// Normal call response: forward the reply to Call/BatchCall. op.err stays nil.
		op.resp <- msg
		return
	}
	// Subscription response: populate op.err and start the subscription BEFORE signaling the waiter.
	// op.resp<-msg is the happens-before edge wait() relies on to read op.err — sending it first (as
	// this code previously did) let wait() read op.err while these lines were still writing it (a data
	// race under -race). The send still happens (Subscribe needs the msg value), just after op.err/sub
	// are fully set. EthSubscribe gets unblocked either way.
	defer close(op.resp)
	if msg.Error != nil {
		op.err = msg.Error
	} else if op.subId != "" {
		go op.sub.run()
		h.clientSubs[op.subId] = op.sub
	} else if op.err = json.Unmarshal(msg.Result, &op.sub.subid); op.err == nil {
		go op.sub.run()
		h.clientSubs[op.sub.subid] = op.sub
	} else {
		// This is because StarkNet Pathfinder is returning an integer instead of a string in the result.
		// int64 rather than int so this key cannot drift from CanonicalSubscriptionID's on a 32-bit build.
		var integerSubId int64
		if json.Unmarshal(msg.Result, &integerSubId) == nil {
			op.err = nil
			op.sub.subid = strconv.FormatInt(integerSubId, 10)
			go op.sub.run()
			h.clientSubs[op.sub.subid] = op.sub
		}
	}
	op.resp <- msg
}

// handleCallMsg executes a call message and returns the answer.
func (h *handler) handleCallMsg(ctx *callProc, msg *JsonrpcMessage) *JsonrpcMessage {
	start := time.Now()
	servedStr := "Served "
	switch {
	case msg.isEthereumNotification(), msg.isTendermintNotification():
		h.handleCall(ctx, msg)
		h.log.Debug(servedStr+msg.Method, "duration", time.Since(start))
		return nil
	case msg.isCall():
		resp := h.handleCall(ctx, msg)
		var ctx []interface{}
		ctx = append(ctx, "reqid", idForLog{msg.ID}, "duration", time.Since(start))
		if resp.Error != nil {
			ctx = append(ctx, "err", resp.Error.Message)
			if resp.Error.Data != nil {
				ctx = append(ctx, "errdata", resp.Error.Data)
			}
			h.log.Warn(servedStr+msg.Method, ctx...)
		} else {
			h.log.Debug(servedStr+msg.Method, ctx...)
		}
		return resp
	case msg.hasValidID():
		return msg.errorResponse(&invalidRequestError{"invalid request"})
	default:
		return errorMessage(&invalidRequestError{"invalid request"})
	}
}

// handleCall processes method calls.
func (h *handler) handleCall(cp *callProc, msg *JsonrpcMessage) *JsonrpcMessage {
	if msg.isSubscribe() {
		return h.handleSubscribe(cp, msg)
	}
	var callb *callback
	if msg.isUnsubscribe() {
		callb = h.unsubscribeCb
	} else {
		callb = h.reg.callback(msg.Method)
	}
	if callb == nil {
		return msg.errorResponse(&methodNotFoundError{method: msg.Method})
	}
	args, err := parsePositionalArguments(msg.Params, callb.argTypes)
	if err != nil {
		return msg.errorResponse(&invalidParamsError{err.Error()})
	}
	start := time.Now()
	answer := h.runMethod(cp.ctx, msg, callb, args)

	// Collect the statistics for RPC calls if metrics is enabled.
	// We only care about pure rpc call. Filter out subscription.
	if callb != h.unsubscribeCb {
		rpcRequestGauge.Inc(1)
		if answer.Error != nil {
			failedRequestGauge.Inc(1)
		} else {
			successfulRequestGauge.Inc(1)
		}
		rpcServingTimer.UpdateSince(start)
		newRPCServingTimer(msg.Method, answer.Error == nil).UpdateSince(start)
	}
	return answer
}

// handleSubscribe processes *_subscribe method calls.
func (h *handler) handleSubscribe(cp *callProc, msg *JsonrpcMessage) *JsonrpcMessage {
	if !h.allowSubscribe {
		return msg.errorResponse(ErrNotificationsUnsupported)
	}

	// Subscription method name is first argument.
	name, err := parseSubscriptionName(msg.Params)
	if err != nil {
		return msg.errorResponse(&invalidParamsError{err.Error()})
	}
	namespace := msg.namespace()
	callb := h.reg.subscription(namespace, name)
	if callb == nil {
		return msg.errorResponse(&subscriptionNotFoundError{namespace, name})
	}

	// Parse subscription name arg too, but remove it before calling the callback.
	argTypes := append([]reflect.Type{stringType}, callb.argTypes...)
	args, err := parsePositionalArguments(msg.Params, argTypes)
	if err != nil {
		return msg.errorResponse(&invalidParamsError{err.Error()})
	}
	args = args[1:]

	// Install notifier in context so the subscription handler can find it.
	n := &Notifier{h: h, namespace: namespace}
	cp.notifiers = append(cp.notifiers, n)
	ctx := context.WithValue(cp.ctx, notifierKey{}, n)

	return h.runMethod(ctx, msg, callb, args)
}

// runMethod runs the Go callback for an RPC method.
func (h *handler) runMethod(ctx context.Context, msg *JsonrpcMessage, callb *callback, args []reflect.Value) *JsonrpcMessage {
	result, err := callb.call(ctx, msg.Method, args)
	if err != nil {
		return msg.errorResponse(err)
	}
	return msg.response(result)
}

// unsubscribe is the callback function for all *_unsubscribe calls.
func (h *handler) unsubscribe(ctx context.Context, id ID) (bool, error) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	s := h.serverSubs[id]
	if s == nil {
		return false, ErrSubscriptionNotFound
	}
	close(s.err)
	delete(h.serverSubs, id)
	return true, nil
}

type idForLog struct{ json.RawMessage }

func (id idForLog) String() string {
	if s, err := strconv.Unquote(string(id.RawMessage)); err == nil {
		return s
	}
	return string(id.RawMessage)
}
