package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

// CustomerRequest is created exclusively by a client-facing listener. Internal
// relays have no such state, even when their method matches a customer's method.
// The context carries observations across selection goroutines; a terminal
// snapshot closes it, so detached losers cannot revise the customer's outcome.
type CustomerRequest struct {
	mu              sync.Mutex
	finished        bool
	guid, requestID string
	reroutes        []CustomerReroute
	attempts        []CustomerAttempt
	result          CustomerResult
	terminal        CustomerTerminal
}

type customerRequestKey struct{}

type CustomerReroute struct {
	Provider     string   `json:"provider,omitempty"`
	Reason       string   `json:"reason"`
	Alternatives []string `json:"alternatives"`
}

type CustomerAttempt struct {
	Provider  string `json:"provider"`
	Outcome   string `json:"outcome"`
	ErrorName string `json:"error_name,omitempty"`
}

// CustomerResult is supplied by the relay decision, before the listener emits.
// Error is an existing registry identity, never a caller-supplied metric label.
type CustomerResult struct {
	Provider, CacheTier, StopReason string
	NodeError                       bool
	Error                           *common.LavaError
	PartialBatch                    bool
	WriteOutcomeUnknown             bool
	StatusCode                      int
	HasReply                        bool
	ReplySize                       int
}

type CustomerTerminal struct {
	SchemaVersion int               `json:"schema_version"`
	Terminal      bool              `json:"terminal"`
	RequestKind   string            `json:"request_kind"`
	GUID          string            `json:"GUID"`
	RequestID     string            `json:"request_id,omitempty"`
	Chain         string            `json:"chain"`
	APIInterface  string            `json:"api_interface"`
	APIMethod     string            `json:"api_method"`
	StartedAtNs   int64             `json:"started_at_ns"`
	CompletedAtNs int64             `json:"completed_at_ns"`
	DurationMs    int64             `json:"duration_ms"`
	Outcome       string            `json:"outcome"`
	Recovery      string            `json:"recovery"`
	CacheHit      bool              `json:"cache_hit"`
	CacheTier     string            `json:"cache_tier"`
	Provider      string            `json:"provider,omitempty"`
	StopReason    string            `json:"stop_reason,omitempty"`
	StatusCode    int               `json:"status"`
	HasReply      bool              `json:"has_reply"`
	ReplySize     int               `json:"reply_size"`
	ErrorName     string            `json:"error_name"`
	ErrorCode     uint32            `json:"error_code"`
	ErrorCategory string            `json:"error_category,omitempty"`
	Retryable     bool              `json:"retryable"`
	PartialBatch  bool              `json:"partial_batch"`
	WriteOutcome  string            `json:"write_outcome,omitempty"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	Attempts      []CustomerAttempt `json:"attempts,omitempty"`
	Reroutes      []CustomerReroute `json:"reroutes,omitempty"`
}

// The additive interface keeps existing metrics implementations and test doubles
// source-compatible. Legacy provider-attempt counters retain their semantics.
type CustomerMetricsRecorder interface {
	RecordCustomerReceived(chain, apiInterface string)
	RecordCustomerOutcome(CustomerTerminal)
}

func (rpccl *RPCConsumerLogs) BeginCustomerRequest(ctx context.Context, data *RelayMetrics) context.Context {
	if data == nil || data.Customer != nil {
		return ctx
	}
	guid, ok := utils.GetUniqueIdentifier(ctx)
	if !ok {
		guid = utils.GenerateUniqueIdentifier()
		ctx = utils.WithUniqueIdentifier(ctx, guid)
	}
	requestID, _ := utils.GetRequestId(ctx)
	data.RequestKind = "customer"
	data.Customer = &CustomerRequest{guid: strconv.FormatUint(guid, 10), requestID: strings.Clone(requestID)}
	if recorder, ok := rpccl.consumerMetricsManager.(CustomerMetricsRecorder); ok {
		recorder.RecordCustomerReceived(data.ChainID, data.APIType)
	}
	return context.WithValue(ctx, customerRequestKey{}, data.Customer)
}

// MarkCustomerReroute records only a causal selection decision. Alternatives must
// be the providers actually selected after bypassing the intended provider/pool.
func MarkCustomerReroute(ctx context.Context, provider, reason string, alternatives []string) {
	state, _ := ctx.Value(customerRequestKey{}).(*CustomerRequest)
	if state == nil || len(alternatives) == 0 {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return
	}
	state.reroutes = append(state.reroutes, CustomerReroute{Provider: provider, Reason: reason, Alternatives: append([]string(nil), alternatives...)})
}

// ObserveCustomerProvider is used by subscription setup, which has no RelayResult. The
// provider is the configured name where the manager knows it, as relay_end writes it; a
// bare url is redacted before it becomes request evidence. It is never a metric label.
func ObserveCustomerProvider(ctx context.Context, provider string) {
	state, _ := ctx.Value(customerRequestKey{}).(*CustomerRequest)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.finished {
		state.result.Provider = utils.RedactIfURL(provider)
	}
}

func (data *RelayMetrics) CaptureCustomerResult(result *common.RelayResult, stopReason string, attempts []CustomerAttempt, classified *common.LavaError, writeOutcomeUnknown bool) {
	if data == nil || data.Customer == nil {
		return
	}
	state := data.Customer
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return
	}
	state.attempts = append([]CustomerAttempt(nil), attempts...)
	state.result = CustomerResult{StopReason: stopReason, Error: classified, WriteOutcomeUnknown: writeOutcomeUnknown}
	if result != nil {
		state.result.StatusCode = result.StatusCode
		state.result.HasReply = result.Reply != nil
		if result.Reply != nil {
			state.result.ReplySize = len(result.Reply.Data)
		}
		state.result.Provider = result.GetProvider()
		state.result.CacheTier = result.CacheLookup.TierHeaderValue()
		state.result.NodeError = result.IsNodeError || result.CustomerError != nil
		if result.CustomerError != nil {
			state.result.Error = result.CustomerError
		}
	}
}

func customerError(err error, apiInterface string) *common.LavaError {
	if err == nil {
		return common.LavaErrorUnknown
	}
	var classified *common.LavaError
	if errors.As(err, &classified) && classified != nil {
		return classified
	}
	var syntax *json.SyntaxError
	var fastSyntax *gojson.SyntaxError
	if errors.As(err, &syntax) || errors.As(err, &fastSyntax) {
		return common.LavaErrorUserParseError
	}
	// An error with no registry identity is classified by its text, the way an attempt's
	// transport error is: a connection fault first, then the registry's message patterns.
	return common.ClassifyError(common.DetectConnectionError(err), common.ChainFamilyUnknown, common.ApiInterfaceToTransport(apiInterface), 0, err.Error())
}

// ObserveCustomerReply reads only the winning reply. Batch reporting uses a
// stricter envelope definition (any failed item fails the envelope), independently
// of the retry policy's BatchNodeErrorOnAny setting. No routing flag is mutated.
func (rpccl *RPCConsumerLogs) ObserveCustomerReply(data *RelayMetrics, reply *common.RelayResult) {
	if data == nil || data.Customer == nil || reply == nil {
		return
	}
	state := data.Customer
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return
	}
	state.result.StatusCode = reply.StatusCode
	state.result.HasReply = reply.Reply != nil
	if reply.Reply != nil {
		state.result.ReplySize = len(reply.Reply.Data)
	}
	state.result.Provider = reply.GetProvider()
	state.result.CacheTier = reply.CacheLookup.TierHeaderValue()
	state.result.NodeError = reply.IsNodeError || reply.CustomerError != nil
	if reply.CustomerError != nil {
		state.result.Error = reply.CustomerError
	}
	if reply.Reply == nil || (data.APIType != "jsonrpc" && data.APIType != "tendermintrpc") {
		return
	}
	body := bytes.TrimSpace(reply.Reply.Data)
	// Single responses are already classified by the relay. Avoid parsing their
	// entire payload a second time on the overwhelmingly common path.
	if len(body) == 0 || body[0] != '[' {
		return
	}
	successes, failures, errorCode, errorMessage := rpcInterfaceMessages.SummarizeJSONRPCBatch(body)
	if failures > 0 {
		state.result.NodeError = true
		if state.result.Error == nil {
			state.result.Error = common.ClassifyError(nil, common.GetChainFamilyOrDefault(data.ChainID), common.TransportJsonRPC, errorCode, errorMessage)
		}
	}
	state.result.PartialBatch = failures > 0 && successes > 0
}

func (rpccl *RPCConsumerLogs) finishCustomer(data *RelayMetrics, err error) bool {
	state := data.Customer
	if state == nil {
		return true
	} // legacy/non-terminal usage (e.g. pushed notifications)
	state.mu.Lock()
	if state.finished {
		state.mu.Unlock()
		return false
	}
	result := state.result
	terminal := CustomerTerminal{SchemaVersion: 1, Terminal: true, RequestKind: "customer", GUID: state.guid, RequestID: state.requestID, Chain: data.ChainID, APIInterface: data.APIType, APIMethod: data.ApiMethod, StartedAtNs: data.Timestamp.UnixNano(), CompletedAtNs: time.Now().UnixNano(), Outcome: "success", Recovery: "none", CacheTier: result.CacheTier, Provider: result.Provider, StopReason: result.StopReason, ErrorName: "none", Attempts: state.attempts, Reroutes: state.reroutes}
	terminal.DurationMs = (terminal.CompletedAtNs - terminal.StartedAtNs) / int64(time.Millisecond)
	terminal.PartialBatch = result.PartialBatch
	terminal.StatusCode, terminal.HasReply, terminal.ReplySize = result.StatusCode, result.HasReply, result.ReplySize
	if result.WriteOutcomeUnknown {
		terminal.WriteOutcome = "unknown"
		result.Error = common.LavaErrorUnknown
	}
	if terminal.CacheTier == "" {
		terminal.CacheTier = common.CacheTierNone
	}
	terminal.CacheHit = terminal.CacheTier == common.CacheTierPrimary || terminal.CacheTier == common.CacheTierSecondary
	if terminal.CacheHit {
		terminal.Provider = "Cached"
	}
	if err != nil || result.NodeError {
		terminal.Outcome = "failure"
		// Only the customer's cancellation is terminal here. Cancelled hedge losers
		// are observations and cannot turn a successful request into a cancellation.
		if errors.Is(err, context.Canceled) {
			terminal.Outcome = "cancelled"
		}
		classified := result.Error
		if classified == nil {
			classified = customerError(err, data.APIType)
		}
		terminal.ErrorName, terminal.ErrorCode = classified.Name, classified.Code
		terminal.ErrorCategory, terminal.Retryable = classified.Category.String(), classified.Retryable
		// Registry descriptions are bounded and contain no payload, URL, credentials
		// or caller text. Detailed incidents join attempt logs using GUID/request_id.
		terminal.ErrorMessage = classified.Description
		if err != nil {
			terminal.ErrorMessage = utils.RedactSecrets(err.Error())
			if len(terminal.ErrorMessage) > 1024 {
				terminal.ErrorMessage = terminal.ErrorMessage[:1024]
			}
		}
	}
	if terminal.Outcome == "success" && !terminal.CacheHit && terminal.Provider != "" {
		retry, reroute := false, false
		for _, attempt := range terminal.Attempts {
			if attempt.Outcome == "failure" && attempt.Provider != "" && attempt.Provider != terminal.Provider {
				retry = true
			}
		}
		for _, decision := range terminal.Reroutes {
			for _, alternative := range decision.Alternatives {
				if alternative == terminal.Provider && alternative != decision.Provider {
					reroute = true
				}
			}
		}
		switch {
		case retry && reroute:
			terminal.Recovery = "both"
		case retry:
			terminal.Recovery = "retry"
		case reroute:
			terminal.Recovery = "reroute"
		}
	}
	state.terminal, state.finished = terminal, true
	state.mu.Unlock()
	data.Success = terminal.Outcome == "success"
	data.CacheHit, data.ProviderAddress, data.Latency = terminal.CacheHit, terminal.Provider, terminal.DurationMs
	if recorder, ok := rpccl.consumerMetricsManager.(CustomerMetricsRecorder); ok {
		recorder.RecordCustomerOutcome(terminal)
	}
	// No log line of its own: relay_end (docs/RELAY-END-EVENT.md) is the one per-relay event, and
	// the Loki readers that count relays select it, so a second line would be counted twice. The
	// snapshot reaches the collector on the usage event (relay_usage, OTel) instead.
	return true
}

func (state *CustomerRequest) Snapshot() CustomerTerminal {
	state.mu.Lock()
	defer state.mu.Unlock()
	snapshot := state.terminal
	snapshot.Attempts = append([]CustomerAttempt(nil), snapshot.Attempts...)
	snapshot.Reroutes = append([]CustomerReroute(nil), snapshot.Reroutes...)
	for i := range snapshot.Reroutes {
		snapshot.Reroutes[i].Alternatives = append([]string(nil), snapshot.Reroutes[i].Alternatives...)
	}
	return snapshot
}
