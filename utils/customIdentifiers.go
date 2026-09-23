package utils

import (
	"context"
	"strings"
)

type request_id_ctx_key struct{}

func WithRequestId(ctx context.Context, reqId string) context.Context {
	return context.WithValue(ctx, request_id_ctx_key{}, reqId)
}

func GetRequestId(ctx context.Context) (reqId string, found bool) {
	reqId, found = ctx.Value(request_id_ctx_key{}).(string)
	return reqId, found
}

type task_id_ctx_key struct{}

func WithTaskId(ctx context.Context, taskId string) context.Context {
	return context.WithValue(ctx, task_id_ctx_key{}, taskId)
}

func GetTaskId(ctx context.Context) (taskId string, found bool) {
	taskId, found = ctx.Value(task_id_ctx_key{}).(string)
	return taskId, found
}

type tx_id_ctx_key struct{}

func WithTxId(ctx context.Context, txId string) context.Context {
	return context.WithValue(ctx, tx_id_ctx_key{}, txId)
}

func GetTxId(ctx context.Context) (txId string, found bool) {
	txId, found = ctx.Value(tx_id_ctx_key{}).(string)
	return txId, found
}

// ExtractWantedHeadersFromCachedMap reads the caller's tracing headers (X-Request-Id,
// X-Task-Id, X-Tx-Id) out of a request's header map and stamps them on the context, so
// they reach the log lines and the relay. Every listener calls it once per request with
// the map it already holds: fiber's GetReqHeaders() on the HTTP interfaces, the incoming
// metadata on gRPC (MAG-3798).
func ExtractWantedHeadersFromCachedMap(headers map[string][]string, ctx context.Context) context.Context {
	if reqId := getHeaderValue(headers, "X-Request-Id"); reqId != "" {
		ctx = WithRequestId(ctx, reqId)
	}

	if taskId := getHeaderValue(headers, "X-Task-Id"); taskId != "" {
		ctx = WithTaskId(ctx, taskId)
	}

	if txId := getHeaderValue(headers, "X-Tx-Id"); txId != "" {
		ctx = WithTxId(ctx, txId)
	}

	return ctx
}

// getHeaderValue returns the first value stored under key, looking the key up as written
// and then in lower case. The HTTP listeners hand over canonical keys (X-Request-Id:
// fasthttp normalises them), while gRPC metadata keys are always lower case
// (x-request-id: HTTP/2 field names are, and grpc-go lowers them on the way in), so a
// single lookup shape would honour the headers on one transport and drop them on the other.
func getHeaderValue(headers map[string][]string, key string) string {
	if values, ok := headers[key]; ok && len(values) > 0 {
		return values[0]
	}
	if values, ok := headers[strings.ToLower(key)]; ok && len(values) > 0 {
		return values[0]
	}
	return ""
}
