package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The three tracing headers reach the context from two header-map shapes: fiber's
// GetReqHeaders() hands over canonical keys, gRPC metadata hands over lower-case ones
// (MAG-3798). Both must stamp the same values.
func TestExtractWantedHeadersFromCachedMap(t *testing.T) {
	type ids struct{ request, task, tx string }
	read := func(ctx context.Context) ids {
		var got ids
		got.request, _ = GetRequestId(ctx)
		got.task, _ = GetTaskId(ctx)
		got.tx, _ = GetTxId(ctx)
		return got
	}
	all := ids{request: "req-42", task: "task-7", tx: "tx-9"}

	tests := []struct {
		name    string
		headers map[string][]string
		want    ids
	}{
		{
			name:    "canonical keys, as fasthttp normalises them",
			headers: map[string][]string{"X-Request-Id": {"req-42"}, "X-Task-Id": {"task-7"}, "X-Tx-Id": {"tx-9"}},
			want:    all,
		},
		{
			name:    "lower-case keys, as gRPC metadata carries them",
			headers: map[string][]string{"x-request-id": {"req-42"}, "x-task-id": {"task-7"}, "x-tx-id": {"tx-9"}},
			want:    all,
		},
		{
			name:    "the first value wins on a repeated header",
			headers: map[string][]string{"X-Request-Id": {"req-42", "req-43"}},
			want:    ids{request: "req-42"},
		},
		{
			name:    "an empty value stamps nothing",
			headers: map[string][]string{"X-Request-Id": {""}, "X-Task-Id": {"task-7"}},
			want:    ids{task: "task-7"},
		},
		{
			name:    "unrelated headers stamp nothing",
			headers: map[string][]string{"Content-Type": {"application/json"}},
			want:    ids{},
		},
		{
			name:    "a nil map stamps nothing",
			headers: nil,
			want:    ids{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, read(ExtractWantedHeadersFromCachedMap(tt.headers, context.Background())))
		})
	}
}

// An absent or empty header must read back as "not found", not as an empty id: the log
// formatter and the relay builder branch on that flag.
func TestExtractWantedHeadersFromCachedMap_EmptyHeaderIsNotFound(t *testing.T) {
	ctx := ExtractWantedHeadersFromCachedMap(map[string][]string{"X-Request-Id": {""}}, context.Background())
	_, found := GetRequestId(ctx)
	require.False(t, found, "an empty header must not register as a present id")
	_, found = GetTaskId(ctx)
	require.False(t, found, "a header that was never sent must not register as a present id")
}
