package utils

import (
	"context"
	"runtime/pprof"
	"testing"
)

func TestWithGoroutineLabels(t *testing.T) {
	t.Cleanup(func() { pprof.SetGoroutineLabels(context.Background()) })

	ctx := WithGoroutineLabels(context.Background(),
		"chain", "ETH1",
		"api_interface", "jsonrpc",
		"provider", "1.2.3.4:443",
		"method", "", // empty value dropped
	)
	for k, want := range map[string]string{"chain": "ETH1", "api_interface": "jsonrpc", "provider": "1.2.3.4:443"} {
		if got, ok := pprof.Label(ctx, k); !ok || got != want {
			t.Fatalf("label %q = %q, %v; want %q", k, got, ok, want)
		}
	}
	if _, ok := pprof.Label(ctx, "method"); ok {
		t.Fatal("empty-valued label should be dropped")
	}
}

func TestWithGoroutineLabels_NoPairs(t *testing.T) {
	base := context.Background()
	if got := WithGoroutineLabels(base); got != base {
		t.Fatal("no pairs must return the context unchanged")
	}
	if got := WithGoroutineLabels(base, "loneKey"); got != base {
		t.Fatal("unpaired key must return the context unchanged")
	}
}
