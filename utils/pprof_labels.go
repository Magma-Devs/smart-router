package utils

import (
	"context"
	"runtime/pprof"
)

// WithGoroutineLabels attaches pprof labels to the CURRENT goroutine and
// returns a context carrying them, so any children spawned from that context
// inherit the same set.
//
// Labels are what make the Go 1.27 goroutineleak profile
// (/debug/pprof/goroutineleak), every other pprof profile, and 1.27 crash
// tracebacks attributable per chain / api-interface / provider instead of an
// anonymous stack. Call it as the first statement inside a relay goroutine.
//
// labelPairs is key, value, key, value, ...; a trailing unpaired key is
// dropped. Empty-valued pairs are skipped so a missing method/provider does
// not create a blank label.
func WithGoroutineLabels(ctx context.Context, labelPairs ...string) context.Context {
	kv := make([]string, 0, len(labelPairs))
	for i := 0; i+1 < len(labelPairs); i += 2 {
		if labelPairs[i] == "" || labelPairs[i+1] == "" {
			continue
		}
		kv = append(kv, labelPairs[i], labelPairs[i+1])
	}
	if len(kv) == 0 {
		return ctx
	}
	ctx = pprof.WithLabels(ctx, pprof.Labels(kv...))
	pprof.SetGoroutineLabels(ctx)
	return ctx
}
