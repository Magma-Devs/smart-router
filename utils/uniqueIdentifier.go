package utils

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"
)

type unique_identifier_ctx_key struct{}

// guidLow is the low half of the next GUID: a random start per process, then +1 per mint.
// Two GUIDs from one process differ in this half until 2^32 mints land in one second, and a
// clock step that revisits a second finds the counter already past that second's values.
var guidLow atomic.Uint32

func init() {
	guidLow.Store(rand.Uint32())
}

// GenerateUniqueIdentifier mints a request GUID: the unix second in the top 32 bits, the
// per-process counter below. The second makes any GUID a bounded log search; the counter keeps
// every GUID one process mints distinct, see guidLow.
func GenerateUniqueIdentifier() uint64 {
	return uint64(uint32(time.Now().Unix()))<<32 | uint64(guidLow.Add(1))
}

func WithUniqueIdentifier(ctx context.Context, guid uint64) context.Context {
	return context.WithValue(ctx, unique_identifier_ctx_key{}, guid)
}

func AppendUniqueIdentifier(ctx context.Context, guid uint64) context.Context {
	if ctx.Value(unique_identifier_ctx_key{}) != nil || guid == 0 {
		// do not add a guid if it exists
		return ctx
	}
	return context.WithValue(ctx, unique_identifier_ctx_key{}, guid)
}

func GetUniqueIdentifier(ctx context.Context) (guid uint64, found bool) {
	guid, found = ctx.Value(unique_identifier_ctx_key{}).(uint64)
	if !found {
		return 0, false
	}
	return guid, found
}
