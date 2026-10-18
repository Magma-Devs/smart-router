package utils

import (
	"context"
	"math/rand"
	"time"
)

type unique_identifier_ctx_key struct{}

// GenerateUniqueIdentifier mints a request GUID: the unix second in the top 32 bits, 32 random bits
// below. The second makes any GUID a bounded log search; two GUIDs collide only when minted in the
// same second with the same random bits.
func GenerateUniqueIdentifier() uint64 {
	return uint64(uint32(time.Now().Unix()))<<32 | uint64(rand.Uint32())
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
