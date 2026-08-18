package cache

import (
	"context"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func newCacheServerForTest(t *testing.T) *RelayerCacheServer {
	t.Helper()
	return &RelayerCacheServer{CacheServer: &CacheServer{
		tempCache:                  newRistrettoForTest(t),
		finalizedCache:             newRistrettoForTest(t),
		blocksHashesToHeightsCache: newRistrettoForTest(t),
		ExpirationFinalized:        time.Hour,
		ExpirationNonFinalized:     500 * time.Millisecond,
	}}
}

// TestSetRelay_RejectsNegativeRequestedBlock pins the cache-server contract that
// the smart router's tryCacheWrite guard relies on: SetRelay refuses every
// negative RequestedBlock, so a block tag the router did not resolve to a height
// can only ever come back as an error. EARLIEST/PENDING/SAFE/FINALIZED all land
// here, and that rejection was surfacing as one "cache write failed" warning per
// relay in production until the router learned to skip the call outright.
func TestSetRelay_RejectsNegativeRequestedBlock(t *testing.T) {
	cases := []struct {
		name  string
		block int64
	}{
		{"NOT_APPLICABLE", spectypes.NOT_APPLICABLE},
		{"LATEST_BLOCK", spectypes.LATEST_BLOCK},
		{"EARLIEST_BLOCK", spectypes.EARLIEST_BLOCK},
		{"PENDING_BLOCK", spectypes.PENDING_BLOCK},
		{"SAFE_BLOCK", spectypes.SAFE_BLOCK},
		{"FINALIZED_BLOCK", spectypes.FINALIZED_BLOCK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCacheServerForTest(t)

			// Response is deliberately nil: SetRelay only dereferences it after the
			// negative-block guard, so a panic here means the guard stopped firing.
			var (
				resp *emptypb.Empty
				err  error
			)
			require.NotPanics(t, func() {
				resp, err = srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
					ChainId:        "BASE",
					RequestHash:    []byte("req-hash"),
					RequestedBlock: tc.block,
				})
			})
			require.Error(t, err)
			require.Nil(t, resp)
			require.Contains(t, err.Error(), "request block is negative")
		})
	}
}

// TestSetRelay_AcceptsNonNegativeRequestedBlock is the positive control for the
// case above, so that test cannot pass because SetRelay rejects everything. It
// also covers block 0 (genesis), the boundary the guard is written against.
func TestSetRelay_AcceptsNonNegativeRequestedBlock(t *testing.T) {
	for _, block := range []int64{0, 12345} {
		srv := newCacheServerForTest(t)
		resp, err := srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
			ChainId:          "BASE",
			RequestHash:      []byte("req-hash"),
			RequestedBlock:   block,
			Response:         &relaytypes.RelayReply{Data: []byte(`{"result":"0x1"}`), LatestBlock: 12345},
			AverageBlockTime: int64(2 * time.Second),
		})
		require.NoError(t, err, "block %d must be accepted", block)
		require.NotNil(t, resp)
	}
}
