package redisstore

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// The live-rotation acceptance proof against a REAL Valkey/Redis — miniredis
// implements neither CONFIG, CLIENT, nor ACL, so it cannot express this.
//
//	docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey:7.2
//	RESP_CACHE_TEST_VALKEY_ADDR=127.0.0.1:63790 go test ./ecosystem/cache/redisstore -run LiveRotationAgainstRealValkey -v
//
// Asserts the PRD criterion literally: rotating between two ACL users through
// the streaming provider re-authenticates the live pooled connection IN PLACE —
// operations flow throughout, and the server-side CLIENT LIST shows the SAME
// connection id now running as the new user. Mere op success would prove
// nothing (a server password change never de-auths existing connections); the
// user= flip on a surviving id is the proof of no connection loss. An
// instrumented dialer additionally bounds dials: go-redis may legitimately
// dial ONE extra connection while the original is being re-authed (checkout
// prefers a fresh conn over blocking), but a reconnect storm means the
// rotation dropped connections.
func TestLiveRotationAgainstRealValkey(t *testing.T) {
	addr := os.Getenv("RESP_CACHE_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set RESP_CACHE_TEST_VALKEY_ADDR to a real Valkey/Redis (docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping(ctx).Err())

	for _, user := range []string{"rotuser1", "rotuser2"} {
		require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+user+"-pw", "~*", "+@all").Err())
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = admin.Do(cleanupCtx, "ACL", "DELUSER", "rotuser1", "rotuser2").Err()
	})

	credFile := writeTempFile(t, "cred", "rotuser1:rotuser1-pw")
	provider := NewStreamingProvider(&FileCredentials{Path: credFile})

	var dials atomic.Int64
	client := redis.NewClient(&redis.Options{
		Addr:                         addr,
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		MaxIdleConns:                 1,
		Dialer: func(ctx context.Context, network, dialAddr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, dialAddr)
		},
	})
	store, err := NewWithClient(client, "rotparity")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	op := func() error { return store.SetHeight(ctx, core.HeightKey("ETH1", "0xh"), 1, time.Minute) }
	require.NoError(t, op())

	connID, err := client.Do(ctx, "CLIENT", "ID").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), dials.Load())

	// Rotate: the file flips to the second ACL user; the provider pushes.
	require.NoError(t, os.WriteFile(credFile, []byte("rotuser2:rotuser2-pw"), 0o600))
	provider.Refresh()

	for i := 0; i < 10; i++ {
		require.NoError(t, op(), "operations must continue through the rotation")
		time.Sleep(20 * time.Millisecond)
	}

	// Server-side proof of in-place re-authentication without connection loss:
	// the ORIGINAL connection id is still alive and now reports the new user.
	require.Eventually(t, func() bool {
		list, listErr := admin.Do(ctx, "CLIENT", "LIST").Text()
		if listErr != nil {
			return false
		}
		needle := fmt.Sprintf("id=%d ", connID)
		for _, line := range strings.Split(list, "\n") {
			if strings.Contains(line, needle) {
				return strings.Contains(line, " user=rotuser2 ")
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "CLIENT LIST must show the ORIGINAL connection alive and re-authenticated as rotuser2")

	// Convergence: once the re-auth cycle completes, serving ops costs no
	// further dials (an exact count during the cycle is timing-dependent; a
	// broken rotation never converges). The quiet window lets the background
	// re-auth worker win the IDLE-state race against our probing.
	require.Eventually(t, func() bool {
		time.Sleep(300 * time.Millisecond)
		before := dials.Load()
		if op() != nil {
			return false
		}
		time.Sleep(50 * time.Millisecond)
		if op() != nil {
			return false
		}
		return dials.Load() == before
	}, 10*time.Second, 100*time.Millisecond, "the pool must converge to dial-free serving after rotation")
}
