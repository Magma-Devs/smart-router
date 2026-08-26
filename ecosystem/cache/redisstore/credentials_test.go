package redisstore

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/stretchr/testify/require"
)

// recordingListener captures pushes like a subscribed connection would.
type recordingListener struct {
	mu     sync.Mutex
	pushes []string
	errs   []error
}

func (r *recordingListener) OnNext(creds auth.Credentials) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pushes = append(r.pushes, creds.RawCredentials())
}

func (r *recordingListener) OnError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *recordingListener) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.pushes...)
}

func TestFileCredentialsParsing(t *testing.T) {
	passwordOnly := writeTempFile(t, "pw", "placeholder-credential\n")
	src := &FileCredentials{Username: "fixed-user", Path: passwordOnly}
	user, pass, err := src.Credentials()
	require.NoError(t, err)
	require.Equal(t, "fixed-user", user)
	require.Equal(t, "placeholder-credential", pass)

	userAndPass := writeTempFile(t, "userpw", "rotated-user:rotated-pass\n")
	src = &FileCredentials{Username: "ignored", Path: userAndPass}
	user, pass, err = src.Credentials()
	require.NoError(t, err)
	require.Equal(t, "rotated-user", user, "a user:pass file rotates the username too (ACL-user rotation)")
	require.Equal(t, "rotated-pass", pass)
}

// The plumbing contract: a push reaches every subscribed connection exactly
// when the credentials actually changed; unsubscribed listeners never hear
// again.
func TestStreamingProviderPushes(t *testing.T) {
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})

	first, second := &recordingListener{}, &recordingListener{}
	creds, unsubFirst, err := provider.Subscribe(first)
	require.NoError(t, err)
	require.Equal(t, "u:pw1", creds.RawCredentials(), "subscription hands out the current credentials")
	_, _, err = provider.Subscribe(second)
	require.NoError(t, err)
	require.Equal(t, 2, provider.subscriberCount())

	provider.Refresh()
	require.Empty(t, first.recorded(), "unchanged credentials must not push")

	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	provider.Refresh()
	require.Equal(t, []string{"u:pw2"}, first.recorded())
	require.Equal(t, []string{"u:pw2"}, second.recorded())

	require.NoError(t, unsubFirst())
	require.NoError(t, os.WriteFile(credFile, []byte("pw3"), 0o600))
	provider.Refresh()
	require.Equal(t, []string{"u:pw2"}, first.recorded(), "unsubscribed listeners hear nothing")
	require.Equal(t, []string{"u:pw2", "u:pw3"}, second.recorded())
}

func TestAuthAgainstMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("correct-pw")

	good, err := New(Config{Addresses: []string{mr.Addr()}, Password: "correct-pw"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = good.Close() })
	require.NoError(t, good.Ping(context.Background()))

	bad, err := New(Config{Addresses: []string{mr.Addr()}, Password: "wrong-pw"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bad.Close() })
	require.Error(t, bad.Ping(context.Background()))
}

// Rotation smoke over miniredis: rotate the server password and push the new
// credentials through the provider with the pool live — operations keep
// succeeding on the SAME connection (an instrumented dialer counts exactly one
// dial). If the provider pushed wrong credentials, the in-place re-AUTH
// would fail loudly. The full acceptance proof (ACL users, CLIENT LIST
// user= flip on a stable CLIENT ID) needs a real server — see
// valkey_rotation_test.go, which requires CONFIG/CLIENT/ACL that miniredis
// does not implement.
func TestLiveRotationSmokeOverMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("pw1")
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Path: credFile})

	var dials atomic.Int64
	client := redis.NewClient(&redis.Options{
		Addr:                         mr.Addr(),
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
	store, err := NewWithClient(client, "rot")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	op := func() error { return store.SetHeight(ctx, core.HeightKey("ETH1", "0xh"), 1, time.Minute) }
	require.NoError(t, op())
	require.Positive(t, provider.subscriberCount(), "the pooled connection subscribed to the provider")

	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	mr.RequireAuth("pw2")
	provider.Refresh()

	for i := 0; i < 6; i++ {
		require.NoError(t, op(), "operations must continue through the rotation")
		time.Sleep(20 * time.Millisecond)
	}

	// go-redis re-auths a marked connection in the background (the worker
	// awaits the conn's IDLE state) and may dial replacement connections while
	// the cycle runs, so an exact dial count is timing-dependent — the
	// guaranteed properties are that ops never fail (above) and that the pool
	// CONVERGES: once re-auth completes, serving ops costs no further dials. A
	// broken rotation (bad push, failed re-AUTH) never converges — every op
	// keeps redialing. Each probe starts with a quiet window so the re-auth
	// worker can win the IDLE-state race against our own probing (relevant
	// under -race slowdown).
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
