package redisstore

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
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

// MAG-3685: whitespace on the LEFT of the file was sent as part of the
// credential. The right side was already trimmed, which is what made the
// report easy to dismiss as fixed — the defect was the side, not the absence.
func TestFileCredentialsTrimBothSides(t *testing.T) {
	for name, content := range map[string]string{
		"leading space":     " placeholder-credential\n",
		"leading newline":   "\nplaceholder-credential\n",
		"leading tab":       "\tplaceholder-credential",
		"CRLF on each side": "\r\n placeholder-credential \r\n",
	} {
		t.Run(name, func(t *testing.T) {
			src := &FileCredentials{Username: "fixed-user", Path: writeTempFile(t, "pw", content)}
			user, pass, err := src.Credentials()
			require.NoError(t, err)
			require.Equal(t, "fixed-user", user)
			require.Equal(t, "placeholder-credential", pass, "the credential must reach the store without the file's surrounding whitespace")
		})
	}

	// In the combined form the leading whitespace landed on the USERNAME: one
	// file, one trim, both halves exposed — and one trim fixes both.
	src := &FileCredentials{Username: "ignored", Path: writeTempFile(t, "userpw", " rotated-user:rotated-pass\n")}
	user, pass, err := src.Credentials()
	require.NoError(t, err)
	require.Equal(t, "rotated-user", user, "a leading space must not become part of the username")
	require.Equal(t, "rotated-pass", pass)
}

// The same defect end to end: the store only accepts the exact credential, so
// a leading space that survived into the AUTH would be refused. Both file
// forms, against a server that requires each.
func TestPasswordFileWithLeadingWhitespaceAuthenticates(t *testing.T) {
	t.Run("password-only file", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireAuth("placeholder-credential")
		store, err := New(Config{
			Addresses:    []string{mr.Addr()},
			PasswordFile: writeTempFile(t, "pw", "\n placeholder-credential\n"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		require.NoError(t, store.Ping(context.Background()), "the trimmed password must be what reaches the store")
	})

	t.Run("username:password file", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireUserAuth("cacheuser", "placeholder-credential")
		store, err := New(Config{
			Addresses:    []string{mr.Addr()},
			PasswordFile: writeTempFile(t, "userpw", " cacheuser:placeholder-credential\n"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		require.NoError(t, store.Ping(context.Background()), "the trimmed username must be what reaches the store")
	})

	t.Run("control: a wrong credential in the file is still refused", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireAuth("placeholder-credential")
		store, err := New(Config{
			Addresses:    []string{mr.Addr()},
			PasswordFile: writeTempFile(t, "pw", " not-the-credential\n"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		require.Error(t, store.Ping(context.Background()), "the server must be the one deciding, or the cases above prove nothing")
	})
}

// The plumbing contract: a push reaches every subscribed connection exactly
// when the credentials actually changed; unsubscribed listeners never hear
// again.
// Under sentinel the data-node credentials go through CredentialsProviderContext,
// not the streaming provider, so the provider has no subscribers and a rotation
// watcher can only ever log "connections=0" — which reads to an operator as
// "re-authenticated in place", the one thing that did not happen. No watcher is
// started there; rotation applies on reconnect.
func TestSentinelPasswordFileStartsNoWatcher(t *testing.T) {
	passwordFile := writeTempFile(t, "data-pass", "placeholder-not-a-real-credential\n")

	sentinel, err := New(Config{
		Topology:     TopologySentinel,
		Addresses:    []string{"127.0.0.1:26379"},
		MasterName:   "mymaster",
		Username:     "cacheuser",
		PasswordFile: passwordFile,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sentinel.Close() })
	require.Nil(t, sentinel.stopWatcher,
		"a watcher under sentinel would detect every rotation and re-authenticate nothing")
	require.Nil(t, sentinel.credentials,
		"nothing subscribes under sentinel, so no provider is built for it either")

	standalone, err := New(Config{
		Topology:     TopologyStandalone,
		Addresses:    []string{"127.0.0.1:6379"},
		Username:     "cacheuser",
		PasswordFile: passwordFile,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = standalone.Close() })
	require.NotNil(t, standalone.stopWatcher,
		"standalone uses the streaming provider, where in-place re-auth does work")
}

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
	// Two collections first: the pooled connection's subscription is anchored
	// only by the closure the client stored on it, and must survive them
	// (MAG-3728) — or the rotation below reaches nobody.
	runtime.GC()
	runtime.GC()
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

// captureLog swaps the global zerolog sink for a buffer while fn runs and
// returns what was written. lavalog writes through that global logger and its
// level gate defaults to debug in tests, so warnings and info both land.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	prev := zerologlog.Logger
	t.Cleanup(func() { zerologlog.Logger = prev })
	var buf bytes.Buffer
	zerologlog.Logger = zerolog.New(&buf)
	fn()
	return buf.String()
}

// MAG-3690: an unreadable credential file was reported on every tick of the
// refresh loop, indefinitely — 21 identical warnings in 22 seconds at a
// one-second interval, about 8,600 a day at the shipped ten seconds — while
// the health loop beside it wrote once over the same window, because it
// reports transitions. This holds Refresh to that discipline: one line when
// the source becomes unreadable, one when it is readable again, nothing in
// between, and a later outage is a new event.
func TestRefreshReportsSourceFailureOncePerOutage(t *testing.T) {
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})
	listener := &recordingListener{}
	_, _, err := provider.Subscribe(listener)
	require.NoError(t, err)

	const failed, recovered = "credential refresh failed", "readable again"

	quiet := captureLog(t, func() { provider.Refresh() })
	require.NotContains(t, quiet, failed, "a readable source is not an event")
	require.NotContains(t, quiet, recovered)

	require.NoError(t, os.Remove(credFile))
	outage := captureLog(t, func() {
		for i := 0; i < 21; i++ {
			provider.Refresh()
		}
	})
	require.Equal(t, 1, strings.Count(outage, failed), "one warning per outage, however many ticks it spans")
	require.Empty(t, listener.recorded(), "nothing is pushed while the source is unreadable")

	require.NoError(t, os.WriteFile(credFile, []byte("pw1"), 0o600))
	back := captureLog(t, func() {
		provider.Refresh()
		provider.Refresh()
	})
	require.Equal(t, 1, strings.Count(back, recovered), "the end of the outage is reported once")
	require.NotContains(t, back, failed)
	require.Empty(t, listener.recorded(), "unchanged credentials still do not push after recovery")

	require.NoError(t, os.Remove(credFile))
	again := captureLog(t, func() {
		provider.Refresh()
		provider.Refresh()
	})
	require.Equal(t, 1, strings.Count(again, failed), "a second outage is a new event")

	// Recovery with ROTATED contents both closes the outage and pushes.
	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	rotated := captureLog(t, func() { provider.Refresh() })
	require.Equal(t, 1, strings.Count(rotated, recovered))
	require.Equal(t, []string{"u:pw2"}, listener.recorded(), "a rotation that lands during recovery is applied")
}

// MAG-3728: go-redis abandons a connection whose setup failed without ever
// calling Close on it, so the unsubscribe it stored on the connection never
// runs; the provider must therefore not be what keeps such a connection alive.
// A subscription is anchored by its unsubscribe closure alone: while something
// holds that closure (a live pooled connection does) the subscription is live,
// and once nothing does, the collector reclaims it and the provider forgets it.
func TestSubscriptionLivesAsLongAsItsUnsubscribeClosure(t *testing.T) {
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})

	anchored := &recordingListener{}
	_, keepAlive, err := provider.Subscribe(anchored)
	require.NoError(t, err)
	_, _, err = provider.Subscribe(&recordingListener{}) // its closure is dropped at once: an abandoned connection
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		runtime.GC()
		return provider.subscriberCount() == 1
	}, 5*time.Second, 20*time.Millisecond, "the subscription nobody anchors is reclaimed; the anchored one stays")

	// The anchored subscription still receives rotations across collections.
	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	runtime.GC()
	provider.Refresh()
	require.Equal(t, []string{"u:pw2"}, anchored.recorded())

	require.NoError(t, keepAlive())
	require.Equal(t, 0, provider.subscriberCount(), "an explicit unsubscribe still removes the subscription at once")
}

// cutoffListener stands in for a store cut off mid-outage, in both shapes the
// ticket measured: it accepts every connection and either closes it at once
// (the store closed or reset it) or holds it open and never answers (the
// store is black-holed). Either way HELLO fails after the connection has
// subscribed, and go-redis abandons it in the closed state without running
// its close path.
type cutoffListener struct {
	listener net.Listener
	accepted atomic.Int64

	mu   sync.Mutex
	held []net.Conn
}

func newCutoffListener(t *testing.T, hold bool) *cutoffListener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	c := &cutoffListener{listener: lis}
	t.Cleanup(func() {
		_ = lis.Close()
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, conn := range c.held {
			_ = conn.Close()
		}
	})
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			c.accepted.Add(1)
			if !hold {
				_ = conn.Close()
				continue
			}
			c.mu.Lock()
			c.held = append(c.held, conn)
			c.mu.Unlock()
		}
	}()
	return c
}

// The defect as measured, in-process: a store that fails every connection, a
// client whose every dial therefore fails during setup, and a provider that
// must not end up holding the wreckage. Before this fix the subscriber count
// climbed with every failed dial and never came down — 3,510 on the ticket,
// each one holding 64 KiB and a socket. Here it returns to zero once the
// collector has run, because nothing anchors those subscriptions any more.
// Both outage shapes take the same go-redis path (a failed HELLO of any
// non-Redis kind abandons the connection), and both are covered.
func TestAbandonedConnectionsAreNotKeptByTheProvider(t *testing.T) {
	for name, hold := range map[string]bool{
		"store closes every connection":                  false,
		"store holds every connection and never answers": true,
	} {
		t.Run(name, func(t *testing.T) {
			cutoff := newCutoffListener(t, hold)
			store, err := New(Config{
				Addresses:    []string{cutoff.listener.Addr().String()},
				PasswordFile: writeTempFile(t, "pw", "placeholder-credential\n"),
				DialTimeout:  200 * time.Millisecond,
				ReadTimeout:  200 * time.Millisecond,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			require.NotNil(t, store.credentials, "a file-backed credential is what the streaming provider exists for")

			for i := 0; i < 20; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_ = store.SetHeight(ctx, core.HeightKey("ETH1", fmt.Sprintf("0x%d", i)), 1, time.Minute)
				cancel()
			}
			require.Positive(t, cutoff.accepted.Load(), "the client did dial the store, and every setup failed")

			require.Eventually(t, func() bool {
				runtime.GC()
				return store.credentials.subscriberCount() == 0
			}, 10*time.Second, 50*time.Millisecond,
				"connections abandoned after a failed setup must be reclaimed, not kept by the provider")
		})
	}
}
