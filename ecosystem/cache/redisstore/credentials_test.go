package redisstore

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
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

// utf8BOMBytes is the byte order mark as a file holds it, spelled in bytes so
// no editor can drop it from this source.
var utf8BOMBytes = string([]byte{0xEF, 0xBB, 0xBF})

// The trim contract, row by row. MAG-3685: whitespace on the LEFT of the file
// was sent as part of the credential while the right side was trimmed, which
// is what made the report easy to dismiss as fixed. Each half of the combined
// form is trimmed on its own, and a leading byte order mark goes too. The
// cutset is the four ASCII bytes the right-hand trim always stripped, not
// Unicode White_Space: the last two rows pin that, so a switch to TrimSpace
// fails here rather than as WRONGPASS on an upgrade.
func TestFileCredentialsParsing(t *testing.T) {
	nbsp, ideographicSpace := string(rune(0x00A0)), string(rune(0x3000))
	cases := []struct {
		name, content, username, wantUser, wantPass string
	}{
		{"password only", "placeholder-credential\n", "fixed-user", "fixed-user", "placeholder-credential"},
		{"username:password rotates the user too", "rotated-user:rotated-pass\n", "ignored", "rotated-user", "rotated-pass"},
		{"leading space", " placeholder-credential\n", "fixed-user", "fixed-user", "placeholder-credential"},
		{"leading newline", "\nplaceholder-credential\n", "fixed-user", "fixed-user", "placeholder-credential"},
		{"leading tab", "\tplaceholder-credential", "fixed-user", "fixed-user", "placeholder-credential"},
		{"CRLF on each side", "\r\n placeholder-credential \r\n", "fixed-user", "fixed-user", "placeholder-credential"},
		{"combined form with a leading space", " rotated-user:rotated-pass\n", "ignored", "rotated-user", "rotated-pass"},
		{"space after the colon", "cacheuser: placeholder-credential\n", "ignored", "cacheuser", "placeholder-credential"},
		{"space before the colon", "cacheuser :placeholder-credential\n", "ignored", "cacheuser", "placeholder-credential"},
		{"UTF-8 BOM before the password", utf8BOMBytes + "placeholder-credential\n", "fixed-user", "fixed-user", "placeholder-credential"},
		{"UTF-8 BOM before the username", utf8BOMBytes + "cacheuser:placeholder-credential\n", "ignored", "cacheuser", "placeholder-credential"},
		{"trailing non-breaking space is part of the credential", "placeholder-credential" + nbsp + "\n", "fixed-user", "fixed-user", "placeholder-credential" + nbsp},
		{"leading ideographic space is part of the credential", ideographicSpace + "placeholder-credential", "fixed-user", "fixed-user", ideographicSpace + "placeholder-credential"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &FileCredentials{Username: tc.username, Path: writeTempFile(t, "cred", tc.content)}
			user, pass, err := src.Credentials()
			require.NoError(t, err)
			require.Equal(t, tc.wantUser, user)
			require.Equal(t, tc.wantPass, pass)
		})
	}
}

// A file the operator pointed at never means "no password". Empty once
// trimmed, it used to yield a credential of "" that New accepted and go-redis
// sent as a HELLO with no AUTH: NOAUTH on every command against a server that
// requires one, an unauthenticated connection against one that does not.
func TestFileCredentialsRefuseAnEmptyFile(t *testing.T) {
	for _, tc := range []struct{ name, content string }{
		{"empty", ""},
		{"whitespace only", " \n\t\r\n"},
		{"BOM only", utf8BOMBytes},
		{"BOM and whitespace", utf8BOMBytes + " \n"},
		{"combined form with an empty password", "cacheuser:\n"},
		{"combined form with a blank password", "cacheuser: \n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempFile(t, "cred", tc.content)
			_, _, err := (&FileCredentials{Username: "u", Path: path}).Credentials()
			require.ErrorContains(t, err, path, "the error names the file")
		})
	}
}

// The startup fail-fast refuses an empty file naming the path, on both
// credential files.
func TestNewRefusesAnEmptyCredentialFile(t *testing.T) {
	t.Run("password-file", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireAuth("placeholder-credential")
		path := writeTempFile(t, "pw", " \n")
		_, err := New(Config{Addresses: []string{mr.Addr()}, PasswordFile: path})
		require.ErrorContains(t, err, path)
	})
	t.Run("sentinel-password-file", func(t *testing.T) {
		path := writeTempFile(t, "sentinel-pw", "\n\t")
		_, err := New(Config{Topology: TopologySentinel, Addresses: []string{"127.0.0.1:26379"}, MasterName: "mymaster", SentinelPasswordFile: path})
		require.ErrorContains(t, err, path)
	})
}

// Mid-rotation a mounted secret can be empty for a moment. The refresh path
// treats that as a failed read: nothing is pushed, the connections keep what
// they have, the failure names the file, and the rotation that follows lands.
func TestRefreshKeepsPreviousCredentialsWhenTheFileIsEmpty(t *testing.T) {
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})
	listener := &recordingListener{}
	creds, _, err := provider.Subscribe(listener)
	require.NoError(t, err)
	require.Equal(t, "u:pw1", creds.RawCredentials())

	require.NoError(t, os.WriteFile(credFile, []byte(" \n"), 0o600))
	out := captureLog(t, provider.Refresh)
	require.Empty(t, listener.recorded(), "an empty file pushes nothing")
	require.Contains(t, out, credFile, "the failed read names the file")

	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	provider.Refresh()
	require.Equal(t, []string{"u:pw2"}, listener.recorded(), "the rotation that follows lands")
}

// An empty file mid-rotation (a secret mount being rewritten) is a source
// outage like any other under the once-per-outage report: however many reads
// see it, one warning; a connection opened meanwhile is handed the last
// credentials read rather than failed; the recovery is one line, and a
// rotation that lands with it reaches every connection.
func TestEmptyCredentialFileIsOneOutage(t *testing.T) {
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})
	listener := &recordingListener{}
	creds, unsubscribe, err := provider.Subscribe(listener)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unsubscribe() })
	require.Equal(t, "u:pw1", creds.RawCredentials())

	require.NoError(t, os.WriteFile(credFile, []byte(" \n"), 0o600))
	late := &recordingListener{}
	out := captureLog(t, func() {
		for i := 0; i < 5; i++ {
			provider.Refresh()
		}
		lateCreds, unsubscribeLate, err := provider.Subscribe(late)
		require.NoError(t, err, "a connection opened during the outage is not failed on the empty file")
		t.Cleanup(func() { _ = unsubscribeLate() })
		require.Equal(t, "u:pw1", lateCreds.RawCredentials(), "it is handed the last credentials read")
	})
	require.Equal(t, 1, strings.Count(out, "credential source unreadable"), "five reads and a subscribe, one line: %s", out)
	require.Contains(t, out, credFile, "the line names the file")
	require.Empty(t, listener.recorded(), "nothing is pushed during the outage")

	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	out = captureLog(t, provider.Refresh)
	require.Equal(t, 1, strings.Count(out, "readable again"), "the recovery is one line: %s", out)
	require.Equal(t, []string{"u:pw2"}, listener.recorded(), "the rotation that lands with the recovery is pushed")
	require.Equal(t, []string{"u:pw2"}, late.recorded(), "to the connection opened during the outage too")
}

// A credential that begins with a rune no trim removes is sent as written,
// and the router says so once, naming the file and the code point and never
// the value.
func TestFileCredentialsWarnOnceAboutAnInvisibleLeadingRune(t *testing.T) {
	zeroWidthSpace, nbsp := string(rune(0x200B)), string(rune(0x00A0))
	t.Run("zero-width space before the password", func(t *testing.T) {
		path := writeTempFile(t, "cred", zeroWidthSpace+"placeholder-credential\n")
		src := &FileCredentials{Username: "u", Path: path}
		out := captureLog(t, func() {
			for i := 0; i < 3; i++ {
				_, pass, err := src.Credentials()
				require.NoError(t, err)
				require.Equal(t, zeroWidthSpace+"placeholder-credential", pass, "sent as written")
			}
		})
		require.Equal(t, 1, strings.Count(out, "non-printable rune"), "three reads, one line: %s", out)
		require.Contains(t, out, "U+200B")
		require.Contains(t, out, path)
		require.NotContains(t, out, "placeholder-credential", "the value never reaches the log")
	})
	t.Run("non-breaking space before the username", func(t *testing.T) {
		path := writeTempFile(t, "cred", nbsp+"cacheuser:placeholder-credential\n")
		src := &FileCredentials{Username: "ignored", Path: path}
		out := captureLog(t, func() {
			user, _, err := src.Credentials()
			require.NoError(t, err)
			require.Equal(t, nbsp+"cacheuser", user)
		})
		require.Contains(t, out, "U+00A0")
		require.Contains(t, out, "username")
	})
	t.Run("control: a clean file says nothing", func(t *testing.T) {
		src := &FileCredentials{Username: "u", Path: writeTempFile(t, "cred", " placeholder-credential\n")}
		out := captureLog(t, func() {
			_, _, err := src.Credentials()
			require.NoError(t, err)
		})
		require.NotContains(t, out, "non-printable rune")
	})
}

// One store reads its credential file through one FileCredentials, so the
// once-only line about the combined form fires once per startup. New used to
// build one for its fail-fast and another for the provider, and the line
// fired for each.
func TestCombinedFormIsReportedOncePerStartup(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireUserAuth("cacheuser", "placeholder-credential")
	out := captureLog(t, func() {
		store, err := New(Config{Addresses: []string{mr.Addr()}, PasswordFile: writeTempFile(t, "userpw", "cacheuser:placeholder-credential\n")})
		require.NoError(t, err)
		require.NoError(t, store.Ping(context.Background()))
		require.NoError(t, store.Close())
	})
	require.Equal(t, 1, strings.Count(out, "is read as"), "one file, one line: %s", out)
}

// The same defect end to end: the store only accepts the exact credential, so
// a leading space that survived into the AUTH would be refused. Both file
// forms, against a server that requires each, with a wrong password and a
// wrong username as the controls that show the server deciding.
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

	t.Run("control: a wrong password in the file is refused by the server", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireAuth("placeholder-credential")
		store, err := New(Config{
			Addresses:    []string{mr.Addr()},
			PasswordFile: writeTempFile(t, "pw", " not-the-credential\n"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		require.ErrorContains(t, store.Ping(context.Background()), "WRONGPASS", "the server must be the one deciding, or the cases above prove nothing")
	})

	t.Run("control: a wrong username in the combined form is refused by the server", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireUserAuth("cacheuser", "placeholder-credential")
		store, err := New(Config{
			Addresses:    []string{mr.Addr()},
			PasswordFile: writeTempFile(t, "userpw", " other-user:placeholder-credential\n"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		require.ErrorContains(t, store.Ping(context.Background()), "WRONGPASS", "the username half is what the server refuses here")
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

	const failed, recovered = "credential source unreadable", "readable again; refresh resumed"

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

	// A change of cause mid-outage is news, and is reported once more: the
	// path now exists but cannot be read as a file.
	require.NoError(t, os.Mkdir(credFile, 0o700))
	cause := captureLog(t, func() {
		provider.Refresh()
		provider.Refresh()
	})
	require.Equal(t, 1, strings.Count(cause, failed), "a new cause within the same outage is one more line, not one per tick")
	require.Contains(t, cause, "is a directory")
	require.NotContains(t, cause, recovered, "the outage did not end")
	require.NoError(t, os.Remove(credFile))

	// Recovery with ROTATED contents both closes the outage and pushes.
	require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
	rotated := captureLog(t, func() { provider.Refresh() })
	require.Equal(t, 1, strings.Count(rotated, recovered))
	require.Equal(t, []string{"u:pw2"}, listener.recorded(), "a rotation that lands during recovery is applied")
}

// Close must not return while a watcher tick can still run: the tick logs
// and pushes, and a store that has been closed should do neither. The loop
// closes its done channel only on the way out, so the channel being closed
// when Close returns is the contract itself.
func TestCloseWaitsForTheCredentialWatcher(t *testing.T) {
	store, err := New(Config{
		Addresses:                 []string{"127.0.0.1:6379"},
		PasswordFile:              writeTempFile(t, "pw", "placeholder-credential\n"),
		CredentialRefreshInterval: time.Millisecond,
	})
	require.NoError(t, err)
	done := store.watcherDone
	require.NotNil(t, done, "a file-backed standalone store runs the watcher")
	time.Sleep(10 * time.Millisecond) // let a few ticks run so a tick can be in flight
	require.NoError(t, store.Close())
	select {
	case <-done:
	default:
		t.Fatal("Close returned before the credential watcher did")
	}
	// A second Close must not close the stop channel twice or wait on a
	// watcher that already returned; what go-redis answers for its own
	// already-closed client is its business.
	_ = store.Close()
}

// MAG-3690 review: "keeps the credentials it already holds" was true of the
// live connections only. go-redis opens connections throughout an outage,
// pool growth, a reconnect after a network blip, an idle replacement, and
// each one's Subscribe re-read the file and failed its setup on the read
// error, which reached no log line at all. A connection opened during an
// outage is now handed the last credentials any read returned, and still
// receives the rotation that ends the outage.
func TestSubscribeDuringSourceOutageHandsOutLastGoodCredentials(t *testing.T) {
	const failed, recovered = "credential source unreadable", "readable again; refresh resumed"
	// A subscription is held for as long as its unsubscribe closure is, the
	// way go-redis holds it on the connection; every subscription asserted on
	// below keeps its closure for the test's life.
	hold := func(t *testing.T, unsubscribe auth.UnsubscribeFunc) {
		t.Helper()
		t.Cleanup(func() { _ = unsubscribe() })
	}

	t.Run("before any successful read the error stands", func(t *testing.T) {
		provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: "/does/not/exist"})
		out := captureLog(t, func() {
			for i := 0; i < 3; i++ {
				_, _, err := provider.Subscribe(&recordingListener{})
				require.ErrorContains(t, err, "/does/not/exist", "nothing to hand out yet: this is New's fail-fast")
			}
		})
		require.Equal(t, 1, strings.Count(out, failed), "the outage is still reported once, whoever observes it")
		require.Equal(t, 0, provider.subscriberCount(), "a failed subscription registers nothing")
	})

	t.Run("during an outage a connection gets the last set read and the outage is one event", func(t *testing.T) {
		credFile := writeTempFile(t, "cred", "pw1")
		provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})
		early := &recordingListener{}
		creds, unsubscribe, err := provider.Subscribe(early)
		require.NoError(t, err)
		hold(t, unsubscribe)
		require.Equal(t, "u:pw1", creds.RawCredentials())

		// A rotation lands on disk and a connection reads it before the watcher
		// ticks: the newest set is what a later fallback must hand out, while
		// the watcher's baseline stays on the set the early connection has.
		require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
		creds, unsubscribe, err = provider.Subscribe(&recordingListener{})
		require.NoError(t, err)
		hold(t, unsubscribe)
		require.Equal(t, "u:pw2", creds.RawCredentials())

		require.NoError(t, os.Remove(credFile))
		late := &recordingListener{}
		out := captureLog(t, func() {
			provider.Refresh() // the watcher notices first
			creds, unsubscribe, err = provider.Subscribe(late)
			require.NoError(t, err, "a connection opened during the outage must not fail its setup")
			hold(t, unsubscribe)
			require.Equal(t, "u:pw2", creds.RawCredentials(), "the last set any read returned, not the watcher's baseline")
			for i := 0; i < 5; i++ {
				_, unsubscribe, err = provider.Subscribe(&recordingListener{})
				require.NoError(t, err)
				hold(t, unsubscribe)
			}
		})
		require.Equal(t, 1, strings.Count(out, failed), "the watcher and six subscriptions observed one outage")
		require.Equal(t, 8, provider.subscriberCount())
		require.Empty(t, early.recorded(), "nothing is pushed while the source is unreadable")

		// The outage ends with the rotation the watcher's baseline never saw:
		// one recovery line, and the push reaches the early connection (still on
		// pw1) and the late one alike.
		require.NoError(t, os.WriteFile(credFile, []byte("pw2"), 0o600))
		back := captureLog(t, func() { provider.Refresh() })
		require.Equal(t, 1, strings.Count(back, recovered))
		require.NotContains(t, back, failed)
		require.Equal(t, []string{"u:pw2"}, early.recorded(), "the connection that had the old set is re-authenticated")
		require.Equal(t, []string{"u:pw2"}, late.recorded(), "a connection that fell back still receives the push that ends the outage")
	})

	t.Run("a connection that noticed first also reports it once", func(t *testing.T) {
		credFile := writeTempFile(t, "cred", "pw1")
		provider := NewStreamingProvider(&FileCredentials{Username: "u", Path: credFile})
		_, unsubscribe, err := provider.Subscribe(&recordingListener{})
		require.NoError(t, err)
		hold(t, unsubscribe)
		require.NoError(t, os.Remove(credFile))
		out := captureLog(t, func() {
			_, unsubscribe, err = provider.Subscribe(&recordingListener{})
			require.NoError(t, err)
			hold(t, unsubscribe)
			provider.Refresh()
			provider.Refresh()
		})
		require.Equal(t, 1, strings.Count(out, failed), "the watcher's ticks add nothing to what the connection reported")
	})
}

// The same rule through go-redis itself: with the file gone, the connections
// the client opens beyond the one it already has authenticate with the last
// credentials read instead of failing their setup on the read error. Four
// blocking commands in flight at once need four connections, so three are
// new dials and new Subscribes during the outage. (go-redis's dedicated
// Conn() cannot be used here: at v9.22.0 it carries no streaming
// credentials manager and panics on init with a streaming provider.)
func TestConnectionsOpenedDuringSourceOutageAuthenticate(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("pw1")
	credFile := writeTempFile(t, "cred", "pw1")
	provider := NewStreamingProvider(&FileCredentials{Path: credFile})

	var dials atomic.Int64
	client := redis.NewClient(&redis.Options{
		Addr:                         mr.Addr(),
		StreamingCredentialsProvider: provider,
		PoolSize:                     4,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	require.NoError(t, client.Ping(ctx).Err(), "the first connection reads the file while it is there")
	require.Equal(t, int64(1), dials.Load())

	require.NoError(t, os.Remove(credFile))
	const inFlight = 4
	block := func() error {
		err := client.BLPop(ctx, 300*time.Millisecond, "a-list-nobody-pushes-to").Err()
		if errors.Is(err, redis.Nil) {
			return nil // the timeout: the command ran on an authenticated connection
		}
		return err
	}
	out := captureLog(t, func() {
		results := make(chan error, inFlight)
		for i := 0; i < inFlight; i++ {
			go func() { results <- block() }()
		}
		for i := 0; i < inFlight; i++ {
			require.NoError(t, <-results, "a command whose connection was opened during the outage must run, not fail its setup")
		}
	})
	require.Equal(t, int64(inFlight), dials.Load(), "the commands in flight forced the pool to open the other connections during the outage")
	require.Equal(t, inFlight, provider.subscriberCount(), "every connection, fallen back or not, is subscribed for the rotation that ends the outage")
	require.Equal(t, 1, strings.Count(out, "credential source unreadable"), "three failed reads, one line")
}
