package redisstore

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
	"weak"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/redis/go-redis/v9/auth"
)

// CredentialsSource yields the CURRENT data-node credentials. Static and
// file-backed sources are built in; anything else (e.g. an IAM token signer)
// implements this interface and plugs into the same streaming path — no cloud
// SDK coupling in the router.
type CredentialsSource interface {
	Credentials() (username, password string, err error)
}

// StaticCredentials never change.
type StaticCredentials struct {
	Username string
	Password string
}

func (s StaticCredentials) Credentials() (string, string, error) {
	return s.Username, s.Password, nil
}

// FileCredentials re-reads Path on every call. The file holds the password,
// or "username:password" to rotate the username too (ACL-user rotation).
// What is trimmed is exactly what readCredentialFile and trimCredential say:
// one leading UTF-8 byte order mark, then the four ASCII whitespace bytes on
// both sides of the file and of each half of the combined form; every other
// byte is the credential. Kubernetes-mounted secrets and sidecar token
// refreshers rotate by rewriting the file.
//
// CONSTRAINT: the combined form makes the first ":" a separator unconditionally,
// so a password that CONTAINS a colon cannot be expressed in this file. Such a
// file authenticates as the username before the colon and the remainder as the
// password, which fails closed — but as an opaque WRONGPASS, and the auth-error
// path deliberately withholds the server's reply, so nothing points at the
// cause. warnedCombined below leaves that breadcrumb.
type FileCredentials struct {
	Username string
	Path     string

	warnedCombined  sync.Once
	warnedInvisible sync.Once
}

func (f *FileCredentials) Credentials() (string, string, error) {
	raw, err := readCredentialFile(f.Path)
	if err != nil {
		return "", "", err
	}
	username, password := f.Username, raw
	if user, pass, found := strings.Cut(raw, ":"); found {
		// Each half is trimmed on its own. The file's edges already were, but
		// the YAML habit of a space after the colon ("cacheuser: secret") rode
		// on the password half, which is never logged, while the line below
		// named a username that was clean (MAG-3685 review).
		username, password = trimCredential(user), trimCredential(pass)
		// Logged once, not per call: this runs on every connection attempt.
		// The password half is never logged. Stated rather than warned about:
		// the combined form is legitimate and documented, so this confirms the
		// interpretation for whoever meant it and is the only breadcrumb for
		// whoever did not.
		f.warnedCombined.Do(func() {
			utils.LavaFormatInfo("resp-cache password file contains ':' and is read as \"username:password\"; a password that itself contains a colon cannot be expressed in this file",
				utils.LogAttr("path", f.Path),
				utils.LogAttr("parsed-username", username),
			)
		})
	}
	if password == "" {
		// The file was configured, so an empty result never means "no
		// password": a HELLO without AUTH answers NOAUTH on every command
		// against a server that requires one, and connects unauthenticated
		// against one that does not, under a credential file the operator
		// believes is in force. Refused here, so New fails fast at startup and
		// Refresh keeps the previous credentials (a mounted secret can be
		// empty for a moment mid-rotation).
		return "", "", fmt.Errorf("credential file %s holds no password once its whitespace is trimmed", f.Path)
	}
	if field, r, ok := invisibleStart(username, password); ok {
		f.warnedInvisible.Do(func() { warnCredentialBeginsInvisibly(f.Path, field, r) })
	}
	return username, password, nil
}

// credentialCutset is what trimCredential strips from both ends: the four
// ASCII whitespace bytes an editor, an echo or a CRLF save leaves behind, and
// exactly the set the right-hand trim has stripped since the file was
// introduced. Not Unicode White_Space: a credential ending in a non-breaking
// space authenticated on v1.5.x, and widening the set would have turned it
// into WRONGPASS on upgrade, behind the same withheld reply this change is
// about (MAG-3685 review). A credential that begins with anything invisible
// outside the set is reported once by warnCredentialBeginsInvisibly instead.
const credentialCutset = "\r\n \t"

// utf8BOM is the byte order mark Windows Notepad ("UTF-8 with BOM") and
// PowerShell 5's Out-File put at the start of a file. It is not whitespace to
// any trim, and in the parsed-username line it rendered as nothing at all.
const utf8BOM = "\uFEFF"

// trimCredential strips credentialCutset from both sides of a credential.
//
// It used to trim the right side only, so a trailing newline from an editor
// was dropped while a leading space or blank line was sent as part of the
// credential — and in the "username:password" form it landed on the USERNAME.
// The store answered WRONGPASS, the auth path deliberately withholds the
// server's reply, and nothing pointed at the file's formatting (MAG-3685).
//
// The trade this makes is the one that shipped, made two-sided: a credential
// ending in one of the four bytes could never be expressed in this file, and
// one beginning with them now cannot either. Neither is a credential anyone
// writes on purpose, and the one-sided trim was the surprising half. The right
// side is byte for byte what it was.
func trimCredential(raw string) string {
	return strings.Trim(raw, credentialCutset)
}

// readCredentialFile reads one credential file the way both credential paths
// read theirs, the data-node file and the sentinel control-plane file: one
// leading byte order mark dropped, credentialCutset trimmed from both ends,
// and an empty result refused, since a file the operator pointed at never
// means "no credential".
func readCredentialFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	trimmed := trimCredential(strings.TrimPrefix(string(raw), utf8BOM))
	if trimmed == "" {
		return "", fmt.Errorf("credential file %s is empty once its whitespace is trimmed", path)
	}
	return trimmed, nil
}

// invisibleStart reports the first of the two halves that begins with a rune
// no trim removes and no log shows — a zero-width space, a non-breaking
// space, a second byte order mark — pasted in with the value and sent as
// part of it. The value itself never leaves this function; the code point is
// what names the byte.
func invisibleStart(username, password string) (field string, r rune, ok bool) {
	for _, half := range []struct{ field, value string }{{"username", username}, {"password", password}} {
		if half.value == "" {
			continue
		}
		if r, _ := utf8.DecodeRuneInString(half.value); !unicode.IsPrint(r) {
			return half.field, r, true
		}
	}
	return "", 0, false
}

func warnCredentialBeginsInvisibly(path, field string, r rune) {
	utils.LavaFormatWarning("resp-cache credential file begins with a non-printable rune that no trim removes; it is sent as part of the credential", nil,
		utils.LogAttr("path", path),
		utils.LogAttr("field", field),
		utils.LogAttr("rune", fmt.Sprintf("U+%04X", r)),
	)
}

// basicCredentials implements go-redis auth.Credentials.
type basicCredentials struct {
	username string
	password string
}

func (c basicCredentials) BasicAuth() (string, string) { return c.username, c.password }
func (c basicCredentials) RawCredentials() string      { return c.username + ":" + c.password }

// StreamingProvider adapts a CredentialsSource onto go-redis
// auth.StreamingCredentialsProvider: every connection subscribes, and when
// Refresh detects changed credentials the new values are pushed to every
// subscribed connection, which re-AUTHs IN PLACE — rotation without
// disconnect or redial. A poll loop (Store-owned) drives Refresh for
// file-backed sources; tests and custom integrations may call it directly.
//
// Subscriptions are held WEAKLY, anchored only by the unsubscribe closure that
// go-redis stores on the connection itself. A connection whose setup fails —
// the store closed or reset it during HELLO — is moved to the closed state and
// abandoned: the client never calls Close on it, so the unsubscribe never runs,
// and a provider holding the listener strongly kept the connection, its two
// 32 KiB buffers and its socket for the life of the process — about eight of
// them per request for as long as an outage lasted, until the router was
// killed for memory (MAG-3728). Anchored to the connection instead, a
// subscription lives exactly as long as the connection is reachable: a live
// pooled connection carries its own unsubscribe closure, so rotation still
// reaches it, and an abandoned one becomes garbage with everything it held;
// the runtime closes its socket when it reclaims it.
type StreamingProvider struct {
	source CredentialsSource

	mu        sync.Mutex
	listeners map[int]weak.Pointer[subscription]
	nextID    int
	// lastRaw is the credential set the subscribers were last given, the
	// baseline Refresh compares a read against to decide whether to push.
	lastRaw string
	// lastGood is the most recent set ANY read returned, and haveLastGood
	// whether there has been one. It can run ahead of lastRaw: a connection
	// that subscribes between a rotation landing on disk and the watcher's
	// next tick reads the new set first, and lastRaw must stay on the old one
	// so that tick still pushes to the connections that have it. lastGood is
	// what a connection opened during a source outage is handed (Subscribe).
	lastGood     basicCredentials
	haveLastGood bool
	// sourceFailingWith is the text of the error the current source outage
	// was reported with, empty while the source is readable. An outage is
	// reported once when it starts, once more if its cause changes (a mount
	// that dropped, then a permission denied) and once when it ends — the
	// health loop's transition discipline — rather than on every read. The
	// built-in sources fail with stable text; a custom source whose error text
	// varies per call reports every variation.
	sourceFailingWith string
}

var _ auth.StreamingCredentialsProvider = (*StreamingProvider)(nil)

// subscription is one subscribed connection's listener, a separate allocation
// so the provider can hold it weakly while the unsubscribe closure handed back
// to go-redis holds it strongly.
type subscription struct {
	listener auth.CredentialsListener
}

func NewStreamingProvider(source CredentialsSource) *StreamingProvider {
	return &StreamingProvider{
		source:    source,
		listeners: map[int]weak.Pointer[subscription]{},
	}
}

// Subscribe hands the connection its current credentials and registers it for
// future pushes. Called by go-redis once per connection during init.
//
// While the source is unreadable the connection is handed the last
// credentials any read returned, the set the live connections are still
// using, rather than failed: go-redis opens connections throughout an outage
// (pool growth under load, a reconnect after a network blip, an idle
// replacement) and each would otherwise fail its setup on a file the
// operator is about to restore, with the failure surfacing only as a counter.
// The read failure still counts towards the outage report, so it is one
// warning whether the watcher or a connection noticed first. Before any read
// has succeeded there is nothing to hand out and the error stands, which is
// New's fail-fast at startup.
func (p *StreamingProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	creds, err := p.readSource()
	if err != nil {
		var ok bool
		if creds, ok = p.lastGoodCredentials(); !ok {
			return nil, nil, err
		}
	}
	sub := &subscription{listener: listener}

	p.mu.Lock()
	// Every subscription forgets the collected ones first, so the registry is
	// bounded by the live connections plus those not yet collected, however
	// many handshakes an outage fails and however rarely the watcher polls.
	// This walks the whole registry under mu on every connection init, the
	// hot path of an outage, and that is acceptable only because the walk is
	// bounded by the records not yet collected: thousands between two
	// collections, microseconds per walk. Pruning from the watcher alone
	// would leave the registry to grow for a whole poll interval per outage,
	// which is the shape the Codex review of #407 found.
	p.pruneCollectedLocked()
	id := p.nextID
	p.nextID++
	p.listeners[id] = weak.Make(sub)
	if p.lastRaw == "" {
		p.lastRaw = creds.RawCredentials()
	}
	p.mu.Unlock()

	// The closure is the subscription's only strong anchor (see the type
	// comment): go-redis stores it on the connection, so the subscription is
	// reachable exactly while the connection is, and no longer.
	unsubscribe := func() error {
		p.mu.Lock()
		delete(p.listeners, id)
		p.mu.Unlock()
		// Not a no-op: this mention is what captures sub in the closure, and
		// the capture is the strong reference the weak pointer above needs.
		runtime.KeepAlive(sub)
		return nil
	}
	return creds, unsubscribe, nil
}

// pruneCollectedLocked forgets the records of subscriptions the collector has
// reclaimed: each belonged to a connection the client abandoned without
// unsubscribing. Called from every Subscribe and every Refresh, whether or
// not the credentials changed, so the registry cannot grow across outages
// while a file-backed password stays the same (Codex review of #407). Caller
// holds mu.
func (p *StreamingProvider) pruneCollectedLocked() {
	for id, ref := range p.listeners {
		if ref.Value() == nil {
			delete(p.listeners, id)
		}
	}
}

// liveListenersLocked returns the listeners of subscriptions whose connection
// is still reachable, after forgetting the rest. Caller holds mu.
func (p *StreamingProvider) liveListenersLocked() []auth.CredentialsListener {
	p.pruneCollectedLocked()
	listeners := make([]auth.CredentialsListener, 0, len(p.listeners))
	for _, ref := range p.listeners {
		if sub := ref.Value(); sub != nil {
			listeners = append(listeners, sub.listener)
		}
	}
	return listeners
}

// Refresh re-reads the source and, when the credentials changed, pushes them
// to every subscribed connection. Safe to call from any goroutine. A failed
// read pushes nothing: the subscribers keep what they have until the source
// is readable again.
func (p *StreamingProvider) Refresh() {
	creds, err := p.readSource()
	if err != nil {
		return
	}

	p.mu.Lock()
	// Housekeeping on every tick, not only on a rotation: the unchanged-file
	// case is the ordinary one for the whole life of a process.
	p.pruneCollectedLocked()
	if creds.RawCredentials() == p.lastRaw {
		p.mu.Unlock()
		return
	}
	p.lastRaw = creds.RawCredentials()
	listeners := p.liveListenersLocked()
	p.mu.Unlock()

	utils.LavaFormatInfo("resp-cache credentials rotated; re-authenticating live connections", utils.LogAttr("connections", len(listeners)))
	for _, listener := range listeners {
		listener.OnNext(creds)
	}
}

// readSource reads the source once, on behalf of Subscribe or Refresh. A
// successful read becomes the last good set and closes any outage report; a
// failed one opens or continues it. Both callers see the same outage, so it
// is reported once however many reads observe it.
func (p *StreamingProvider) readSource() (basicCredentials, error) {
	username, password, err := p.source.Credentials()
	if err != nil {
		p.noteSourceFailure(err)
		return basicCredentials{}, err
	}
	creds := basicCredentials{username: username, password: password}
	p.mu.Lock()
	p.lastGood, p.haveLastGood = creds, true
	p.mu.Unlock()
	p.noteSourceRecovered()
	return creds, nil
}

// lastGoodCredentials returns the most recent set any read returned, and
// whether there has been one.
func (p *StreamingProvider) lastGoodCredentials() (basicCredentials, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastGood, p.haveLastGood
}

// noteSourceFailure reports the FIRST failed read of an outage and stays quiet
// for the rest of it. Refresh runs on a timer for the life of the process, and
// a source that stays unreadable — a mount that dropped, a rotation that failed,
// a permission change — used to produce one warning per tick with no end:
// about 8,600 identical lines a day at the shipped interval, for a fact worth
// exactly one line (MAG-3690). The connections keep the credentials they have
// either way, and a new one gets the last set read; what changes is only how
// often the router says so. A change of cause mid-outage is one more line,
// since it is news. The health loop in resp_cache.go reports its transitions
// the same way, and it is the model.
func (p *StreamingProvider) noteSourceFailure(err error) {
	cause := err.Error()
	p.mu.Lock()
	sameCause := p.sourceFailingWith == cause
	p.sourceFailingWith = cause
	p.mu.Unlock()
	if sameCause {
		return
	}
	utils.LavaFormatWarning("resp-cache credential source unreadable; connections keep the last credentials read until it is readable again (reported once per outage)", err)
}

// noteSourceRecovered closes an outage with one line, so whoever saw the
// warning learns that it ended without having to notice an absence.
func (p *StreamingProvider) noteSourceRecovered() {
	p.mu.Lock()
	wasFailing := p.sourceFailingWith != ""
	p.sourceFailingWith = ""
	p.mu.Unlock()
	if wasFailing {
		utils.LavaFormatInfo("resp-cache credential source readable again; refresh resumed")
	}
}

// subscriberCount reports live subscriptions (observability/tests): those
// whose connection is still reachable. It prunes as it counts.
func (p *StreamingProvider) subscriberCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.liveListenersLocked())
}

// registrySize is the raw number of records held, collected or not, with no
// pruning: what a test reads to check that production's own housekeeping,
// not the counting helper, keeps the registry bounded.
func (p *StreamingProvider) registrySize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.listeners)
}

// watchCredentials polls Refresh until stop closes — the rotation driver for
// file-backed sources. done is closed on the way out, after the last tick
// has returned, for Store.Close to wait on.
func watchCredentials(provider *StreamingProvider, interval time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			provider.Refresh()
		}
	}
}
