package redisstore

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
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
// or "username:password" to rotate the username too (ACL-user rotation);
// whitespace and newlines on BOTH sides are trimmed (see trimCredential).
// Kubernetes-mounted secrets and sidecar token refreshers rotate by rewriting
// the file.
//
// CONSTRAINT: the combined form makes the first ":" a separator unconditionally,
// so a password that CONTAINS a colon cannot be expressed in this file. Such a
// file authenticates as the username before the colon and the remainder as the
// password, which fails closed — but as an opaque WRONGPASS, and the auth-error
// path deliberately withholds the server's reply, so nothing points at the
// cause. warnOnce below leaves that breadcrumb.
type FileCredentials struct {
	Username string
	Path     string

	warnedCombined sync.Once
}

func (f *FileCredentials) Credentials() (string, string, error) {
	raw, err := readCredentialFile(f.Path)
	if err != nil {
		return "", "", err
	}
	if user, pass, found := strings.Cut(raw, ":"); found {
		// Logged once, not per call: this runs on every connection attempt.
		// The password half is never logged. Stated rather than warned about:
		// the combined form is legitimate and documented, so this confirms the
		// interpretation for whoever meant it and is the only breadcrumb for
		// whoever did not.
		f.warnedCombined.Do(func() {
			utils.LavaFormatInfo("resp-cache password file contains ':' and is read as \"username:password\"; a password that itself contains a colon cannot be expressed in this file",
				utils.LogAttr("path", f.Path),
				utils.LogAttr("parsed-username", user),
			)
		})
		return user, pass, nil
	}
	return f.Username, raw, nil
}

// trimCredential strips whitespace from both sides of the file's contents.
//
// It used to trim the right side only, so a trailing newline from an editor
// was dropped while a leading space or blank line was sent as part of the
// credential — and in the "username:password" form it landed on the USERNAME.
// The store answered WRONGPASS, the auth path deliberately withholds the
// server's reply, and nothing pointed at the file's formatting (MAG-3685).
//
// The trade this makes is already the one that shipped: a credential ending in
// whitespace could never be expressed in this file, and one beginning with it
// now cannot either. Neither is a credential anyone writes on purpose, and the
// one-sided trim was the surprising half.
func trimCredential(raw string) string {
	return strings.TrimSpace(raw)
}

func readCredentialFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return trimCredential(string(raw)), nil
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
	lastRaw   string
	// refreshFailing records that the last Refresh could not read the source,
	// so an outage is reported once when it starts and once when it ends — the
	// health loop's transition discipline — rather than on every tick.
	refreshFailing bool
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
func (p *StreamingProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	username, password, err := p.source.Credentials()
	if err != nil {
		return nil, nil, err
	}
	creds := basicCredentials{username: username, password: password}
	sub := &subscription{listener: listener}

	p.mu.Lock()
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
		runtime.KeepAlive(sub)
		return nil
	}
	return creds, unsubscribe, nil
}

// liveListenersLocked returns the listeners of subscriptions whose connection
// is still reachable and forgets the rest: a collected subscription belonged
// to a connection the client abandoned without unsubscribing. Caller holds mu.
func (p *StreamingProvider) liveListenersLocked() []auth.CredentialsListener {
	listeners := make([]auth.CredentialsListener, 0, len(p.listeners))
	for id, ref := range p.listeners {
		sub := ref.Value()
		if sub == nil {
			delete(p.listeners, id)
			continue
		}
		listeners = append(listeners, sub.listener)
	}
	return listeners
}

// Refresh re-reads the source and, when the credentials changed, pushes them
// to every subscribed connection. Safe to call from any goroutine.
func (p *StreamingProvider) Refresh() {
	username, password, err := p.source.Credentials()
	if err != nil {
		p.noteRefreshFailure(err)
		return
	}
	p.noteRefreshRecovered()
	creds := basicCredentials{username: username, password: password}

	p.mu.Lock()
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

// noteRefreshFailure reports the FIRST failed read of an outage and stays quiet
// for the rest of it. Refresh runs on a timer for the life of the process, and
// a source that stays unreadable — a mount that dropped, a rotation that failed,
// a permission change — used to produce one warning per tick with no end:
// about 8,600 identical lines a day at the shipped interval, for a fact worth
// exactly one line (MAG-3690). The router keeps the credentials it already has
// either way; what changes is only how often it says so. The health loop in
// resp_cache.go reports its transitions the same way, and it is the model.
func (p *StreamingProvider) noteRefreshFailure(err error) {
	p.mu.Lock()
	alreadyFailing := p.refreshFailing
	p.refreshFailing = true
	p.mu.Unlock()
	if alreadyFailing {
		return
	}
	utils.LavaFormatWarning("resp-cache credential refresh failed; keeping previous credentials until the source is readable again (reported once per outage)", err)
}

// noteRefreshRecovered closes an outage with one line, so whoever saw the
// warning learns that it ended without having to notice an absence.
func (p *StreamingProvider) noteRefreshRecovered() {
	p.mu.Lock()
	wasFailing := p.refreshFailing
	p.refreshFailing = false
	p.mu.Unlock()
	if wasFailing {
		utils.LavaFormatInfo("resp-cache credential source readable again; refresh resumed")
	}
}

// subscriberCount reports live subscriptions (observability/tests): those
// whose connection is still reachable.
func (p *StreamingProvider) subscriberCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.liveListenersLocked())
}

// watchCredentials polls Refresh until stop closes — the rotation driver for
// file-backed sources.
func watchCredentials(provider *StreamingProvider, interval time.Duration, stop <-chan struct{}) {
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
