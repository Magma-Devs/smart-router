package redisstore

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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
type StreamingProvider struct {
	source CredentialsSource

	mu        sync.Mutex
	listeners map[int]auth.CredentialsListener
	nextID    int
	lastRaw   string
	// refreshFailing records that the last Refresh could not read the source,
	// so an outage is reported once when it starts and once when it ends — the
	// health loop's transition discipline — rather than on every tick.
	refreshFailing bool
}

var _ auth.StreamingCredentialsProvider = (*StreamingProvider)(nil)

func NewStreamingProvider(source CredentialsSource) *StreamingProvider {
	return &StreamingProvider{
		source:    source,
		listeners: map[int]auth.CredentialsListener{},
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

	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.listeners[id] = listener
	if p.lastRaw == "" {
		p.lastRaw = creds.RawCredentials()
	}
	p.mu.Unlock()

	unsubscribe := func() error {
		p.mu.Lock()
		delete(p.listeners, id)
		p.mu.Unlock()
		return nil
	}
	return creds, unsubscribe, nil
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
	listeners := make([]auth.CredentialsListener, 0, len(p.listeners))
	for _, l := range p.listeners {
		listeners = append(listeners, l)
	}
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

// subscriberCount reports live subscriptions (observability/tests).
func (p *StreamingProvider) subscriberCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.listeners)
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
