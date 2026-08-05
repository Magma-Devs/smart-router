package redisstore

import (
	"os"
	"strings"
	"sync"
	"time"

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
// surrounding whitespace/newlines are trimmed. Kubernetes-mounted secrets and
// sidecar token refreshers rotate by rewriting the file.
type FileCredentials struct {
	Username string
	Path     string
}

func (f *FileCredentials) Credentials() (string, string, error) {
	raw, err := readCredentialFile(f.Path)
	if err != nil {
		return "", "", err
	}
	if user, pass, found := strings.Cut(raw, ":"); found {
		return user, pass, nil
	}
	return f.Username, raw, nil
}

func trimCredential(raw string) string {
	return strings.TrimRight(raw, "\r\n \t")
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
type StreamingProvider struct {
	source CredentialsSource

	mu        sync.Mutex
	listeners map[int]auth.CredentialsListener
	nextID    int
	lastRaw   string
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
		utils.LavaFormatWarning("resp-cache credential refresh failed; keeping previous credentials", err)
		return
	}
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
