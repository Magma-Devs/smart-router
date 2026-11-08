package utils

import (
	"encoding/json"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

// Redaction by value (MAG-4863). RedactSecrets' url shapes cannot see a key once it leaves its
// url: a node that answers "invalid api key <key>" or "Cannot POST /v2/<key>", or a scheme-less
// address with a dotless host. The router knows every secret it was configured with, so it looks
// for those values themselves, in every outgoing text that RedactSecrets covers.

// minSecretLen is the shortest value treated as a secret. Shorter than any provider key format,
// and a shorter value would start replacing ordinary words and numbers in every log line.
const minSecretLen = 8

// minDigitlessSecretLen is the shortest url path segment or query value without a digit treated
// as a secret. Network and chain names in node paths ("ethereum-mainnet", 16) have no digit and
// are not secrets; a provider key has a digit, or is long enough to pass this anyway.
const minDigitlessSecretLen = 20

type secretRedactor struct {
	values   map[string]struct{}
	replacer *strings.Replacer
}

var (
	configuredSecrets   atomic.Pointer[secretRedactor]
	configuredSecretsMu sync.Mutex
)

// RegisterSecrets adds values the configuration names as credentials (an auth header value, an
// auth-query value, a url password). Each value of at least minSecretLen characters is redacted
// from then on, in every text RedactSecrets handles. Meant for start-up; safe at any time.
func RegisterSecrets(values ...string) {
	registerSecrets(values, func(v string) bool { return len(v) >= minSecretLen })
}

// RegisterSecretCandidates adds values that sit where a credential may sit but often does not: a
// url path segment or query value. Each counts only when it looks like a key: at least
// minSecretLen characters with a digit, or minDigitlessSecretLen characters without one.
func RegisterSecretCandidates(values ...string) {
	registerSecrets(values, looksLikeSecret)
}

func looksLikeSecret(v string) bool {
	if len(v) >= minDigitlessSecretLen {
		return true
	}
	return len(v) >= minSecretLen && strings.IndexFunc(v, unicode.IsDigit) >= 0
}

func registerSecrets(values []string, counts func(string) bool) {
	configuredSecretsMu.Lock()
	defer configuredSecretsMu.Unlock()

	current := configuredSecrets.Load()
	merged := map[string]struct{}{}
	if current != nil {
		for v := range current.values {
			merged[v] = struct{}{}
		}
	}
	added := false
	for _, v := range values {
		if !counts(v) {
			continue
		}
		if _, ok := merged[v]; !ok {
			merged[v] = struct{}{}
			added = true
		}
	}
	if !added {
		return
	}
	configuredSecrets.Store(&secretRedactor{values: merged, replacer: newSecretReplacer(merged)})
}

// newSecretReplacer replaces each secret in every form it can take in outgoing text: as given,
// percent-encoded (a url, a query), and escaped as inside a JSON string, with and without escaped
// slashes. Longer forms go first, so a secret that contains another is replaced whole.
func newSecretReplacer(values map[string]struct{}) *strings.Replacer {
	forms := map[string]struct{}{}
	for v := range values {
		forms[v] = struct{}{}
		forms[url.QueryEscape(v)] = struct{}{}
		forms[url.PathEscape(v)] = struct{}{}
		if quoted, err := json.Marshal(v); err == nil {
			escaped := string(quoted[1 : len(quoted)-1])
			forms[escaped] = struct{}{}
			forms[strings.ReplaceAll(escaped, "/", `\/`)] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(forms))
	for f := range forms {
		ordered = append(ordered, f)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if len(ordered[i]) != len(ordered[j]) {
			return len(ordered[i]) > len(ordered[j])
		}
		return ordered[i] < ordered[j]
	})
	pairs := make([]string, 0, 2*len(ordered))
	for _, f := range ordered {
		pairs = append(pairs, f, RedactedMark)
	}
	return strings.NewReplacer(pairs...)
}

// redactConfiguredSecrets replaces every registered secret in s.
func redactConfiguredSecrets(s string) string {
	redactor := configuredSecrets.Load()
	if redactor == nil || s == "" {
		return s
	}
	return redactor.replacer.Replace(s)
}
