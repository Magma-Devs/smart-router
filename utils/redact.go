package utils

import (
	"regexp"
	"sort"
	"strings"
)

// RedactedMark stands in for any withheld value.
const RedactedMark = "[redacted]"

// RedactedURLMark replaces the credential-bearing tail of a url. It is a path
// segment so a redacted url still reads as a url in logs and error text.
const RedactedURLMark = "/" + RedactedMark

// urlStop and queryStop are the characters that end a url inside text. The match
// stops at ",;)" as well as at quotes and brackets so a url embedded in a log
// attribute list ("{url:https://host/p,method:eth_call}") does not swallow what
// follows it, and the host — all this needs to identify — always precedes them.
// A query is the exception for ";": Go's HTTP client sends "?a=1;apikey=<key>"
// as is, so inside a query ";" is part of the url (MAG-4863).
const (
	urlStop   = `\s"'` + "`" + `<>\\^{}|\[\],;)`
	queryStop = `\s"'` + "`" + `<>\\^{}|\[\],)`
)

// urlInText matches a scheme-qualified url inside arbitrary text. Anchoring on
// "://" keeps prose and scheme-less identifiers — a host:port listen address, a
// provider name, a "dial tcp 10.0.0.1:443" — untouched.
var urlInText = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.\-]*://[^` + urlStop + `?]+(?:\?[^` + queryStop + `]*)?`)

// escapedURLInText matches a url whose slashes are escaped, as JSON allows
// ("https:\/\/host\/v2\/<key>"). A backslash is part of the match only in "\/",
// so the one that escapes a closing quote ends it and the JSON stays valid.
var escapedURLInText = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.\-]*:\\/\\/(?:[^` + queryStop + `]|\\/)+`)

// schemeLessURLInText matches a credential-bearing scheme-less node url while
// requiring a recognizable host. That keeps ordinary prose, provider names,
// lava addresses and bare host:port values untouched.
var schemeLessURLInText = regexp.MustCompile(`\b(?:(?:localhost|(?:[a-zA-Z0-9-]+\.)+[a-zA-Z0-9-]+)(?::[0-9]+)?|[a-zA-Z0-9-]+:[0-9]+)(?:[/#][^` + urlStop + `?]*(?:\?[^` + queryStop + `]*)?|\?[^` + queryStop + `]+)`)

// RedactURL reduces one url to scheme://host[:port]. Upstream vendors carry the
// api key in the userinfo, the path (".../v2/<key>") or the query
// ("?apikey=<key>"), so everything past the host is dropped wholesale — a
// heuristic that kept "harmless-looking" segments would eventually keep a key.
//
// Scheme-less urls ("host/v2/<key>") are handled too: the config accepts them
// and the direct-connection layer resolves the scheme from the api-interface.
func RedactURL(raw string) string {
	if raw == "" {
		return raw
	}

	scheme, rest := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}

	authority := rest
	rest = ""
	if i := strings.IndexAny(authority, "/?#"); i >= 0 {
		authority, rest = authority[:i], authority[i:]
	}

	// "user:pass@host" keeps only "host".
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		authority = authority[i+1:]
	}

	if authority == "" {
		return scheme
	}
	// A bare "/" carries nothing, so it survives rather than becoming a mark.
	if rest == "" || rest == "/" {
		return scheme + authority + rest
	}
	return scheme + authority + RedactedURLMark
}

// RedactIfURL redacts s only when it looks like a url — it carries a "://"
// scheme, or a path/query/fragment delimiter. Anything else comes back
// untouched, which matters where the input is a url OR an identifier: a lava
// provider address ("provider@provider") is not a url, and RedactURL would read its
// "@" as userinfo and drop the prefix.
func RedactIfURL(s string) string {
	if strings.Contains(s, "://") || strings.ContainsAny(s, "/?#") {
		return RedactURL(s)
	}
	return s
}

// RedactSecrets withholds every secret in s: each value the configuration
// registered (RegisterSecrets), in any form, and then every url, rewritten with
// RedactURL. Use it on any text that may have picked up a secret indirectly — an
// error from net/http, whose *url.Error message embeds the full request url (Go
// masks only the userinfo password), a message that interpolated a node-url, or
// a body a node sent back.
//
// The delimiter check keeps the url pass near-free for the log lines that hold
// no url, which is nearly all of them.
func RedactSecrets(s string) string {
	s = redactConfiguredSecrets(s)
	if !strings.Contains(s, "://") && !strings.ContainsAny(s, "/?#") {
		return s
	}
	if strings.Contains(s, `:\/\/`) {
		s = escapedURLInText.ReplaceAllStringFunc(s, redactEscapedURL)
	}
	redacted := urlInText.ReplaceAllStringFunc(s, RedactURL)
	redacted = schemeLessURLInText.ReplaceAllStringFunc(redacted, RedactURL)
	if strings.Contains(redacted, RedactedURLMark+RedactedMark) {
		redacted = repeatedURLMark.ReplaceAllString(redacted, RedactedURLMark)
	}
	return redacted
}

// repeatedURLMark is a url mark followed by more marks. A registered secret inside a url is
// replaced first, and the url pass stops at the "[" of that mark, so it cuts the url just before
// the mark and leaves it standing after its own: "https://host/[redacted][redacted]".
var repeatedURLMark = regexp.MustCompile(regexp.QuoteMeta(RedactedURLMark) + `(?:` + regexp.QuoteMeta(RedactedMark) + `)+`)

// redactEscapedURL is RedactURL for a url whose slashes are escaped.
func redactEscapedURL(escaped string) string {
	return strings.ReplaceAll(RedactURL(strings.ReplaceAll(escaped, `\/`, "/")), "/", `\/`)
}

// RedactSecretsErr returns err with its message redacted, preserving the cause
// so errors.Is/As still traverse it. Returns err unchanged when it holds no url.
func RedactSecretsErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	redacted := RedactSecrets(msg)
	if redacted == msg {
		return err
	}
	return &wrappedRouterError{msg: redacted, cause: err}
}

// safeHeaderValues names the headers whose values may be logged. Everything else
// is withheld, because auth header names are operator-configured per node-url
// (auth-config.auth-headers) and inbound requests carry the caller's own
// credentials — no deny-list can enumerate either. Header NAMES are always
// logged, so a redacted line still shows what was sent.
var safeHeaderValues = map[string]struct{}{
	"accept": {}, "accept-encoding": {}, "accept-language": {}, "allow": {},
	"cache-control": {}, "connection": {}, "content-encoding": {},
	"content-language": {}, "content-length": {}, "content-type": {},
	"date": {}, "expect": {}, "host": {}, "keep-alive": {}, "server": {},
	"te": {}, "trailer": {}, "transfer-encoding": {}, "upgrade": {},
	"user-agent": {}, "vary": {}, "via": {},
}

// IsSensitiveHeader reports whether a header's value must be withheld from logs.
// Fails closed: anything not explicitly known-safe is sensitive.
func IsSensitiveHeader(name string) bool {
	_, safe := safeHeaderValues[strings.ToLower(strings.TrimSpace(name))]
	return !safe
}

// RedactHeaderValue returns value when the header is known-safe to log, and the
// redaction mark otherwise.
func RedactHeaderValue(name, value string) string {
	if IsSensitiveHeader(name) {
		return RedactedMark
	}
	return value
}

// RedactHeaders renders a header map for logging with sensitive values withheld.
// Takes the unnamed map type so http.Header and grpc metadata.MD both assign to
// it. Output is sorted by name — map iteration order would otherwise make log
// lines differ run to run.
func RedactHeaders(h map[string][]string) string {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(name)
		b.WriteByte(':')
		if IsSensitiveHeader(name) {
			b.WriteString(RedactedMark)
			continue
		}
		b.WriteString(strings.Join(h[name], ","))
	}
	b.WriteByte('}')
	return b.String()
}

// RedactHeaderMap is RedactHeaders for a single-valued header map.
func RedactHeaderMap(h map[string]string) string {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(RedactHeaderValue(name, h[name]))
	}
	b.WriteByte('}')
	return b.String()
}

// RedactSecretsBytes is RedactSecrets for a body. It returns b itself when there is nothing to
// withhold, so a body without secrets is not copied.
func RedactSecretsBytes(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	text := string(b)
	if redacted := RedactSecrets(text); redacted != text {
		return []byte(redacted)
	}
	return b
}
