package utils

import (
	"regexp"
	"strings"
)

// RedactedURLMark replaces the credential-bearing tail of a url. It is a path
// segment so a redacted url still reads as a url in logs and error text.
const RedactedURLMark = "/[redacted]"

// urlInText matches a scheme-qualified url inside arbitrary text. Anchoring on
// "://" keeps prose and scheme-less identifiers — a host:port listen address, a
// provider name, a "dial tcp 10.0.0.1:443" — untouched.
//
// The match stops at ",;)" as well as at quotes and brackets so a url embedded in
// a log attribute list ("{url:https://host/p,method:eth_call}") does not swallow
// what follows it. No credential charset uses those, and the host — all this
// needs to identify — always precedes them.
var urlInText = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.\-]*://[^\s"'` + "`" + `<>\\^{}|\[\],;)]+`)

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
// provider address ("lava@provider") is not a url, and RedactURL would read its
// "@" as userinfo and drop the prefix.
func RedactIfURL(s string) string {
	if strings.Contains(s, "://") || strings.ContainsAny(s, "/?#") {
		return RedactURL(s)
	}
	return s
}

// RedactSecrets rewrites every url in s with RedactURL. Use it on any text that
// may have picked up a url indirectly — an error from net/http, whose *url.Error
// message embeds the full request url (Go masks only the userinfo password), or
// a message that interpolated a node-url.
//
// The Contains check keeps this near-free for the log lines that hold no url,
// which is nearly all of them.
func RedactSecrets(s string) string {
	if !strings.Contains(s, "://") {
		return s
	}
	return urlInText.ReplaceAllStringFunc(s, RedactURL)
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
	return &wrappedLavaError{msg: redacted, cause: err}
}
