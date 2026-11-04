package common

import (
	"net/url"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
)

// RegisterSecrets hands every secret this node-url carries to utils' value redaction, so that
// none of them reaches a log line or a client reply in any shape: inside its url, echoed back by
// the node on its own, or behind a scheme-less address that no url pattern recognises (MAG-4863).
// Call it once per configured node-url, at start-up.
func (nurl *NodeUrl) RegisterSecrets() {
	credentials, candidates := nurl.SecretValues()
	utils.RegisterSecrets(credentials...)
	utils.RegisterSecretCandidates(candidates...)
}

// SecretValues lists what RegisterSecrets registers. credentials are values the configuration
// names as secrets: a url password (or the user, when it stands alone, as in "<key>@host"), the
// auth-query values and the auth-header values. candidates are the parts of the url that carry a
// key on some vendors and a plain word on others (path segments, query values, the fragment, a
// user beside a password); utils keeps only those that look like a key.
func (nurl *NodeUrl) SecretValues() (credentials, candidates []string) {
	if nurl == nil {
		return nil, nil
	}
	for _, value := range nurl.AuthConfig.AuthHeaders {
		credentials = append(credentials, value)
		// "Bearer <token>": the token on its own is what a node would echo.
		credentials = append(credentials, strings.Fields(value)...)
	}
	credentials = append(credentials, queryValues(nurl.AuthConfig.AuthQuery)...)

	raw := nurl.Url
	if raw == "" {
		return credentials, candidates
	}
	if !strings.Contains(raw, "://") {
		// The config accepts scheme-less addresses ("host/v2/<key>"); "//" makes url.Parse read
		// the host as a host instead of as a path.
		raw = "//" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		// An address url.Parse rejects is still dialed as configured, so its parts still count:
		// every piece between the url delimiters is a candidate.
		candidates = append(candidates, strings.FieldsFunc(nurl.Url, func(r rune) bool {
			return strings.ContainsRune(":/?#&;=@", r)
		})...)
		return credentials, candidates
	}
	if parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			credentials = append(credentials, password)
			candidates = append(candidates, parsed.User.Username())
		} else {
			credentials = append(credentials, parsed.User.Username())
		}
	}
	candidates = append(candidates, strings.Split(parsed.Path, "/")...)
	candidates = append(candidates, queryValues(parsed.RawQuery)...)
	candidates = append(candidates, parsed.Fragment)
	return credentials, candidates
}

// queryValues returns the values of a raw query, unescaped where they can be. It splits on ";" as
// well as "&": Go's HTTP client sends a ";" query unchanged, and url.ParseQuery rejects one.
func queryValues(rawQuery string) []string {
	pairs := strings.FieldsFunc(rawQuery, func(r rune) bool { return r == '&' || r == ';' })
	values := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		value := pair
		if i := strings.IndexByte(pair, '='); i >= 0 {
			value = pair[i+1:]
		}
		if unescaped, err := url.QueryUnescape(value); err == nil {
			value = unescaped
		}
		values = append(values, value)
	}
	return values
}
