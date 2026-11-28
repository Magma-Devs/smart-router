package common

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// UnixSocketPrefix marks a listen or dial address as a unix socket path ("unix:/tmp/x.sock").
const UnixSocketPrefix = "unix:"

// JoinURLPath joins a request's path, with its query, to a base URL. The path goes under the base
// URL's own path, so a base path like /gateway/cosmoshub/rest/KEY is kept, whatever the path holds:
//   - a leading "/" or none: either is a path under the base path;
//   - a scheme, host or userinfo ("//other.example", "http://other.example"): path text on the base
//     URL's host, which the join never changes (MAG-3970);
//   - "." and ".." segments, plain or percent-encoded: resolved against the path's own root before
//     the join, so "/../x" is "/x" under the base path and the node gets no dot segment (MAG-5699).
//
// The query goes after the base URL's own query, which stays as configured: a pair whose name the
// base URL's query already sets is left out (MAG-5699). Like every path, the path is decoded here
// and re-encoded by String(), so "%2F" leaves as "/".
func JoinURLPath(base, path string) (string, error) {
	return joinURLPath(base, path, "")
}

// RequestURL is the URL a request for path, with its query, goes to on this node url: the path under
// the node url's own path and the query after the node url's own, as JoinURLPath joins them, and
// without the pairs whose name the auth-query sets. AddAuthPath adds the auth-query itself when the
// request is sent, so a node url's parameters go as configured whatever the request's query repeats
// (MAG-5699).
func (nu *NodeUrl) RequestURL(path string) (string, error) {
	if nu == nil {
		return "", errors.New("no node url")
	}
	return joinURLPath(nu.Url, path, nu.AuthConfig.AuthQuery)
}

// joinURLPath is JoinURLPath with reservedQuery, a raw query whose names the path's query may not
// set either.
func joinURLPath(base, path, reservedQuery string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}

	pathURL, err := url.Parse(path)
	if err != nil {
		return "", fmt.Errorf("invalid path: %w", err)
	}
	// The path is the client's; the scheme and host are the operator's. A path that parses with a
	// host — "//other.example" is a network-path reference, "http://other.example" an absolute URL —
	// would otherwise replace the configured node's host and carry its auth headers there (MAG-3970).
	// Read it as a path on the configured node instead; the node decides what it means.
	if pathURL.Scheme != "" || pathURL.Host != "" || pathURL.User != nil {
		rawPath, rawQuery, _ := strings.Cut(path, "?")
		unescaped, err := url.PathUnescape(rawPath)
		if err != nil {
			return "", fmt.Errorf("invalid path: %w", err)
		}
		pathURL = &url.URL{Path: unescaped, RawQuery: rawQuery}
	}

	// Every path is a path under the base path. Dot segments are resolved against the path's own
	// root, not the base path, so the result is a path under the base path whatever they say, and
	// the node gets none to resolve itself.
	relPath := resolveDotSegments("/" + strings.TrimPrefix(pathURL.Path, "/"))
	basePath := strings.TrimSuffix(baseURL.Path, "/")
	if relPath == "/" {
		baseURL.Path = basePath
	} else {
		baseURL.Path = basePath + relPath
	}
	baseURL.RawPath = "" // let EscapedPath() derive from Path

	// The base URL's query goes as configured and the path's after it, without the names the base
	// URL's query or the reserved query set.
	if query := withoutQueryNames(pathURL.RawQuery, queryNames(baseURL.RawQuery, reservedQuery)); query != "" {
		if baseURL.RawQuery == "" {
			baseURL.RawQuery = query
		} else {
			baseURL.RawQuery += URL_QUERY_PARAMETERS_SEPARATOR_OTHER_PARAMETERS + query
		}
	}
	return baseURL.String(), nil
}

// ResolveDotSegments is path, a decoded request path as url.URL.Path holds it, with its "." and ".."
// segments resolved as JoinURLPath resolves them before the join, so a parser reads the request
// off the path the node gets. A ".." that climbs to the root can leave the path starting with "//",
// which url.Parse reads as a host, so the leading slashes are collapsed to one, as the REST listener
// collapses a client's. A path with no dot segment is returned as it is.
func ResolveDotSegments(path string) string {
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return "/" + strings.TrimLeft(resolveDotSegments("/"+strings.TrimPrefix(path, "/")), "/")
		}
	}
	return path
}

// resolveDotSegments is p, a path with a leading "/", with its "." and ".." segments resolved against
// p's own root the way a server resolves them (RFC 3986 section 5.2.4): "/a/./b" is "/a/b", "/a/../b"
// is "/b", and so is "/../b", since nothing climbs above the root. Every other segment stays, an
// empty one and a trailing "/" included: a spec can name an api with a trailing slash.
func resolveDotSegments(p string) string {
	return (&url.URL{Path: "/"}).ResolveReference(&url.URL{Path: p}).Path
}

// queryNames lists the parameter names the raw queries set, as queryName reads them. A pair ends at
// "&" or ";": some servers split on both.
func queryNames(rawQueries ...string) map[string]bool {
	names := map[string]bool{}
	for _, rawQuery := range rawQueries {
		for _, pair := range strings.FieldsFunc(rawQuery, isQuerySeparator) {
			names[queryName(pair)] = true
		}
	}
	return names
}

// queryName is a query pair's name, unescaped and lowercased, so a name a server reads the same way
// compares equal however it is spelled.
func queryName(pair string) string {
	name, _, _ := strings.Cut(pair, "=")
	if unescaped, err := url.QueryUnescape(name); err == nil {
		name = unescaped
	}
	return strings.ToLower(name)
}

func isQuerySeparator(r rune) bool { return r == '&' || r == ';' }

// withoutQueryNames is rawQuery without the pairs whose name is in names. The pairs it keeps, and the
// separator before each, stay as they were.
func withoutQueryNames(rawQuery string, names map[string]bool) string {
	if len(names) == 0 {
		return rawQuery
	}
	var kept strings.Builder
	wrote := false
	before := ""
	for {
		pair, after, rest, found := rawQuery, "", "", false
		if i := strings.IndexFunc(rawQuery, isQuerySeparator); i >= 0 {
			pair, after, rest, found = rawQuery[:i], rawQuery[i:i+1], rawQuery[i+1:], true
		}
		if pair == "" || !names[queryName(pair)] {
			if wrote {
				kept.WriteString(before)
			}
			kept.WriteString(pair)
			wrote = true
		}
		if !found {
			return kept.String()
		}
		rawQuery, before = rest, after
	}
}
