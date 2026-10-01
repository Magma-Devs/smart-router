package common

import (
	"fmt"
	"net/url"
	"strings"
)

// JoinURLPath joins base URL and path robustly (handles slashes and query params correctly).
// When path is absolute (starts with /), it is appended to the base URL's path so that
// base paths like /gateway/lava/rest/KEY are preserved (ResolveReference would replace them).
func JoinURLPath(base, path string) (string, error) {
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
	// Read it as a plain path on the configured node instead, byte for byte: the node answers it
	// (usually 404), as it would any other path the client sends.
	if pathURL.Scheme != "" || pathURL.Host != "" || pathURL.User != nil {
		rawPath, rawQuery, _ := strings.Cut(path, "?")
		unescaped, err := url.PathUnescape(rawPath)
		if err != nil {
			return "", fmt.Errorf("invalid path: %w", err)
		}
		pathURL = &url.URL{Path: unescaped, RawQuery: rawQuery}
	}

	// If path is absolute, append it to base path instead of replacing (preserves e.g. /gateway/lava/rest/KEY).
	if strings.HasPrefix(pathURL.Path, "/") {
		basePath := strings.TrimSuffix(baseURL.Path, "/")
		relPath := strings.TrimPrefix(pathURL.Path, "/")
		if relPath != "" {
			baseURL.Path = basePath + "/" + relPath
		} else {
			baseURL.Path = basePath
		}
		baseURL.RawPath = "" // let EscapedPath() derive from Path
		if pathURL.RawQuery != "" {
			baseURL.RawQuery = pathURL.RawQuery
		}
		return baseURL.String(), nil
	}

	// Relative path: use ResolveReference (handles ., .., query params)
	joined := baseURL.ResolveReference(pathURL)
	if joined.Scheme != baseURL.Scheme || joined.Host != baseURL.Host || joined.User.String() != baseURL.User.String() {
		return "", fmt.Errorf("path %q would leave the configured host", path)
	}
	return joined.String(), nil
}
