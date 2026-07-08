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
	return baseURL.ResolveReference(pathURL).String(), nil
}
