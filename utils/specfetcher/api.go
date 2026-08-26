package specfetcher

import (
	"context"

	types "github.com/magma-Devs/smart-router/types/spec"
)

// FetchAllSpecsFromRemote fetches all specs from a remote repository without expansion.
// This is useful for aggregating specs from multiple sources before expanding.
// The returned map contains unexpanded specs keyed by their chain ID (Index).
func FetchAllSpecsFromRemote(ctx context.Context, repoURL, token string) (map[string]types.Spec, error) {
	config := DefaultConfig()
	config.Token = token
	fetcher := New(config)
	return fetcher.FetchAllSpecs(ctx, repoURL)
}

// IsGitHubURL returns true if the URL is a GitHub repository URL.
func IsGitHubURL(rawURL string) bool {
	info, err := ParseRepoURL(rawURL)
	if err != nil {
		return false
	}
	return info.Provider == ProviderGitHub
}

// IsGitLabURL returns true if the URL is a GitLab repository URL.
func IsGitLabURL(rawURL string) bool {
	info, err := ParseRepoURL(rawURL)
	if err != nil {
		return false
	}
	return info.Provider == ProviderGitLab
}

// IsRemoteRepoURL returns true if the URL is a supported remote repository URL.
func IsRemoteRepoURL(rawURL string) bool {
	_, err := ParseRepoURL(rawURL)
	return err == nil
}
