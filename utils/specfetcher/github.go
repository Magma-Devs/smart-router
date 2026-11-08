package specfetcher

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
)

// gitHub reads specs from GitHub: the tarball comes from codeload.github.com,
// which is not metered by the REST API rate limit (60 req/hour unauthenticated
// per IP); the fallback lists the directory with the contents API and
// downloads each file from raw.githubusercontent.com.
func (f *Fetcher) gitHub() repoHost {
	return repoHost{
		name:       "GitHub",
		archive:    "tarball",
		listingAPI: "contents API",
		anonymous:  utils.LogAttr("rate_limit", "60 req/hour"),
		fileType:   "file",
		tarballURL: f.buildGitHubTarballURL,
		listingURL: f.buildGitHubAPIURL,
		fileURL: func(info *RepoInfo, entry repoListingEntry) string {
			return f.buildGitHubRawURL(info, entry.Name)
		},
		setHeaders: f.setGitHubHeaders,
	}
}

// buildGitHubTarballURL constructs the codeload.github.com tarball URL for the repository ref.
func (f *Fetcher) buildGitHubTarballURL(info *RepoInfo) string {
	ref := info.Branch
	if ref == "" {
		ref = "HEAD"
	}
	return fmt.Sprintf("https://codeload.github.com/%s/tar.gz/%s", info.ProjectPath, ref)
}

// buildGitHubAPIURL constructs the GitHub API URL for listing directory contents.
func (f *Fetcher) buildGitHubAPIURL(info *RepoInfo) string {
	// Split project path into owner/repo
	parts := strings.SplitN(info.ProjectPath, "/", 2)
	owner, repo := parts[0], parts[1]

	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s",
		owner, repo, info.Path)
	// An empty branch means the default branch, which the API uses when ref is omitted.
	if info.Branch != "" {
		apiURL += "?ref=" + info.Branch
	}
	return apiURL
}

// buildGitHubRawURL constructs a raw.githubusercontent.com URL for file content.
func (f *Fetcher) buildGitHubRawURL(info *RepoInfo, filename string) string {
	parts := strings.SplitN(info.ProjectPath, "/", 2)
	owner, repo := parts[0], parts[1]

	ref := info.Branch
	if ref == "" {
		ref = "HEAD" // raw.githubusercontent.com resolves HEAD to the default branch
	}

	if info.Path != "" {
		return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s/%s",
			owner, repo, ref, info.Path, filename)
	}
	return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s",
		owner, repo, ref, filename)
}

// setGitHubHeaders sets the appropriate headers for GitHub API requests.
func (f *Fetcher) setGitHubHeaders(req *http.Request) {
	if f.config.Token != "" {
		req.Header.Set("Authorization", "token "+f.config.Token)
	}
}
