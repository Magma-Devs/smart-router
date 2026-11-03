package specfetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	types "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// fetchFromGitLab fetches all specs from a GitLab repository.
//
// It first downloads the repository archive from the /-/archive/ endpoint —
// a single request served outside GitLab's metered REST API — and falls back
// to the tree-API listing flow if the archive fetch fails (e.g. private
// repositories where the token is only honoured by the API).
func (f *Fetcher) fetchFromGitLab(ctx context.Context, info *RepoInfo) (map[string]types.Spec, error) {
	specs, tarballErr := f.fetchFromGitLabTarball(ctx, info)
	if tarballErr == nil {
		return specs, nil
	}

	utils.LavaFormatWarning("GitLab archive fetch failed, falling back to tree API", tarballErr,
		utils.LogAttr("repo", info.ProjectPath))

	specs, apiErr := f.fetchFromGitLabAPI(ctx, info)
	if apiErr != nil {
		return nil, fmt.Errorf("GitLab fetch failed (tarball: %v): %w", tarballErr, apiErr)
	}
	return specs, nil
}

// fetchFromGitLabTarball downloads the repository archive and parses every
// .json spec file directly under the configured path.
func (f *Fetcher) fetchFromGitLabTarball(ctx context.Context, info *RepoInfo) (map[string]types.Spec, error) {
	return f.fetchTarball(ctx, f.buildGitLabTarballURL(info), info.Path, f.setGitLabHeaders)
}

// buildGitLabTarballURL constructs the /-/archive/ tarball URL for the repository ref.
// Format: {host}/{project_path}/-/archive/{ref}/{repo}-{ref}.tar.gz
func (f *Fetcher) buildGitLabTarballURL(info *RepoInfo) string {
	ref := info.Branch
	if ref == "" {
		ref = "HEAD"
	}
	basename := info.ProjectPath[strings.LastIndex(info.ProjectPath, "/")+1:]
	// GitLab names the archive after the ref with slashes flattened to dashes.
	fileRef := strings.ReplaceAll(ref, "/", "-")
	return fmt.Sprintf("%s/%s/-/archive/%s/%s-%s.tar.gz",
		info.Host, info.ProjectPath, ref, basename, fileRef)
}

// fetchFromGitLabAPI fetches all specs via the GitLab tree API — one
// rate-limited listing call plus a files-API download per file.
func (f *Fetcher) fetchFromGitLabAPI(ctx context.Context, info *RepoInfo) (map[string]types.Spec, error) {
	return f.fetchFromListingAPI(ctx, info, listingAPI{
		provider:        "GitLab",
		listingURL:      f.buildGitLabTreeAPIURL,
		parseListing:    f.parseGitLabDirectoryListing,
		setHeaders:      f.setGitLabHeaders,
		unauthenticated: utils.LogAttr("note", "private repos require authentication"),
	})
}

// buildGitLabTreeAPIURL constructs the GitLab API URL for listing directory contents.
// Format: /api/v4/projects/{project_path}/repository/tree?ref={branch}&path={path}
func (f *Fetcher) buildGitLabTreeAPIURL(info *RepoInfo) string {
	encodedProject := url.PathEscape(info.ProjectPath)
	apiURL := fmt.Sprintf("%s/api/v4/projects/%s/repository/tree?ref=%s",
		info.Host, encodedProject, url.QueryEscape(gitLabRef(info)))

	if info.Path != "" {
		apiURL += "&path=" + url.QueryEscape(info.Path)
	}
	return apiURL
}

// buildGitLabFileAPIURL constructs the GitLab API URL for fetching raw file content.
// Format: /api/v4/projects/{project_path}/repository/files/{file_path}/raw?ref={branch}
func (f *Fetcher) buildGitLabFileAPIURL(info *RepoInfo, filePath string) string {
	encodedProject := url.PathEscape(info.ProjectPath)
	encodedFilePath := url.PathEscape(filePath)

	return fmt.Sprintf("%s/api/v4/projects/%s/repository/files/%s/raw?ref=%s",
		info.Host, encodedProject, encodedFilePath, url.QueryEscape(gitLabRef(info)))
}

// gitLabRef resolves the ref to use in GitLab URLs; an empty branch means the
// repository's default branch, which GitLab addresses as HEAD.
func gitLabRef(info *RepoInfo) string {
	if info.Branch == "" {
		return "HEAD"
	}
	return info.Branch
}

// setGitLabHeaders sets the appropriate headers for GitLab API requests.
func (f *Fetcher) setGitLabHeaders(req *http.Request) {
	if f.config.Token != "" {
		req.Header.Set("PRIVATE-TOKEN", f.config.Token)
	}
}

// gitLabFileEntry represents a file entry in GitLab API response.
type gitLabFileEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // "blob" for files, "tree" for directories
	Path string `json:"path"` // full path within repository
}

// parseGitLabDirectoryListing parses the GitLab API response and returns URLs for JSON files.
func (f *Fetcher) parseGitLabDirectoryListing(body io.Reader, info *RepoInfo) ([]string, error) {
	var files []gitLabFileEntry
	if err := json.NewDecoder(body).Decode(&files); err != nil {
		return nil, fmt.Errorf("failed to parse GitLab API response: %w", err)
	}

	var urls []string
	for _, file := range files {
		if file.Type == "blob" && strings.HasSuffix(file.Name, ".json") {
			urls = append(urls, f.buildGitLabFileAPIURL(info, file.Path))
		}
	}
	return urls, nil
}
