package specfetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	types "github.com/magma-Devs/smart-router/types/spec"
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
	// Build the API URL for listing directory contents
	apiURL := f.buildGitLabTreeAPIURL(info)

	utils.LavaFormatInfo("Fetching spec file list from GitLab",
		utils.LogAttr("api_url", apiURL))

	if f.config.Token != "" {
		utils.LavaFormatInfo("Using GitLab token authentication",
			utils.LogAttr("token_prefix", f.config.Token[:min(4, len(f.config.Token))]))
	} else {
		utils.LavaFormatInfo("Using unauthenticated GitLab access",
			utils.LogAttr("note", "private repos require authentication"))
	}

	// Fetch the directory listing
	apiCtx, cancel := context.WithTimeout(ctx, f.config.APITimeout)
	defer cancel()

	resp, err := f.doRequest(apiCtx, http.MethodGet, apiURL, f.setGitLabHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from GitLab API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GitLab API error (status: %d, body: %s)", resp.StatusCode, string(body))
	}

	// Parse directory listing
	fileURLs, err := f.parseGitLabDirectoryListing(resp.Body, info)
	if err != nil {
		return nil, err
	}

	if len(fileURLs) == 0 {
		return nil, fmt.Errorf("no .json spec files found in repository")
	}

	utils.LavaFormatInfo("Found spec files to fetch",
		utils.LogAttr("file_count", len(fileURLs)))

	// Fetch all spec files in parallel
	return f.fetchFilesParallel(ctx, fileURLs, f.setGitLabHeaders)
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
