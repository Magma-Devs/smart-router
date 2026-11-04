package specfetcher

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
)

// gitLab reads specs from GitLab: the archive comes from the /-/archive/
// endpoint, served outside GitLab's metered REST API; the fallback lists the
// directory with the tree API and downloads each file from the files API (e.g.
// private repositories where the token is only honoured by the API).
func (f *Fetcher) gitLab() repoHost {
	return repoHost{
		name:       "GitLab",
		archive:    "archive",
		listingAPI: "tree API",
		anonymous:  utils.LogAttr("note", "private repos require authentication"),
		fileType:   "blob",
		tarballURL: f.buildGitLabTarballURL,
		listingURL: f.buildGitLabTreeAPIURL,
		fileURL: func(info *RepoInfo, entry repoListingEntry) string {
			return f.buildGitLabFileAPIURL(info, entry.Path)
		},
		setHeaders: f.setGitLabHeaders,
	}
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
