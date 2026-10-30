package specfetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	types "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// repoHost is what differs between the git hosts the fetcher reads: where the
// archive and the directory listing live, how a token is sent, which listing
// entries are files and where each one is downloaded from. The fetch itself —
// archive first, listing API as the fallback — is the same for every host.
type repoHost struct {
	name       string // "GitHub", "GitLab": names the host in log lines and errors
	archive    string // what the host calls its archive, in the fallback warning
	listingAPI string // the listing API's name, in the fallback warning
	anonymous  utils.Attribute
	fileType   string // the listing's type for a file entry
	tarballURL func(info *RepoInfo) string
	listingURL func(info *RepoInfo) string
	fileURL    func(info *RepoInfo, entry repoListingEntry) string
	setHeaders func(req *http.Request)
}

// repoListingEntry is one entry of a directory listing: GitHub's contents API
// and GitLab's tree API both name it, type it and (GitLab) give its full path.
type repoListingEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // GitHub: "file" or "dir"; GitLab: "blob" or "tree"
	Path string `json:"path"` // full path within the repository
}

// fetchFromRepo fetches all specs from one repository. It first downloads the
// repository archive — a single request outside the host's metered REST API —
// and falls back to the listing flow if the archive fetch fails.
func (f *Fetcher) fetchFromRepo(ctx context.Context, host repoHost, info *RepoInfo) (map[string]types.Spec, error) {
	specs, tarballErr := f.fetchTarball(ctx, host.tarballURL(info), info.Path, host.setHeaders)
	if tarballErr == nil {
		return specs, nil
	}

	utils.LavaFormatWarning(host.name+" "+host.archive+" fetch failed, falling back to "+host.listingAPI, tarballErr,
		utils.LogAttr("repo", info.ProjectPath))

	specs, apiErr := f.fetchFromRepoAPI(ctx, host, info)
	if apiErr != nil {
		return nil, fmt.Errorf("%s fetch failed (tarball: %v): %w", host.name, tarballErr, apiErr)
	}
	return specs, nil
}

// fetchFromRepoAPI fetches all specs via the host's listing API — one
// rate-limited listing call plus one download per file.
func (f *Fetcher) fetchFromRepoAPI(ctx context.Context, host repoHost, info *RepoInfo) (map[string]types.Spec, error) {
	apiURL := host.listingURL(info)

	utils.LavaFormatInfo("Fetching spec file list from "+host.name,
		utils.LogAttr("api_url", apiURL))

	if f.config.Token != "" {
		utils.LavaFormatInfo("Using "+host.name+" token authentication",
			utils.LogAttr("token_prefix", f.config.Token[:min(4, len(f.config.Token))]))
	} else {
		utils.LavaFormatInfo("Using unauthenticated "+host.name+" access", host.anonymous)
	}

	// Fetch the directory listing
	apiCtx, cancel := context.WithTimeout(ctx, f.config.APITimeout)
	defer cancel()

	resp, err := f.doRequest(apiCtx, apiURL, host.setHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from %s API: %w", host.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s API error (status: %d, body: %s)", host.name, resp.StatusCode, string(body))
	}

	fileURLs, err := parseRepoListing(resp.Body, host, info)
	if err != nil {
		return nil, err
	}

	if len(fileURLs) == 0 {
		return nil, fmt.Errorf("no .json spec files found in repository")
	}

	utils.LavaFormatInfo("Found spec files to fetch",
		utils.LogAttr("file_count", len(fileURLs)))

	// Fetch all spec files in parallel
	return f.fetchFilesParallel(ctx, fileURLs, host.setHeaders)
}

// parseRepoListing parses a directory listing and returns the download URLs of its .json files.
func parseRepoListing(body io.Reader, host repoHost, info *RepoInfo) ([]string, error) {
	var files []repoListingEntry
	if err := json.NewDecoder(body).Decode(&files); err != nil {
		return nil, fmt.Errorf("failed to parse %s API response: %w", host.name, err)
	}

	var urls []string
	for _, file := range files {
		if file.Type == host.fileType && strings.HasSuffix(file.Name, ".json") {
			urls = append(urls, host.fileURL(info, file))
		}
	}
	return urls, nil
}
