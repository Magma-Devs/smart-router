package specfetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"

	types "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// listingAPI is what differs between providers' contents-listing flows. A fetch falls back
// to that flow when the provider's tarball or archive cannot be downloaded, and GitHub and
// GitLab run the same steps through it: list the spec directory, refuse a failed or empty
// listing, then download every .json file with the provider's own credentials. Only the
// pieces below differ, so the steps live once, in fetchFromListingAPI (MAG-4244 §7).
type listingAPI struct {
	// provider names the provider in log lines and errors: "GitHub", "GitLab".
	provider string
	// listingURL is the endpoint that lists the spec directory.
	listingURL func(*RepoInfo) string
	// parseListing reads a listing body and returns a download URL per .json spec in it.
	parseListing func(body io.Reader, info *RepoInfo) ([]string, error)
	// setHeaders adds the provider's credentials. It is applied to the listing AND to every
	// download: a private repository refuses either one without them.
	setHeaders func(*http.Request)
	// unauthenticated is what to tell an operator who configured no token — the limit they
	// will hit, which differs by provider.
	unauthenticated utils.Attribute
}

// fetchFromListingAPI lists api's spec directory and downloads every .json spec it names.
func (f *Fetcher) fetchFromListingAPI(ctx context.Context, info *RepoInfo, api listingAPI) (map[string]types.Spec, error) {
	listingURL := api.listingURL(info)

	utils.LavaFormatInfo("Fetching spec file list from "+api.provider,
		utils.LogAttr("api_url", listingURL))

	if f.config.Token != "" {
		utils.LavaFormatInfo("Using "+api.provider+" token authentication",
			utils.LogAttr("token_prefix", f.config.Token[:min(4, len(f.config.Token))]))
	} else {
		utils.LavaFormatInfo("Using unauthenticated "+api.provider+" access", api.unauthenticated)
	}

	listingCtx, cancel := context.WithTimeout(ctx, f.config.APITimeout)
	defer cancel()

	resp, err := f.doRequest(listingCtx, listingURL, api.setHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from %s API: %w", api.provider, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s API error (status: %d, body: %s)", api.provider, resp.StatusCode, string(body))
	}

	fileURLs, err := api.parseListing(resp.Body, info)
	if err != nil {
		return nil, err
	}
	if len(fileURLs) == 0 {
		return nil, fmt.Errorf("no .json spec files found in repository")
	}

	utils.LavaFormatInfo("Found spec files to fetch",
		utils.LogAttr("file_count", len(fileURLs)))

	return f.fetchFilesParallel(ctx, fileURLs, api.setHeaders)
}
