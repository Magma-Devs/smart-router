package specfetcher

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// listingCase is one provider's contents-listing flow: the flow a fetch falls back to when
// the provider's tarball or archive cannot be downloaded. GitHub and GitLab run the same
// steps through it — list the directory, refuse a failed or empty listing, download every
// .json file with the provider's own credentials — and differ only in the URLs, the listing
// format and the auth header. These cases pin those steps for both, so the two flows can be
// shown to behave identically before and after they share one implementation (MAG-4244).
type listingCase struct {
	provider    string // as it appears in log lines and errors
	repoURL     string // what the caller asks FetchAllSpecs for
	tarballPath string // failed on purpose, to force the listing flow
	listingPath string
	filePath    string // the download path of the one spec the listing names
	listing     string // a listing that names one .json spec
	noSpecs     string // a listing that names no .json spec
	authHeader  string // the header the provider authenticates with
	authValue   string // its value for the token "tok-1234"
	otherHeader string // the OTHER provider's header, which must never be sent
}

var listingCases = []listingCase{
	{
		provider:    "GitHub",
		repoURL:     "https://github.com/user/repo/tree/main/",
		tarballPath: "/user/repo/tar.gz/main",
		listingPath: "/repos/user/repo/contents/",
		filePath:    "/user/repo/main/ethereum.json",
		listing:     `[{"name": "ethereum.json", "type": "file"}, {"name": "docs", "type": "dir"}]`,
		noSpecs:     `[{"name": "README.md", "type": "file"}, {"name": "docs", "type": "dir"}]`,
		authHeader:  "Authorization",
		authValue:   "token tok-1234",
		otherHeader: "PRIVATE-TOKEN",
	},
	{
		provider:    "GitLab",
		repoURL:     "https://gitlab.com/myorg/myrepo/-/tree/main",
		tarballPath: "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz",
		listingPath: "/api/v4/projects/myorg/myrepo/repository/tree",
		filePath:    "/api/v4/projects/myorg/myrepo/repository/files/ethereum.json/raw",
		listing:     `[{"name": "ethereum.json", "type": "blob", "path": "ethereum.json"}, {"name": "docs", "type": "tree", "path": "docs"}]`,
		noSpecs:     `[{"name": "README.md", "type": "blob", "path": "README.md"}, {"name": "docs", "type": "tree", "path": "docs"}]`,
		authHeader:  "PRIVATE-TOKEN",
		authValue:   "tok-1234",
		otherHeader: "Authorization",
	},
}

// listingFetcher serves tc's tarball as a failure, so every fetch falls back to the listing
// flow, and hands the listing request to onListing.
func listingFetcher(t *testing.T, tc listingCase, onListing http.HandlerFunc) *Fetcher {
	t.Helper()
	return newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tc.tarballPath:
			http.Error(w, "not found", http.StatusNotFound)
		case tc.listingPath:
			onListing(w, r)
		case tc.filePath:
			_, _ = w.Write(specProposalJSON("ETH1"))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestListingAPI_AFailedListingNamesTheProviderAndStatus(t *testing.T) {
	for _, tc := range listingCases {
		t.Run(tc.provider, func(t *testing.T) {
			fetcher := listingFetcher(t, tc, func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "upstream down", http.StatusServiceUnavailable)
			})
			_, err := fetcher.FetchAllSpecs(context.Background(), tc.repoURL)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.provider+" API error (status: 503")
			require.Contains(t, err.Error(), "upstream down", "the listing's own body reaches the error")
		})
	}
}

func TestListingAPI_AnUnreachableListingNamesTheProvider(t *testing.T) {
	for _, tc := range listingCases {
		t.Run(tc.provider, func(t *testing.T) {
			fetcher := listingFetcher(t, tc, func(w http.ResponseWriter, r *http.Request) {
				panic(http.ErrAbortHandler) // drops the connection: the request itself fails
			})
			_, err := fetcher.FetchAllSpecs(context.Background(), tc.repoURL)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to fetch from "+tc.provider+" API")
		})
	}
}

func TestListingAPI_AListingWithNoSpecIsRefused(t *testing.T) {
	for _, tc := range listingCases {
		t.Run(tc.provider, func(t *testing.T) {
			fetcher := listingFetcher(t, tc, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.noSpecs))
			})
			_, err := fetcher.FetchAllSpecs(context.Background(), tc.repoURL)
			require.Error(t, err)
			require.Contains(t, err.Error(), "no .json spec files found in repository")
		})
	}
}

// The provider's token has to reach BOTH the listing and every file download. The two use
// separate requests, and a private repository refuses either one without the token — so a
// flow that authenticated the listing and forgot the downloads would list the specs and
// then fail to fetch every one of them.
func TestListingAPI_TheProvidersTokenReachesTheListingAndTheDownloads(t *testing.T) {
	for _, tc := range listingCases {
		t.Run(tc.provider, func(t *testing.T) {
			var mu sync.Mutex
			seen := map[string]http.Header{}
			record := func(r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				seen[r.URL.Path] = r.Header.Clone()
			}

			fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case tc.tarballPath:
					http.Error(w, "not found", http.StatusNotFound)
				case tc.listingPath:
					record(r)
					_, _ = w.Write([]byte(tc.listing))
				case tc.filePath:
					record(r)
					_, _ = w.Write(specProposalJSON("ETH1"))
				default:
					http.NotFound(w, r)
				}
			}))
			fetcher.config.Token = "tok-1234"

			specs, err := fetcher.FetchAllSpecs(context.Background(), tc.repoURL)
			require.NoError(t, err)
			require.Contains(t, specs, "ETH1")

			mu.Lock()
			defer mu.Unlock()
			for _, path := range []string{tc.listingPath, tc.filePath} {
				headers, ok := seen[path]
				require.True(t, ok, "%s was never requested", path)
				require.Equal(t, tc.authValue, headers.Get(tc.authHeader), "%s carries %s's credentials", path, tc.provider)
				require.Empty(t, headers.Get(tc.otherHeader), "%s must not carry the other provider's header", path)
			}
		})
	}
}
