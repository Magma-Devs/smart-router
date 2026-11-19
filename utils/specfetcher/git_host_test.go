package specfetcher

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// gitHost is one host a spec repository can live on: the URLs a caller hands the fetcher, and
// the paths the fetcher reads them at. The fetcher downloads the branch's tarball and, when that
// fails, falls back to the host's listing API and raw files.
type gitHost struct {
	name string

	rootURL     string // the repository's main branch, read at its root
	subdirURL   string // the same branch, read at its specs directory
	tarballPath string // where the main branch's tarball is served
	topDir      string // the top directory of that tarball

	bareURL         string // the repository with no branch named, read at HEAD
	headTarballPath string // where the HEAD tarball is served
	headTopDir      string

	archiveURL    string // a direct download URL of the main branch's tarball
	archiveAtPath string // where that download is served
	archiveTopDir string
	listingPath   string // the directory listing the fallback reads
	listing       string // that listing: ethereum.json and a docs directory
	rawSpecPath   string // where the fallback reads ethereum.json
}

var gitHosts = []gitHost{
	{
		name:            "github",
		rootURL:         "https://github.com/user/repo/tree/main/",
		subdirURL:       "https://github.com/user/repo/tree/main/specs",
		tarballPath:     "/user/repo/tar.gz/main",
		topDir:          "repo-main",
		bareURL:         "https://github.com/user/repo/",
		headTarballPath: "/user/repo/tar.gz/HEAD",
		headTopDir:      "repo-main",
		archiveURL:      "https://codeload.github.com/Magma-Devs/lava-specs/tar.gz/refs/heads/main",
		archiveAtPath:   "/Magma-Devs/lava-specs/tar.gz/main",
		archiveTopDir:   "lava-specs-main",
		listingPath:     "/repos/user/repo/contents/",
		listing:         `[{"name": "ethereum.json", "type": "file"}, {"name": "docs", "type": "dir"}]`,
		rawSpecPath:     "/user/repo/main/ethereum.json",
	},
	{
		name:            "gitlab",
		rootURL:         "https://gitlab.com/myorg/myrepo/-/tree/main",
		subdirURL:       "https://gitlab.com/myorg/myrepo/-/tree/main/specs",
		tarballPath:     "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz",
		topDir:          "myrepo-main",
		bareURL:         "https://gitlab.com/myorg/myrepo",
		headTarballPath: "/myorg/myrepo/-/archive/HEAD/myrepo-HEAD.tar.gz",
		headTopDir:      "myrepo-HEAD",
		archiveURL:      "https://gitlab.com/myorg/myrepo/-/archive/main/myrepo-main.tar.gz",
		archiveAtPath:   "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz",
		archiveTopDir:   "myrepo-main",
		// r.URL.Path is the decoded path, so %2F in the project id shows as "/".
		listingPath: "/api/v4/projects/myorg/myrepo/repository/tree",
		listing:     `[{"name": "ethereum.json", "type": "blob", "path": "ethereum.json"}, {"name": "docs", "type": "tree", "path": "docs"}]`,
		rawSpecPath: "/api/v4/projects/myorg/myrepo/repository/files/ethereum.json/raw",
	},
}

// serveAt answers path with body and every other path with 404.
func serveAt(path string, body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	})
}

// TestFetchAllSpecs_FromGitHost reads a spec repository on each host every way a caller can name
// it, and through the fallback when the tarball fails.
func TestFetchAllSpecs_FromGitHost(t *testing.T) {
	for _, host := range gitHosts {
		t.Run(host.name, func(t *testing.T) {
			t.Run("tarball at the root reads only the root's specs", func(t *testing.T) {
				tarball := buildTarball(t, host.topDir, map[string][]byte{
					"ethereum.json":    specProposalJSON("ETH1"),
					"solana.json":      specProposalJSON("SOLANA"),
					"README.md":        []byte("# not a spec"),
					"docs/nested.json": specProposalJSON("NESTED"), // subdirectory — must be skipped for root path
				})
				var requestedPaths []string
				fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestedPaths = append(requestedPaths, r.URL.Path)
					serveAt(host.tarballPath, tarball).ServeHTTP(w, r)
				}))

				specs, err := fetcher.FetchAllSpecs(context.Background(), host.rootURL)
				require.NoError(t, err)
				require.Len(t, specs, 2)
				require.Contains(t, specs, "ETH1")
				require.Contains(t, specs, "SOLANA")
				require.NotContains(t, specs, "NESTED")
				require.Equal(t, []string{host.tarballPath}, requestedPaths)
			})

			t.Run("a bare repository URL reads HEAD", func(t *testing.T) {
				tarball := buildTarball(t, host.headTopDir, map[string][]byte{"ethereum.json": specProposalJSON("ETH1")})
				specs, err := newTestFetcher(t, serveAt(host.headTarballPath, tarball)).FetchAllSpecs(context.Background(), host.bareURL)
				require.NoError(t, err)
				require.Contains(t, specs, "ETH1")
			})

			t.Run("a direct archive URL", func(t *testing.T) {
				tarball := buildTarball(t, host.archiveTopDir, map[string][]byte{"ethereum.json": specProposalJSON("ETH1")})
				specs, err := newTestFetcher(t, serveAt(host.archiveAtPath, tarball)).FetchAllSpecs(context.Background(), host.archiveURL)
				require.NoError(t, err)
				require.Contains(t, specs, "ETH1")
			})

			t.Run("a subdirectory reads only its own specs", func(t *testing.T) {
				tarball := buildTarball(t, host.topDir, map[string][]byte{
					"specs/ethereum.json":    specProposalJSON("ETH1"),
					"specs/deep/nested.json": specProposalJSON("NESTED"),
					"other/solana.json":      specProposalJSON("SOLANA"),
					"root.json":              specProposalJSON("ROOT"),
				})
				specs, err := newTestFetcher(t, serveAt(host.tarballPath, tarball)).FetchAllSpecs(context.Background(), host.subdirURL)
				require.NoError(t, err)
				require.Len(t, specs, 1)
				require.Contains(t, specs, "ETH1")
			})

			t.Run("a failed tarball falls back to the listing API", func(t *testing.T) {
				fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case host.tarballPath:
						http.Error(w, "not found", http.StatusNotFound)
					case host.listingPath:
						require.Equal(t, "main", r.URL.Query().Get("ref"))
						_, _ = w.Write([]byte(host.listing))
					case host.rawSpecPath:
						_, _ = w.Write(specProposalJSON("ETH1"))
					default:
						http.NotFound(w, r)
					}
				}))

				specs, err := fetcher.FetchAllSpecs(context.Background(), host.rootURL)
				require.NoError(t, err)
				require.Len(t, specs, 1)
				require.Contains(t, specs, "ETH1")
			})

			t.Run("the tarball and the fallback both fail", func(t *testing.T) {
				fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "boom", http.StatusInternalServerError)
				}))
				_, err := fetcher.FetchAllSpecs(context.Background(), host.rootURL)
				require.ErrorContains(t, err, "tarball")
			})

			t.Run("no spec in the repository", func(t *testing.T) {
				tarball := buildTarball(t, host.topDir, map[string][]byte{"README.md": []byte("nothing here")})
				fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case host.tarballPath:
						_, _ = w.Write(tarball)
					case host.listingPath:
						_, _ = w.Write([]byte(`[]`))
					default:
						http.NotFound(w, r)
					}
				}))
				_, err := fetcher.FetchAllSpecs(context.Background(), host.rootURL)
				require.ErrorContains(t, err, "no .json spec files found")
			})
		})
	}
}

func TestIsDirectJSONChild(t *testing.T) {
	tests := []struct {
		relPath string
		dir     string
		want    bool
	}{
		{"ethereum.json", "", true},
		{"docs/nested.json", "", false},
		{"specs/ethereum.json", "specs", true},
		{"specs/deep/nested.json", "specs", false},
		{"specs/ethereum.json", "specs/", true}, // trailing slash on dir
		{"README.md", "", false},
		{"specs/README.md", "specs", false},
	}

	for _, tt := range tests {
		t.Run(tt.relPath+"|"+tt.dir, func(t *testing.T) {
			require.Equal(t, tt.want, isDirectJSONChild(tt.relPath, tt.dir))
		})
	}
}

func TestBuildGitLabTarballURL_SlashedRef(t *testing.T) {
	fetcher := New(DefaultConfig())
	info := &RepoInfo{
		Provider:    ProviderGitLab,
		Host:        "https://gitlab.example.com",
		ProjectPath: "group/subgroup/repo",
		Branch:      "release/v1",
	}
	require.Equal(t,
		"https://gitlab.example.com/group/subgroup/repo/-/archive/release/v1/repo-release-v1.tar.gz",
		fetcher.buildGitLabTarballURL(info))
}
