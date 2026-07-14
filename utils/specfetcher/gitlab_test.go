package specfetcher

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchAllSpecs_GitLabTarball(t *testing.T) {
	tarball := buildTarball(t, "myrepo-main", map[string][]byte{
		"ethereum.json":    specProposalJSON("ETH1"),
		"docs/nested.json": specProposalJSON("NESTED"), // subdirectory — must be skipped for root path
	})

	var requestedPaths []string
	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.URL.Path)
		if r.URL.Path == "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://gitlab.com/myorg/myrepo/-/tree/main")
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Contains(t, specs, "ETH1")
	require.Equal(t, []string{"/myorg/myrepo/-/archive/main/myrepo-main.tar.gz"}, requestedPaths)
}

func TestFetchAllSpecs_GitLabTarball_BareRepoURLUsesHEAD(t *testing.T) {
	tarball := buildTarball(t, "myrepo-HEAD", map[string][]byte{
		"ethereum.json": specProposalJSON("ETH1"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/myorg/myrepo/-/archive/HEAD/myrepo-HEAD.tar.gz" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://gitlab.com/myorg/myrepo")
	require.NoError(t, err)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitLabTarball_ArchiveURL(t *testing.T) {
	tarball := buildTarball(t, "myrepo-main", map[string][]byte{
		"ethereum.json": specProposalJSON("ETH1"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://gitlab.com/myorg/myrepo/-/archive/main/myrepo-main.tar.gz")
	require.NoError(t, err)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitLab_FallbackToTreeAPI(t *testing.T) {
	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Note: r.URL.Path is the decoded path, so %2F in the project id shows as "/".
		switch r.URL.Path {
		case "/myorg/myrepo/-/archive/main/myrepo-main.tar.gz": // archive — simulate failure
			http.Error(w, "not found", http.StatusNotFound)
		case "/api/v4/projects/myorg/myrepo/repository/tree":
			require.Equal(t, "main", r.URL.Query().Get("ref"))
			_, _ = w.Write([]byte(`[{"name": "ethereum.json", "type": "blob", "path": "ethereum.json"}, {"name": "docs", "type": "tree", "path": "docs"}]`))
		case "/api/v4/projects/myorg/myrepo/repository/files/ethereum.json/raw":
			_, _ = w.Write(specProposalJSON("ETH1"))
		default:
			http.NotFound(w, r)
		}
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://gitlab.com/myorg/myrepo/-/tree/main")
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Contains(t, specs, "ETH1")
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
