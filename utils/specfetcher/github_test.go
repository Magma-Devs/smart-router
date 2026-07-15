package specfetcher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// rewriteTransport redirects every request to the test server while keeping
// the request path, so one handler can serve codeload, api.github.com and
// raw.githubusercontent.com traffic (their paths don't overlap).
type rewriteTransport struct {
	serverURL *url.URL
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = rt.serverURL.Scheme
	req.URL.Host = rt.serverURL.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newTestFetcher(t *testing.T, handler http.Handler) *Fetcher {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	config := DefaultConfig()
	config.HTTPClient = &http.Client{Transport: rewriteTransport{serverURL: serverURL}}
	return New(config)
}

func specProposalJSON(indexes ...string) []byte {
	specs := ""
	for i, index := range indexes {
		if i > 0 {
			specs += ","
		}
		specs += fmt.Sprintf(`{"index": %q}`, index)
	}
	return []byte(fmt.Sprintf(`{"proposal": {"specs": [%s]}}`, specs))
}

// buildTarball creates an in-memory gzipped tarball with the given entries,
// prefixed with topDir the way codeload prefixes "{repo}-{ref}/".
func buildTarball(t *testing.T, topDir string, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	require.NoError(t, tw.WriteHeader(&tar.Header{Name: topDir + "/", Typeflag: tar.TypeDir, Mode: 0o755}))
	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     topDir + "/" + name,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(content)),
		}))
		_, err := tw.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func TestFetchAllSpecs_GitHubTarball(t *testing.T) {
	tarball := buildTarball(t, "lava-specs-main", map[string][]byte{
		"ethereum.json":    specProposalJSON("ETH1"),
		"solana.json":      specProposalJSON("SOLANA"),
		"README.md":        []byte("# not a spec"),
		"docs/nested.json": specProposalJSON("NESTED"), // subdirectory — must be skipped for root path
	})

	var requestedPaths []string
	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.URL.Path)
		if r.URL.Path == "/magma-devs/lava-specs/tar.gz/main" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/magma-devs/lava-specs/tree/main/")
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Contains(t, specs, "ETH1")
	require.Contains(t, specs, "SOLANA")
	require.NotContains(t, specs, "NESTED")
	require.Equal(t, []string{"/magma-devs/lava-specs/tar.gz/main"}, requestedPaths)
}

func TestFetchAllSpecs_GitHubTarball_BareRepoURLUsesHEAD(t *testing.T) {
	tarball := buildTarball(t, "lava-specs-main", map[string][]byte{
		"ethereum.json": specProposalJSON("ETH1"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/magma-devs/lava-specs/tar.gz/HEAD" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/magma-devs/lava-specs/")
	require.NoError(t, err)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitHubTarball_CodeloadURL(t *testing.T) {
	tarball := buildTarball(t, "lava-specs-main", map[string][]byte{
		"ethereum.json": specProposalJSON("ETH1"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/Magma-Devs/lava-specs/tar.gz/main" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://codeload.github.com/Magma-Devs/lava-specs/tar.gz/refs/heads/main")
	require.NoError(t, err)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitHubTarball_SubdirectoryPath(t *testing.T) {
	tarball := buildTarball(t, "repo-main", map[string][]byte{
		"specs/ethereum.json":    specProposalJSON("ETH1"),
		"specs/deep/nested.json": specProposalJSON("NESTED"),
		"other/solana.json":      specProposalJSON("SOLANA"),
		"root.json":              specProposalJSON("ROOT"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user/repo/tar.gz/main" {
			_, _ = w.Write(tarball)
			return
		}
		http.NotFound(w, r)
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/user/repo/tree/main/specs")
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitHub_FallbackToContentsAPI(t *testing.T) {
	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user/repo/tar.gz/main": // codeload — simulate failure
			http.Error(w, "not found", http.StatusNotFound)
		case "/repos/user/repo/contents/": // api.github.com directory listing
			require.Equal(t, "main", r.URL.Query().Get("ref"))
			_, _ = w.Write([]byte(`[{"name": "ethereum.json", "type": "file"}, {"name": "docs", "type": "dir"}]`))
		case "/user/repo/main/ethereum.json": // raw.githubusercontent.com
			_, _ = w.Write(specProposalJSON("ETH1"))
		default:
			http.NotFound(w, r)
		}
	}))

	specs, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/user/repo/tree/main/")
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Contains(t, specs, "ETH1")
}

func TestFetchAllSpecs_GitHub_TarballAndFallbackBothFail(t *testing.T) {
	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))

	_, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/user/repo/tree/main/")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tarball")
}

func TestFetchAllSpecs_GitHubTarball_NoSpecsFound(t *testing.T) {
	tarball := buildTarball(t, "repo-main", map[string][]byte{
		"README.md": []byte("nothing here"),
	})

	fetcher := newTestFetcher(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user/repo/tar.gz/main":
			_, _ = w.Write(tarball)
		case "/repos/user/repo/contents/":
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))

	_, err := fetcher.FetchAllSpecs(context.Background(), "https://github.com/user/repo/tree/main/")
	require.Error(t, err)
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
