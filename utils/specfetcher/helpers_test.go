package specfetcher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
