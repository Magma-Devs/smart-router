package specfetcher

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	types "github.com/magma-Devs/smart-router/types/spec"
)

// MaxTarballDecompressedBytes caps how much decompressed tarball data is read
// (decompression-bomb guard for operator-supplied repository URLs).
const MaxTarballDecompressedBytes = 256 << 20 // 256 MiB

// fetchTarball downloads a repository archive (gzipped tarball) and parses
// every .json spec file directly under pathInRepo. Both GitHub (codeload) and
// GitLab (/-/archive/) serve archives outside their metered REST APIs, so a
// single unauthenticated request replaces the listing + per-file flow.
func (f *Fetcher) fetchTarball(ctx context.Context, tarballURL, pathInRepo string, setHeaders func(*http.Request)) (map[string]types.Spec, error) {
	utils.LavaFormatInfo("Fetching specs tarball",
		utils.LogAttr("tarball_url", tarballURL))

	fetchCtx, cancel := context.WithTimeout(ctx, f.config.FileFetchTimeout)
	defer cancel()

	resp, err := f.doRequest(fetchCtx, http.MethodGet, tarballURL, setHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tarball: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tarball fetch error (status: %d)", resp.StatusCode)
	}

	specs, err := extractSpecsFromTarGz(resp.Body, pathInRepo)
	if err != nil {
		return nil, err
	}

	logLoadedSpecs(specs)
	return specs, nil
}

// extractSpecsFromTarGz streams a gzipped tarball and parses every .json spec
// file directly under pathInRepo (matching the listing APIs' non-recursive
// directory semantics).
func extractSpecsFromTarGz(r io.Reader, pathInRepo string) (map[string]types.Spec, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress tarball: %w", err)
	}
	defer gz.Close()

	specs := make(map[string]types.Spec)
	var parseErrors []string

	tarReader := tar.NewReader(io.LimitReader(gz, MaxTarballDecompressedBytes))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tarball: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}

		// Every entry is prefixed with a "{repo}-{ref}/" top-level directory.
		relPath, ok := stripTarTopLevelDir(header.Name)
		if !ok || !isDirectJSONChild(relPath, pathInRepo) {
			continue
		}

		content, err := io.ReadAll(tarReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s from tarball: %w", header.Name, err)
		}

		fileSpecs, err := parseSpecProposal(content)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("%s: failed to parse JSON: %v", relPath, err))
			continue
		}
		for k, v := range fileSpecs {
			specs[k] = v
		}
	}

	if len(specs) == 0 {
		if len(parseErrors) > 0 {
			return nil, fmt.Errorf("failed to parse specs from tarball: %s", strings.Join(parseErrors, "; "))
		}
		return nil, fmt.Errorf("no .json spec files found in tarball path %q", pathInRepo)
	}

	if len(parseErrors) > 0 {
		utils.LavaFormatWarning("Some spec files in the tarball failed to parse", nil,
			utils.LogAttr("error_count", len(parseErrors)),
			utils.LogAttr("errors", strings.Join(parseErrors, "; ")))
	}

	return specs, nil
}

// stripTarTopLevelDir drops the top-level directory archives prefix on every
// entry ("{repo}-{ref}/..."). Returns false for the top-level dir entry itself.
func stripTarTopLevelDir(name string) (string, bool) {
	idx := strings.IndexByte(name, '/')
	if idx < 0 || idx+1 >= len(name) {
		return "", false
	}
	return name[idx+1:], true
}

// isDirectJSONChild reports whether relPath is a .json file directly inside
// dir — matching the listing APIs' behaviour of listing a single directory
// non-recursively (dir == "" means the repository root).
func isDirectJSONChild(relPath, dir string) bool {
	if !strings.HasSuffix(relPath, ".json") {
		return false
	}
	if dir == "" {
		return !strings.Contains(relPath, "/")
	}
	return path.Dir(relPath) == strings.TrimSuffix(dir, "/")
}
