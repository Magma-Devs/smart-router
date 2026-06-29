// Package catalog enumerates Lava chain specs and resolves each chain's
// effective interfaces + addons (merged transitively across `imports`).
//
// Mirrors scripts/wizard/lib/specs.sh: source the magma-Devs/lava-specs repo
// (flat <chain>.json, each .proposal.specs[] keyed by .index), then UNION every
// chain's api_collections with those of every spec it transitively imports.
//
// Sources, in order: `gh` tarball → raw github tarball → local specs/ dir.
package catalog

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

// Chain is one resolved spec the wizard can target.
type Chain struct {
	Index      string   // spec index, e.g. "ETH1"
	Name       string   // display name, e.g. "ethereum mainnet"
	Interfaces []string // merged, e.g. ["jsonrpc"] or ["grpc","rest","tendermintrpc"]
	Addons     []string // merged non-empty add_ons, e.g. ["debug","trace"]
	Imports    []string // direct imports (for classification)
	IsTestnet  bool     // heuristic from the name
	SpecFile   string   // source spec file basename, e.g. "ethereum.json" (for icon slug)
}

const (
	repo = "magma-Devs/lava-specs"
	ref  = "main"
)

// rawSpec is the slice of a spec file we care about.
type rawSpec struct {
	Index          string   `json:"index"`
	Name           string   `json:"name"`
	Enabled        *bool    `json:"enabled"`
	Imports        []string `json:"imports"`
	APICollections []struct {
		CollectionData struct {
			APIInterface string `json:"api_interface"`
			AddOn        string `json:"add_on"`
		} `json:"collection_data"`
		Extensions []struct {
			Name string `json:"name"`
		} `json:"extensions"`
	} `json:"api_collections"`
	file string // source basename, set during parse (not from JSON)
}

type specFile struct {
	Proposal struct {
		Specs []rawSpec `json:"specs"`
	} `json:"proposal"`
}

// Source selects where specs come from.
type Source int

const (
	SourceAuto   Source = iota // remote (gh → raw) then local fallback
	SourceRemote               // force remote (gh → raw), no local fallback
	SourceLocal                // force the local specs/ dir
)

// Load enumerates and resolves every enabled chain. localDir is the repo's
// specs/ dir. The returned source string ("gh"/"raw"/"local") is for display.
func Load(localDir string) (chains []Chain, source string, err error) {
	return LoadFrom(localDir, SourceAuto)
}

// LoadFrom is Load with an explicit source preference.
func LoadFrom(localDir string, pref Source) (chains []Chain, source string, err error) {
	specs, source, err := loadRawSpecsPref(localDir, pref)
	if err != nil {
		return nil, "", err
	}
	byIndex := make(map[string]rawSpec, len(specs))
	for _, s := range specs {
		byIndex[s.Index] = s
	}
	for _, s := range specs {
		if s.Enabled != nil && !*s.Enabled {
			continue
		}
		ifs, addons := resolve(s.Index, byIndex, map[string]bool{})
		chains = append(chains, Chain{
			Index:      s.Index,
			Name:       prettyName(s.Name),
			Interfaces: ifs,
			Addons:     addons,
			Imports:    s.Imports,
			IsTestnet:  isTestnet(s.Name, s.Index),
			SpecFile:   s.file,
		})
	}
	sort.Slice(chains, func(i, j int) bool { return chains[i].Index < chains[j].Index })
	return chains, source, nil
}

// resolve unions a spec's interfaces+addons with those of its imports (cycle-safe).
func resolve(index string, byIndex map[string]rawSpec, seen map[string]bool) (ifs, addons []string) {
	if seen[index] {
		return nil, nil
	}
	seen[index] = true
	s, ok := byIndex[index]
	if !ok {
		return nil, nil
	}
	ifSet, addonSet := map[string]bool{}, map[string]bool{}
	for _, c := range s.APICollections {
		if v := c.CollectionData.APIInterface; v != "" {
			ifSet[v] = true
		}
		if v := c.CollectionData.AddOn; v != "" {
			addonSet[v] = true
		}
		// Extensions (e.g. "archive") are addon-like capabilities the health
		// command verifies; Cosmos chains carry them here, not under add_on.
		for _, ext := range c.Extensions {
			if ext.Name != "" {
				addonSet[ext.Name] = true
			}
		}
	}
	for _, imp := range s.Imports {
		iifs, iaddons := resolve(imp, byIndex, seen)
		for _, v := range iifs {
			ifSet[v] = true
		}
		for _, v := range iaddons {
			addonSet[v] = true
		}
	}
	return sortedKeys(ifSet), sortedKeys(addonSet)
}

func loadRawSpecsPref(localDir string, pref Source) ([]rawSpec, string, error) {
	tryRemote := func() ([]rawSpec, string, bool) {
		if data, err := ghTarball(); err == nil {
			if specs, err := parseTarball(data); err == nil && len(specs) > 0 {
				return specs, "gh", true
			}
		}
		if data, err := httpTarball(); err == nil {
			if specs, err := parseTarball(data); err == nil && len(specs) > 0 {
				return specs, "raw", true
			}
		}
		return nil, "", false
	}
	tryLocal := func() ([]rawSpec, string, bool) {
		if specs, err := parseDir(localDir); err == nil && len(specs) > 0 {
			return specs, "local", true
		}
		return nil, "", false
	}

	switch pref {
	case SourceLocal:
		if specs, src, ok := tryLocal(); ok {
			return specs, src, nil
		}
		return nil, "", fmt.Errorf("no local specs in %s", localDir)
	case SourceRemote:
		if specs, src, ok := tryRemote(); ok {
			return specs, src, nil
		}
		return nil, "", fmt.Errorf("remote spec fetch failed (gh / raw github)")
	default: // SourceAuto
		if specs, src, ok := tryRemote(); ok {
			return specs, src, nil
		}
		if specs, src, ok := tryLocal(); ok {
			return specs, src, nil
		}
		return nil, "", fmt.Errorf("no spec source reachable (gh, raw github, or %s)", localDir)
	}
}

func ghTarball() ([]byte, error) {
	if _, err := exec.LookPath("gh"); err != nil {
		return nil, err
	}
	cmd := exec.Command("gh", "api", fmt.Sprintf("repos/%s/tarball/%s", repo, ref))
	return cmd.Output()
}

func httpTarball() ([]byte, error) {
	url := fmt.Sprintf("https://github.com/%s/archive/refs/heads/%s.tar.gz", repo, ref)
	cl := &http.Client{Timeout: 20 * time.Second}
	resp, err := cl.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func parseTarball(data []byte) ([]rawSpec, error) {
	gz, err := gzip.NewReader(strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var out []rawSpec
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if !strings.HasSuffix(h.Name, ".json") || strings.Count(h.Name, "/") != 1 {
			continue // only top-level *.json
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		var sf specFile
		if json.Unmarshal(b, &sf) == nil {
			base := h.Name[strings.LastIndex(h.Name, "/")+1:]
			for i := range sf.Proposal.Specs {
				sf.Proposal.Specs[i].file = base
			}
			out = append(out, sf.Proposal.Specs...)
		}
	}
	return out, nil
}

func parseDir(dir string) ([]rawSpec, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []rawSpec
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var sf specFile
		if json.Unmarshal(b, &sf) == nil {
			for i := range sf.Proposal.Specs {
				sf.Proposal.Specs[i].file = e.Name()
			}
			out = append(out, sf.Proposal.Specs...)
		}
	}
	return out, nil
}

// acronyms keep their canonical casing when title-casing a chain name.
var acronyms = map[string]string{
	"bsc": "BSC", "evm": "EVM", "rpc": "RPC", "p": "P", "c": "C",
	"btc": "BTC", "eth": "ETH", "bch": "BCH", "ltc": "LTC", "xrp": "XRP",
	"ton": "TON", "iota": "IOTA", "near": "NEAR", "sdk": "SDK", "ibc": "IBC",
	"l1": "L1", "l2": "L2", "api": "API", "ai": "AI", "evmos": "Evmos",
}

// smallWords stay lowercase mid-name (but are capitalized when leading).
var smallWords = map[string]bool{"of": true, "and": true, "the": true}

// prettyName turns "celo alfajores testnet" → "Celo Alfajores Testnet", with
// acronym-aware casing (e.g. "bsc mainnet" → "BSC Mainnet").
func prettyName(name string) string {
	words := strings.Fields(name)
	for i, w := range words {
		lw := strings.ToLower(w)
		if ac, ok := acronyms[lw]; ok {
			words[i] = ac
			continue
		}
		if i > 0 && smallWords[lw] {
			words[i] = lw
			continue
		}
		r := []rune(lw)
		r[0] = unicode.ToUpper(r[0])
		words[i] = string(r)
	}
	return strings.Join(words, " ")
}

func isTestnet(name, index string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, "testnet") || strings.Contains(n, "sepolia") ||
		strings.Contains(n, "holesky") || strings.Contains(n, "devnet") ||
		strings.HasSuffix(index, "T") && strings.ToUpper(index) == index
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
