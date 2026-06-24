// Package docs reads the published docs catalog (chains-data.js) — the single
// source for docs-derived per-chain metadata: the ecosystem/family and the icon
// slug. Both classify and icons consume it, so the taxonomy isn't maintained by
// hand in this repo; it's extrapolated from the docs.
//
// Catalog: http://docs.magmadevs.com/javascripts/chains-data.js — objects like
//   {"id":"ethereum","name":"Ethereum","eco":"EVM",…,"specs":[".../ethereum.json"]}
// keyed here by spec-file basename (e.g. "ethereum.json").
package docs

import (
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"
)

const catalogURL = "http://docs.magmadevs.com/javascripts/chains-data.js"

// Info is what the docs know about one chain.
type Info struct {
	Slug string // icon slug, e.g. "avalanche-c"
	Eco  string // ecosystem: "EVM" | "L1" | "Cosmos" | "BTC" | "Specialty"
}

// Catalog maps spec-file basename → Info.
type Catalog struct {
	byFile map[string]Info
}

// objRe captures id, eco, and the spec-file basename. chains-data.js field
// order (id, name, eco, …, specs) is stable.
var objRe = regexp.MustCompile(`"id":"([^"]*)","name":"[^"]*","eco":"([^"]*)"[^}]*?/([^"/]+)\.json"`)

var (
	once   sync.Once
	cached *Catalog
)

// Load fetches and parses the docs catalog once (memoized for the process).
// A network failure yields an empty catalog — callers fall back gracefully.
func Load() *Catalog {
	once.Do(func() { cached = fetch() })
	return cached
}

func fetch() *Catalog {
	c := &Catalog{byFile: map[string]Info{}}
	cl := &http.Client{Timeout: 8 * time.Second}
	resp, err := cl.Get(catalogURL)
	if err != nil {
		return c
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return c
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	for _, m := range objRe.FindAllStringSubmatch(string(body), -1) {
		c.byFile[m[3]+".json"] = Info{Slug: m[1], Eco: m[2]}
	}
	return c
}

// Lookup returns the docs Info for a spec-file basename.
func (c *Catalog) Lookup(specFile string) (Info, bool) {
	if c == nil {
		return Info{}, false
	}
	i, ok := c.byFile[specFile]
	return i, ok
}

// Len reports how many chains the docs catalog covers (for status display).
func (c *Catalog) Len() int {
	if c == nil {
		return 0
	}
	return len(c.byFile)
}
