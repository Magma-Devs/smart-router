package icons

import (
	"strings"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/docs"
)

// docsBase is the published docs origin serving the chain SVGs.
const docsBase = "http://docs.magmadevs.com/assets/chains"

// slugMap resolves a spec-file basename to a docs icon slug, sourced from the
// shared docs catalog (no duplicate fetch) with a filename-derived fallback.
type slugMap struct {
	cat *docs.Catalog
}

func loadSlugMap() *slugMap { return &slugMap{cat: docs.Load()} }

// slugFor returns the docs icon slug for a spec file, preferring the docs
// catalog and falling back to a filename derivation.
func (m *slugMap) slugFor(specFile string) string {
	if specFile == "" {
		return ""
	}
	if info, ok := m.cat.Lookup(specFile); ok && info.Slug != "" {
		return info.Slug
	}
	return deriveSlug(specFile)
}

// deriveSlug turns "avalanche_c.json" → "avalanche-c" as a best-effort fallback.
func deriveSlug(specFile string) string {
	s := strings.TrimSuffix(specFile, ".json")
	s = strings.ReplaceAll(s, "_", "-")
	return strings.ToLower(s)
}

// iconURL is the published SVG URL for a slug ("" if no slug).
func iconURL(slug string) string {
	if slug == "" {
		return ""
	}
	return docsBase + "/" + slug + ".svg"
}
