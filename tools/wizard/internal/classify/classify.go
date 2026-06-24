// Package classify buckets each chain into exactly one family:
// EVM · Cosmos · BTC · Other-L1 · Specialty.
//
// The taxonomy is EXTRAPOLATED FROM THE DOCS: docs/chains-data.js carries an
// "eco" field (EVM | L1 | Cosmos | BTC | Specialty) for every published chain,
// keyed by spec-file. We map that directly (L1 → Other-L1). For the handful of
// spec files the docs don't list (import-only base specs like cosmossdk/ibc —
// never user-selectable) we fall back to a structural heuristic, so nothing is
// hand-maintained here.
package classify

import (
	"slices"
	"strings"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/docs"
)

type Family string

const (
	EVM       Family = "EVM"
	Cosmos    Family = "Cosmos"
	BTC       Family = "BTC"
	OtherL1   Family = "Other-L1"
	Specialty Family = "Specialty"
)

// Order is the display order of family tabs.
var Order = []Family{EVM, Cosmos, BTC, OtherL1, Specialty}

// Icon is a small glyph shown next to a family in the UI. All single-width
// (emoji-presentation glyphs like ⚛/⬡/✦ render double-width and break tab/row
// alignment).
func (f Family) Icon() string {
	switch f {
	case EVM:
		return "◆"
	case Cosmos:
		return "◇"
	case BTC:
		return "₿"
	case OtherL1:
		return "◈"
	default:
		return "●"
	}
}

// fromEco maps a docs "eco" string to a Family.
func fromEco(eco string) (Family, bool) {
	switch strings.ToLower(eco) {
	case "evm":
		return EVM, true
	case "cosmos":
		return Cosmos, true
	case "btc":
		return BTC, true
	case "l1":
		return OtherL1, true
	case "specialty":
		return Specialty, true
	}
	return "", false
}

// FamilyOf returns the family for a single chain using the memoized docs
// catalog. Convenience for UI code that doesn't hold a byIndex map; the EVM
// import-walk fallback is skipped (only matters for unlisted base specs).
func FamilyOf(c catalog.Chain) Family {
	return Of(c, docs.Load(), nil)
}

// Of returns the family for a chain — docs eco first, structural fallback
// otherwise. cat is the docs catalog; byIndex backs the EVM import walk.
func Of(c catalog.Chain, cat *docs.Catalog, byIndex map[string]catalog.Chain) Family {
	if info, ok := cat.Lookup(c.SpecFile); ok {
		if f, ok := fromEco(info.Eco); ok {
			return f
		}
	}
	return heuristic(c, byIndex)
}

// Classify buckets every chain (docs-driven), preserving catalog order within
// each bucket. Fetches the docs catalog once.
func Classify(chains []catalog.Chain) map[Family][]catalog.Chain {
	cat := docs.Load()
	byIndex := make(map[string]catalog.Chain, len(chains))
	for _, c := range chains {
		byIndex[c.Index] = c
	}
	out := map[Family][]catalog.Chain{}
	for _, c := range chains {
		out[Of(c, cat, byIndex)] = append(out[Of(c, cat, byIndex)], c)
	}
	return out
}

// heuristic is the fallback for spec files the docs don't list (import-only
// base specs). Structural only — no per-chain allow-lists.
func heuristic(c catalog.Chain, byIndex map[string]catalog.Chain) Family {
	if isCosmos(c) {
		return Cosmos
	}
	if importsETH1(c, byIndex, map[string]bool{}) || strings.EqualFold(c.Index, "ETH1") {
		return EVM
	}
	if hasInterface(c, "jsonrpc") || hasInterface(c, "rest") {
		return OtherL1
	}
	return Specialty
}

func isCosmos(c catalog.Chain) bool {
	if hasInterface(c, "tendermintrpc") || hasInterface(c, "grpc") {
		return true
	}
	for _, imp := range c.Imports {
		u := strings.ToUpper(imp)
		if strings.HasPrefix(u, "COSMOS") || u == "IBC" {
			return true
		}
	}
	return false
}

func importsETH1(c catalog.Chain, byIndex map[string]catalog.Chain, seen map[string]bool) bool {
	if seen[c.Index] {
		return false
	}
	seen[c.Index] = true
	for _, imp := range c.Imports {
		if strings.EqualFold(imp, "ETH1") {
			return true
		}
		if parent, ok := byIndex[imp]; ok && importsETH1(parent, byIndex, seen) {
			return true
		}
	}
	return false
}

func hasInterface(c catalog.Chain, name string) bool {
	return slices.Contains(c.Interfaces, name)
}
