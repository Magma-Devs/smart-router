package keeper

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// removedSpecFields are the 15 fields stripped from the bundled specs during the
// field-level cleanup (they no longer exist in the Go spec model). The guard
// below fails if any reappears under specs/, so a manual edit or an upstream
// re-sync can't silently reintroduce a dead field.
//
// The dedicated legacy-compatibility fixture lives under utils/keeper/testdata/,
// not specs/, so it is intentionally outside this guard's scope.
var removedSpecFields = map[string]struct{}{
	"min_stake_provider": {}, "providers_types": {}, "contributor": {},
	"contributor_percentage": {}, "shares": {}, "identity": {},
	"block_last_updated": {}, "reliability_threshold": {}, "data_reliability_enabled": {},
	"title": {}, "description": {}, "deposit": {},
	"extra_compute_units": {}, "local": {}, "subscription": {},
}

func collectRemovedKeys(v interface{}, path string, hits *[]string) {
	switch node := v.(type) {
	case map[string]interface{}:
		for k, child := range node {
			if _, bad := removedSpecFields[k]; bad {
				*hits = append(*hits, path+"."+k)
			}
			collectRemovedKeys(child, path+"."+k, hits)
		}
	case []interface{}:
		for i, item := range node {
			collectRemovedKeys(item, fmt.Sprintf("%s[%d]", path, i), hits)
		}
	}
}

// TestBundledSpecsHaveNoRemovedFields guards the bundled spec mirror against
// reintroduction of the fields removed in the field-level cleanup.
func TestBundledSpecsHaveNoRemovedFields(t *testing.T) {
	dir := filepath.Join("..", "..", "specs")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	checked := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		var doc interface{}
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("%s: invalid JSON: %v", e.Name(), err)
		}
		var hits []string
		collectRemovedKeys(doc, e.Name(), &hits)
		if len(hits) > 0 {
			t.Errorf("%s reintroduced removed spec field(s): %v", e.Name(), hits)
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("no bundled specs found under %s", dir)
	}
	t.Logf("checked %d bundled specs; none carry a removed field", checked)
}
