package spec

import (
	"encoding/json"
	"testing"
)

// TestLegacyFieldsAreIgnoredOnDecode replaces the former Coin / FlexFloat64 /
// Spec_ProvidersTypes decoding tests. Those custom unmarshalers were deleted
// together with the unused governance fields; this test guards the property
// that provides backward compatibility in their place: encoding/json silently
// ignores the removed keys, so legacy spec JSON still decodes and its active
// fields are preserved.
func TestLegacyFieldsAreIgnoredOnDecode(t *testing.T) {
	// A spec carrying every removed key, in the rich shapes the deleted custom
	// unmarshalers used to handle: a string coin amount, a string
	// contributor_percentage, api-level extra_compute_units and
	// category.local/subscription.
	legacy := []byte(`{
		"index": "LEGACY",
		"name": "Legacy Chain",
		"enabled": true,
		"average_block_time": 13000,
		"block_distance_for_finalized_data": 8,
		"reliability_threshold": 268435455,
		"data_reliability_enabled": true,
		"block_last_updated": 99,
		"min_stake_provider": {"denom": "ulava", "amount": "5000000000"},
		"providers_types": "static",
		"contributor": ["a", "b"],
		"contributor_percentage": "0.015",
		"shares": 3,
		"identity": "id",
		"api_collections": [
			{
				"enabled": true,
				"collection_data": {"api_interface": "jsonrpc", "type": "POST"},
				"apis": [
					{
						"name": "m",
						"compute_units": 10,
						"extra_compute_units": 99,
						"category": {"deterministic": true, "local": true, "subscription": true, "stateful": 1}
					}
				]
			}
		]
	}`)

	var s Spec
	if err := json.Unmarshal(legacy, &s); err != nil {
		t.Fatalf("legacy spec must decode, got: %v", err)
	}

	// Active fields survive.
	if s.Index != "LEGACY" || s.Name != "Legacy Chain" || !s.Enabled {
		t.Fatalf("active identity fields not preserved: %+v", s)
	}
	if s.AverageBlockTime != 13000 || s.BlockDistanceForFinalizedData != 8 {
		t.Fatalf("active block-timing fields not preserved: %+v", s)
	}
	if len(s.ApiCollections) != 1 || len(s.ApiCollections[0].Apis) != 1 {
		t.Fatalf("api collections not preserved: %+v", s)
	}
	api := s.ApiCollections[0].Apis[0]
	if api.Name != "m" || api.ComputeUnits != 10 {
		t.Fatalf("active api fields not preserved: %+v", api)
	}
	// compute_units is the only CU input; category.deterministic/stateful survive.
	if !api.Category.Deterministic || api.Category.Stateful != 1 {
		t.Fatalf("active category fields not preserved: %+v", api.Category)
	}
}

// TestUnknownProviderTypesVariantsDecode ensures the previously bespoke
// providers_types shapes (null, numeric, string) all still decode now that the
// field and its custom unmarshaler are gone — each is simply ignored.
func TestUnknownProviderTypesVariantsDecode(t *testing.T) {
	for _, raw := range []string{
		`{"index":"A","providers_types":null}`,
		`{"index":"B","providers_types":1}`,
		`{"index":"C","providers_types":"static"}`,
	} {
		var s Spec
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			t.Fatalf("providers_types variant %q must decode, got: %v", raw, err)
		}
		if s.Index == "" {
			t.Fatalf("active index lost for %q", raw)
		}
	}
}

// The finality field declaration (MAG-5339) is optional: a spec that sets it decodes with both
// halves, a spec that does not decodes with none, and half a declaration reads as none.
func TestSpecFinalityFieldDecode(t *testing.T) {
	for _, tc := range []struct {
		name, raw        string
		path, finalValue string
	}{
		{"declared", `{"index":"XRP","block_distance_for_finalized_data":4,"finality_field_path":"result.validated","finality_field_final_value":"true"}`, "result.validated", "true"},
		{"not declared", `{"index":"ETH1","block_distance_for_finalized_data":8}`, "", ""},
		{"path only", `{"index":"X","finality_field_path":"result.validated"}`, "", ""},
		{"value only", `{"index":"X","finality_field_final_value":"true"}`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s Spec
			if err := json.Unmarshal([]byte(tc.raw), &s); err != nil {
				t.Fatalf("spec must decode, got: %v", err)
			}
			path, finalValue := s.FinalityField()
			if path != tc.path || finalValue != tc.finalValue {
				t.Fatalf("FinalityField() = %q, %q, want %q, %q", path, finalValue, tc.path, tc.finalValue)
			}
		})
	}
	var none *Spec
	if path, finalValue := none.FinalityField(); path != "" || finalValue != "" {
		t.Fatalf("a nil spec declares nothing, got %q, %q", path, finalValue)
	}
}
