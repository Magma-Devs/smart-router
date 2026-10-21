package spec

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-4185: a disabled api has to survive expansion into every spec that imports it, however
// deep, or the router sees the method as undeclared and relays it instead of refusing it.

func expandForTest(t *testing.T, specs map[string]Spec, index string) Spec {
	t.Helper()
	spec := specs[index]
	_, err := DoExpandSpec(context.Background(), &spec, map[string]bool{index: true}, &map[string]bool{}, index,
		func(_ context.Context, idx string) (Spec, bool) {
			s, ok := specs[idx]
			return s, ok
		})
	require.NoError(t, err)
	return spec
}

func collectionWith(apis ...*Api) *ApiCollection {
	return &ApiCollection{
		Enabled:        true,
		CollectionData: CollectionData{ApiInterface: "jsonrpc", Type: "POST"},
		Apis:           apis,
	}
}

func apiByName(t *testing.T, spec Spec, name string) *Api {
	t.Helper()
	require.Len(t, spec.ApiCollections, 1)
	var found *Api
	for _, api := range spec.ApiCollections[0].Apis {
		if api.Name == name {
			require.Nil(t, found, "%s appears twice", name)
			found = api
		}
	}
	return found
}

func TestExpand_DisabledApiReachesGrandchild(t *testing.T) {
	specs := map[string]Spec{
		"BASE": {Index: "BASE", Enabled: true, ApiCollections: []*ApiCollection{collectionWith(
			&Api{Name: "send", Enabled: true, ComputeUnits: 10},
			&Api{Name: "read", Enabled: true, ComputeUnits: 10},
		)}},
		"MID": {Index: "MID", Enabled: true, Imports: []string{"BASE"}, ApiCollections: []*ApiCollection{collectionWith(
			&Api{Name: "send", Enabled: false},
		)}},
		"LEAF": {Index: "LEAF", Enabled: true, Imports: []string{"MID"}, ApiCollections: []*ApiCollection{collectionWith()}},
	}

	for _, index := range []string{"MID", "LEAF"} {
		spec := expandForTest(t, specs, index)
		send := apiByName(t, spec, "send")
		require.NotNil(t, send, "%s lost the disabled marker", index)
		require.False(t, send.Enabled, index)
		require.True(t, apiByName(t, spec, "read").Enabled, index)
	}
}

func TestExpand_EnabledDefinitionBeatsInheritedDisabled(t *testing.T) {
	specs := map[string]Spec{
		"OFF": {Index: "OFF", Enabled: true, ApiCollections: []*ApiCollection{collectionWith(&Api{Name: "m", Enabled: false})}},
		"ON":  {Index: "ON", Enabled: true, ApiCollections: []*ApiCollection{collectionWith(&Api{Name: "m", Enabled: true, ComputeUnits: 5})}},
		// The importing spec re-enables it itself.
		"OWN": {Index: "OWN", Enabled: true, Imports: []string{"OFF"}, ApiCollections: []*ApiCollection{collectionWith(
			&Api{Name: "m", Enabled: true, ComputeUnits: 7},
		)}},
		// Two parents disagree, in both import orders: the enabled one is served.
		"OFF_ON": {Index: "OFF_ON", Enabled: true, Imports: []string{"OFF", "ON"}, ApiCollections: []*ApiCollection{collectionWith()}},
		"ON_OFF": {Index: "ON_OFF", Enabled: true, Imports: []string{"ON", "OFF"}, ApiCollections: []*ApiCollection{collectionWith()}},
	}

	own := apiByName(t, expandForTest(t, specs, "OWN"), "m")
	require.True(t, own.Enabled)
	require.Equal(t, uint64(7), own.ComputeUnits)

	for _, index := range []string{"OFF_ON", "ON_OFF"} {
		m := apiByName(t, expandForTest(t, specs, index), "m")
		require.NotNil(t, m, index)
		require.True(t, m.Enabled, index)
		require.Equal(t, uint64(5), m.ComputeUnits, index)
	}
}
