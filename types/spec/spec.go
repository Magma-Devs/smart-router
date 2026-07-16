package spec

import (
	"fmt"
	"sort"
)

// Spec describes a single blockchain specification including all API
// collections and performance parameters.
type Spec struct {
	Index                         string           `json:"index"                            mapstructure:"index"`
	Name                          string           `json:"name"                             mapstructure:"name"`
	Enabled                       bool             `json:"enabled,omitempty"`
	BlockDistanceForFinalizedData uint32           `json:"block_distance_for_finalized_data,omitempty"`
	BlocksInFinalizationProof     uint32           `json:"blocks_in_finalization_proof,omitempty"`
	AverageBlockTime              int64            `json:"average_block_time,omitempty"`
	AllowedBlockLagForQosSync     int64            `json:"allowed_block_lag_for_qos_sync,omitempty"`
	Imports                       []string         `json:"imports,omitempty"`
	ApiCollections                []*ApiCollection `json:"api_collections,omitempty"`
}

// ---------------------------------------------------------------------------
// Spec getters (nil-safe value accessors matching the original protobuf API)
// ---------------------------------------------------------------------------

func (m *Spec) GetIndex() string {
	if m != nil {
		return m.Index
	}
	return ""
}

func (m *Spec) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Spec) GetEnabled() bool {
	if m != nil {
		return m.Enabled
	}
	return false
}

func (m *Spec) GetBlockDistanceForFinalizedData() uint32 {
	if m != nil {
		return m.BlockDistanceForFinalizedData
	}
	return 0
}

func (m *Spec) GetBlocksInFinalizationProof() uint32 {
	if m != nil {
		return m.BlocksInFinalizationProof
	}
	return 0
}

func (m *Spec) GetAverageBlockTime() int64 {
	if m != nil {
		return m.AverageBlockTime
	}
	return 0
}

func (m *Spec) GetAllowedBlockLagForQosSync() int64 {
	if m != nil {
		return m.AllowedBlockLagForQosSync
	}
	return 0
}

func (m *Spec) GetImports() []string {
	if m != nil {
		return m.Imports
	}
	return nil
}

func (m *Spec) GetApiCollections() []*ApiCollection {
	if m != nil {
		return m.ApiCollections
	}
	return nil
}

// ---------------------------------------------------------------------------
// Spec methods (ported from original x/spec/types/spec.go without Cosmos deps)
// ---------------------------------------------------------------------------

// CombineCollections appends parent api collections that are not already
// present in the current spec, combining multiple parent entries for the same
// CollectionData into a single merged collection.
func (spec *Spec) CombineCollections(parentsCollections map[CollectionData][]*ApiCollection) error {
	if spec == nil {
		return fmt.Errorf("CombineCollections: spec is nil")
	}

	// Build a deterministically sorted list of keys.
	collectionDataList := make([]CollectionData, 0, len(parentsCollections))
	for key := range parentsCollections {
		collectionDataList = append(collectionDataList, key)
	}
	sort.SliceStable(collectionDataList, func(i, j int) bool {
		return collectionDataList[i].String() < collectionDataList[j].String()
	})

	for _, collectionData := range collectionDataList {
		collectionsToCombine := parentsCollections[collectionData]
		if len(collectionsToCombine) == 0 {
			return fmt.Errorf("collection with length 0 %v", collectionData)
		}

		var combined *ApiCollection
		var others []*ApiCollection
		for i := 0; i < len(collectionsToCombine); i++ {
			combined = collectionsToCombine[i]
			others = append(collectionsToCombine[:i:i], collectionsToCombine[i+1:]...)
			if combined.Enabled {
				break
			}
		}
		if combined == nil || !combined.Enabled {
			// No enabled collection to combine; skip.
			continue
		}

		if err := combined.CombineWithOthers(others, false, false); err != nil {
			return err
		}
		spec.ApiCollections = append(spec.ApiCollections, combined)
	}
	return nil
}
