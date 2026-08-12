package keeper

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	types "github.com/magma-Devs/smart-router/types/spec"
	utils "github.com/magma-Devs/smart-router/utils"
)

func decodeProposal(path string) (types.SpecAddProposalJSON, error) {
	proposal := types.SpecAddProposalJSON{}
	contents, err := os.ReadFile(path)
	if err != nil {
		return proposal, err
	}
	// Note: we intentionally allow unknown fields because our standalone types
	// are a subset of the full protobuf-generated spec types. Spec JSON files
	// may contain fields (e.g. deprecated fields) that we don't model.
	err = json.Unmarshal(contents, &proposal)
	return proposal, err
}

// GetSpecFromLocalDirs loads specs from multiple directories, merging them into
// a single pool before resolving the requested spec and its dependencies.
// Later directories override earlier ones for the same spec index.
func GetSpecFromLocalDirs(specPaths []string, index string) (types.Spec, error) {
	allSpecs := map[string]types.Spec{}
	for _, specPath := range specPaths {
		_ = filepath.WalkDir(specPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			proposal, err := decodeProposal(path)
			if err != nil {
				return nil // skip unparseable files
			}
			for _, spec := range proposal.Proposal.Specs {
				allSpecs[spec.Index] = spec
			}
			return nil
		})
	}
	if len(allSpecs) == 0 {
		return types.Spec{}, fmt.Errorf("no specs loaded from any of %v", specPaths)
	}
	spec, err := expandSpecWithDependencies(allSpecs, index)
	if err != nil {
		return types.Spec{}, err
	}
	return *spec, nil
}

// expandSpecWithDependencies expands a spec by resolving all its dependencies (inherited specs).
func expandSpecWithDependencies(specs map[string]types.Spec, index string) (*types.Spec, error) {
	spec, ok := specs[index]
	if !ok {
		availableSpecs := make([]string, 0, len(specs))
		for id := range specs {
			availableSpecs = append(availableSpecs, id)
		}
		return nil, fmt.Errorf("spec not found for chainId: %s (available specs: %v)", index, availableSpecs)
	}

	getBaseSpec := func(_ context.Context, idx string) (types.Spec, bool) {
		s, found := specs[idx]
		return s, found
	}

	depends := map[string]bool{index: true}
	inherit := map[string]bool{}

	_, err := types.DoExpandSpec(context.Background(), &spec, depends, &inherit, spec.Index, getBaseSpec)
	if err != nil {
		return nil, fmt.Errorf("spec expand failed: %w", err)
	}

	return &spec, nil
}

// ExpandSpecWithDependencies is the public version of expandSpecWithDependencies.
// It expands a spec by resolving all its dependencies (inherited specs) from a provided spec map.
func ExpandSpecWithDependencies(specs map[string]types.Spec, index string) (*types.Spec, error) {
	return expandSpecWithDependencies(specs, index)
}

// GetAllSpecsFromFile loads all specs from a single file without expansion.
// Returns a map of specs keyed by their chain ID (Index).
func GetAllSpecsFromFile(path string) (map[string]types.Spec, error) {
	proposal, err := decodeProposal(path)
	if err != nil {
		return nil, fmt.Errorf("error decoding proposal from %s: %w", path, err)
	}

	specs := make(map[string]types.Spec)
	for _, spec := range proposal.Proposal.Specs {
		specs[spec.Index] = spec
	}
	return specs, nil
}

// GetAllSpecsFromLocalDir loads all specs from a local directory without expansion.
// Returns a map of specs keyed by their chain ID (Index).
// Later files in directory order override earlier ones for the same chain ID.
func GetAllSpecsFromLocalDir(specPath string) (map[string]types.Spec, error) {
	specs := make(map[string]types.Spec)
	var errs []error

	// Walk through all files and subdirectories in the specPath
	err := filepath.WalkDir(specPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errs = append(errs, fmt.Errorf("error accessing path %s: %w", path, err))
			return nil // Continue walking, but record the error
		}

		if d.IsDir() {
			return nil // Skip directories
		}

		// Only process JSON files
		if !strings.HasSuffix(path, ".json") {
			return nil
		}

		// Attempt to decode the proposal from the file
		proposal, err := decodeProposal(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("error decoding proposal from %s: %w", path, err))
			return nil // Continue walking, but record the error
		}

		// Extract specs from the proposal and add them to the map
		for _, spec := range proposal.Proposal.Specs {
			specs[spec.Index] = spec
		}
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 && len(specs) == 0 {
		// Only return error if we couldn't load any specs
		return nil, fmt.Errorf("failed to load any specs: %v", errs)
	}

	// Log loaded specs for debugging
	if len(specs) > 0 {
		specIDs := make([]string, 0, len(specs))
		for id := range specs {
			specIDs = append(specIDs, id)
		}
		utils.FormatInfo("Loaded specs from local directory",
			utils.LogAttr("spec_count", len(specs)),
			utils.LogAttr("directory", specPath),
			utils.LogAttr("spec_ids", strings.Join(specIDs, ", ")))
	}

	return specs, nil
}

// GetAllSpecsFromPath loads all specs from a local path (file or directory) without expansion.
// Returns a map of specs keyed by their chain ID (Index).
func GetAllSpecsFromPath(path string) (map[string]types.Spec, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
	}

	if fileInfo.IsDir() {
		return GetAllSpecsFromLocalDir(path)
	}

	return GetAllSpecsFromFile(path)
}
