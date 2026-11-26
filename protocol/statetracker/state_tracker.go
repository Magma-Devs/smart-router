package statetracker

import (
	"context"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	speckeeper "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/specfetcher"
)

// expandCommaSeparatedPaths takes a slice of paths (from StringArray flag) and expands
// any comma-separated values within each element. This allows users to specify multiple
// local files either as separate flags or as comma-separated values:
//
//	--use-static-spec file1.json --use-static-spec file2.json
//	--use-static-spec file1.json,file2.json
//	--use-static-spec file1.json,file2.json --use-static-spec file3.json
//
// Note: Comma-separated values are only supported for local files, not for remote URLs.
func expandCommaSeparatedPaths(specPaths []string) []string {
	var expanded []string
	for _, path := range specPaths {
		// Skip empty paths
		if path == "" {
			continue
		}

		// If it's a remote URL, don't split by comma (URLs may contain commas in query params)
		if specfetcher.IsRemoteRepoURL(path) {
			expanded = append(expanded, path)
			continue
		}

		// Split by comma for local paths
		parts := strings.Split(path, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				expanded = append(expanded, trimmed)
			}
		}
	}
	return expanded
}

// LoadStaticSpecs loads specs from multiple sources and aggregates them, expands the endpoint's spec with
// the write-method lock applied (speckeeper.ExpandSpecWithWriteLock) and reports what the lock forced.
// Sources are processed in order; later sources override earlier ones for the same chain ID.
//
// It supports:
//   - Remote repositories (GitHub/GitLab) - detected automatically by URL pattern
//   - Local directories - all JSON files in the directory are loaded
//   - Local files - single JSON file or comma-separated list of files
//
// Local files can be specified either as separate flags or comma-separated:
//
//	--use-static-spec file1.json --use-static-spec file2.json
//	--use-static-spec file1.json,file2.json
//
// For remote repositories, the appropriate token (githubToken or gitlabToken) is used for authentication.
// smart-router loads specs statically; empty specPaths is an error (there is no live blockchain fallback).
func LoadStaticSpecs(ctx context.Context, chainParser chainlib.ChainParser, specPaths []string, rpcEndpoint routersession.RPCEndpoint, githubToken string, gitlabToken string) (speckeeper.WriteLockReport, error) {
	if len(specPaths) == 0 {
		return speckeeper.WriteLockReport{}, utils.FormatError("no spec paths provided", nil,
			utils.LogAttr("chain_id", rpcEndpoint.ChainID))
	}

	// Expand comma-separated paths (for local files)
	expandedPaths := expandCommaSeparatedPaths(specPaths)

	if len(expandedPaths) == 0 {
		return speckeeper.WriteLockReport{}, utils.FormatError("no valid spec paths after expansion", nil,
			utils.LogAttr("chain_id", rpcEndpoint.ChainID))
	}

	// Aggregate all specs from all sources
	aggregatedSpecs := make(map[string]spectypes.Spec)

	for i, specPath := range expandedPaths {
		utils.FormatInfo("Loading specs from source",
			utils.LogAttr("source_index", i+1),
			utils.LogAttr("total_sources", len(expandedPaths)),
			utils.LogAttr("source", specPath))

		specs, err := loadAllSpecsFromSource(ctx, specPath, githubToken, gitlabToken)
		if err != nil {
			return speckeeper.WriteLockReport{}, utils.FormatError("failed loading specs from source", err,
				utils.LogAttr("source_index", i+1),
				utils.LogAttr("source", specPath))
		}

		// Merge into aggregated specs (later sources override earlier)
		for chainID, spec := range specs {
			if _, exists := aggregatedSpecs[chainID]; exists {
				utils.FormatInfo("Overriding spec from later source",
					utils.LogAttr("chain_id", chainID),
					utils.LogAttr("source", specPath))
			}
			aggregatedSpecs[chainID] = spec
		}
	}

	if len(aggregatedSpecs) == 0 {
		return speckeeper.WriteLockReport{}, utils.FormatError("no specs loaded from any source", nil,
			utils.LogAttr("sources", expandedPaths))
	}

	// Expand the requested spec with its dependencies
	spec, writeLock, err := speckeeper.ExpandSpecWithWriteLock(aggregatedSpecs, rpcEndpoint.ChainID)
	if err != nil {
		return speckeeper.WriteLockReport{}, utils.FormatError("failed expanding spec", err,
			utils.LogAttr("chain_id", rpcEndpoint.ChainID),
			utils.LogAttr("available_specs", len(aggregatedSpecs)))
	}

	utils.FormatInfo("Successfully loaded and expanded spec from aggregated sources",
		utils.LogAttr("chain_id", spec.Index),
		utils.LogAttr("total_sources", len(specPaths)),
		utils.LogAttr("total_specs_loaded", len(aggregatedSpecs)))

	// A chain takes its family from its spec's imports, so the families are registered from the
	// catalog just loaded, before the parser reads its own (SetSpec) and before any reply is
	// classified (MAG-5246).
	common.RegisterChainFamilies(aggregatedSpecs)
	if _, ok := common.GetChainFamily(rpcEndpoint.ChainID); !ok {
		utils.FormatWarning("Chain has no chain family: no chain-specific error row applies to its replies", nil,
			utils.LogAttr("chain_id", rpcEndpoint.ChainID))
	}

	chainParser.SetSpec(*spec)
	return writeLock, nil
}

// RegisterForSpecUpdatesOrSetStaticSpecsWithToken is LoadStaticSpecs for a caller that does not publish
// the write-method lock's gauge: the health command and the tests.
func RegisterForSpecUpdatesOrSetStaticSpecsWithToken(ctx context.Context, chainParser chainlib.ChainParser, specPaths []string, rpcEndpoint routersession.RPCEndpoint, githubToken string, gitlabToken string) error {
	_, err := LoadStaticSpecs(ctx, chainParser, specPaths, rpcEndpoint, githubToken, gitlabToken)
	return err
}

// loadAllSpecsFromSource loads all specs from a single source without expansion.
// Returns a map of specs keyed by their chain ID.
func loadAllSpecsFromSource(ctx context.Context, source, githubToken, gitlabToken string) (map[string]spectypes.Spec, error) {
	// Check if it's a remote repository URL
	if specfetcher.IsRemoteRepoURL(source) {
		return loadAllSpecsFromRemoteRepo(ctx, source, githubToken, gitlabToken)
	}

	// Local file or directory
	return speckeeper.GetAllSpecsFromPath(source)
}

// loadAllSpecsFromRemoteRepo fetches all specs from a GitHub or GitLab repository.
func loadAllSpecsFromRemoteRepo(ctx context.Context, repoURL, githubToken, gitlabToken string) (map[string]spectypes.Spec, error) {
	// Determine which token to use based on the provider
	var token string
	if specfetcher.IsGitHubURL(repoURL) {
		token = githubToken
	} else if specfetcher.IsGitLabURL(repoURL) {
		token = gitlabToken
	}

	specs, err := specfetcher.FetchAllSpecsFromRemote(ctx, repoURL, token)
	if err != nil {
		return nil, utils.FormatError("failed fetching specs from remote repository", err,
			utils.LogAttr("repo_url", repoURL))
	}

	return specs, nil
}
