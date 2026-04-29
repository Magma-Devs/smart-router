//go:build !enterprise

package rpcsmartrouter

// init is intentionally empty in the community build. Without an
// enterpriseConfigFactory registered, IsEnterpriseBuild() returns false and
// activeConfig stays as communityConfig — the default the community binary
// must ship with.
//
// This file's sole job is to be a clear hook (and tag-anchor) that documents
// the community-build init contract. Sprint 3+ may add observability here.
func init() {}
