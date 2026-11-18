package common

// IsSolanaFamily returns true if the chain ID belongs to the Solana/SVM family.
// These chains use slot-based block numbering where slots can be skipped or
// not yet propagated, requiring special handling in block fetching.
//
// Delegates to the chain families in error_registry.go, where a new SVM chain
// takes the Solana family from its spec's import of SOLANA — no parallel switch
// to keep in sync.
func IsSolanaFamily(chainID string) bool {
	family, ok := GetChainFamily(chainID)
	return ok && family == ChainFamilySolana
}
