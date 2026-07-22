package spec

// SpecAddProposal represents a governance proposal to add or update one or
// more specs.  It mirrors the original protobuf-generated type that was
// removed together with the blockchain modules.
type SpecAddProposal struct {
	Specs []Spec `json:"specs"`
}

// SpecAddProposalJSON is the on-disk JSON envelope used by spec proposal files.
// It is the authoritative format understood by the spec fetcher and loader.
type SpecAddProposalJSON struct {
	Proposal SpecAddProposal `json:"proposal"`
}
