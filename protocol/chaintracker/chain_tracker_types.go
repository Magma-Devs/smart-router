package chaintracker

// BlockStore holds a block number and its hash.
type BlockStore struct {
	Block int64  `json:"block,omitempty"`
	Hash  string `json:"hash,omitempty"`
}
