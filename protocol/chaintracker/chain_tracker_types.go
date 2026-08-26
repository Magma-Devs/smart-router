package chaintracker

import "encoding/json"

// BlockStore holds a block number and its hash.
type BlockStore struct {
	Block int64  `json:"block,omitempty"`
	Hash  string `json:"hash,omitempty"`
}

func (m *BlockStore) GetBlock() int64 {
	if m != nil {
		return m.Block
	}
	return 0
}

func (m *BlockStore) GetHash() string {
	if m != nil {
		return m.Hash
	}
	return ""
}

func (m *BlockStore) Reset()        { *m = BlockStore{} }
func (m *BlockStore) ProtoMessage() {}
func (m *BlockStore) String() string {
	b, _ := json.Marshal(m)
	return string(b)
}

func (m *BlockStore) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *BlockStore) Unmarshal(b []byte) error { return json.Unmarshal(b, m) }
