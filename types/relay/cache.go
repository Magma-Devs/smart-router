package relay

// BlockHashToHeight maps a block hash to its height, used in cache entries.
type BlockHashToHeight struct {
	Hash   string `json:"hash"`
	Height int64  `json:"height"`
}

func (b *BlockHashToHeight) GetHash() string {
	if b != nil {
		return b.Hash
	}
	return ""
}

func (b *BlockHashToHeight) GetHeight() int64 {
	if b != nil {
		return b.Height
	}
	return 0
}

// CacheRelayReply is the value stored in the relay cache for a given request.
type CacheRelayReply struct {
	Reply                 *RelayReply          `json:"reply"`
	OptionalMetadata      []Metadata           `json:"optional_metadata"`
	SeenBlock             int64                `json:"seen_block"`
	BlocksHashesToHeights []*BlockHashToHeight `json:"blocks_hashes_to_heights"`
	// IsNodeError marks entries whose payload is a cached node error rather than a
	// normal successful response. Absent on the wire from older cache servers, which
	// decodes to false — consumers that must not treat a node error as a success
	// (e.g. secondary-cache backfill) combine this with the legacy placeholder check
	// (common.CachedErrorGUIDPlaceholder).
	IsNodeError bool `json:"is_node_error"`
	// StatusCode is the upstream HTTP status recorded when the entry was written.
	// Zero means "unknown" (entry written by an older writer that did not record
	// status) and retains the legacy assume-success semantics; readers that gate on
	// status (e.g. the caching populator's 429/504/non-2xx checks) treat zero as
	// no-signal rather than as a failure.
	StatusCode int `json:"status_code"`
}

func (c *CacheRelayReply) GetReply() *RelayReply {
	if c != nil {
		return c.Reply
	}
	return nil
}

func (c *CacheRelayReply) GetOptionalMetadata() []Metadata {
	if c != nil {
		return c.OptionalMetadata
	}
	return nil
}

func (c *CacheRelayReply) GetSeenBlock() int64 {
	if c != nil {
		return c.SeenBlock
	}
	return 0
}

func (c *CacheRelayReply) GetBlocksHashesToHeights() []*BlockHashToHeight {
	if c != nil {
		return c.BlocksHashesToHeights
	}
	return nil
}

func (c *CacheRelayReply) GetIsNodeError() bool {
	if c != nil {
		return c.IsNodeError
	}
	return false
}

func (c *CacheRelayReply) GetStatusCode() int {
	if c != nil {
		return c.StatusCode
	}
	return 0
}

// RelayCacheGet is the request message sent to the relay cache service.
type RelayCacheGet struct {
	RequestHash           []byte               `json:"request_hash"`
	BlockHash             []byte               `json:"block_hash"`
	Finalized             bool                 `json:"finalized"`
	RequestedBlock        int64                `json:"requested_block"`
	SharedStateId         string               `json:"shared_state_id"`
	ChainId               string               `json:"chain_id"`
	SeenBlock             int64                `json:"seen_block"`
	BlocksHashesToHeights []*BlockHashToHeight `json:"blocks_hashes_to_heights"`
}

func (r *RelayCacheGet) GetRequestHash() []byte {
	if r != nil {
		return r.RequestHash
	}
	return nil
}

func (r *RelayCacheGet) GetBlockHash() []byte {
	if r != nil {
		return r.BlockHash
	}
	return nil
}

func (r *RelayCacheGet) GetFinalized() bool {
	if r != nil {
		return r.Finalized
	}
	return false
}

func (r *RelayCacheGet) GetRequestedBlock() int64 {
	if r != nil {
		return r.RequestedBlock
	}
	return 0
}

func (r *RelayCacheGet) GetSharedStateId() string {
	if r != nil {
		return r.SharedStateId
	}
	return ""
}

func (r *RelayCacheGet) GetChainId() string {
	if r != nil {
		return r.ChainId
	}
	return ""
}

func (r *RelayCacheGet) GetSeenBlock() int64 {
	if r != nil {
		return r.SeenBlock
	}
	return 0
}

func (r *RelayCacheGet) GetBlocksHashesToHeights() []*BlockHashToHeight {
	if r != nil {
		return r.BlocksHashesToHeights
	}
	return nil
}

// RelayCacheSet is the request message sent to the relay cache service to store an entry.
type RelayCacheSet struct {
	RequestHash           []byte               `json:"request_hash"`
	BlockHash             []byte               `json:"block_hash"`
	Response              *RelayReply          `json:"response"`
	Finalized             bool                 `json:"finalized"`
	OptionalMetadata      []Metadata           `json:"optional_metadata"`
	SharedStateId         string               `json:"shared_state_id"`
	RequestedBlock        int64                `json:"requested_block"`
	ChainId               string               `json:"chain_id"`
	SeenBlock             int64                `json:"seen_block"`
	AverageBlockTime      int64                `json:"average_block_time"`
	IsNodeError           bool                 `json:"is_node_error"`
	BlocksHashesToHeights []*BlockHashToHeight `json:"blocks_hashes_to_heights"`
	// StatusCode is the upstream HTTP status of the response being cached; zero when
	// the writer does not know it (legacy writers, non-HTTP flows). See
	// CacheRelayReply.StatusCode for reader semantics.
	StatusCode int `json:"status_code"`
}

func (r *RelayCacheSet) GetRequestHash() []byte {
	if r != nil {
		return r.RequestHash
	}
	return nil
}

func (r *RelayCacheSet) GetBlockHash() []byte {
	if r != nil {
		return r.BlockHash
	}
	return nil
}

func (r *RelayCacheSet) GetResponse() *RelayReply {
	if r != nil {
		return r.Response
	}
	return nil
}

func (r *RelayCacheSet) GetFinalized() bool {
	if r != nil {
		return r.Finalized
	}
	return false
}

func (r *RelayCacheSet) GetOptionalMetadata() []Metadata {
	if r != nil {
		return r.OptionalMetadata
	}
	return nil
}

func (r *RelayCacheSet) GetSharedStateId() string {
	if r != nil {
		return r.SharedStateId
	}
	return ""
}

func (r *RelayCacheSet) GetRequestedBlock() int64 {
	if r != nil {
		return r.RequestedBlock
	}
	return 0
}

func (r *RelayCacheSet) GetChainId() string {
	if r != nil {
		return r.ChainId
	}
	return ""
}

func (r *RelayCacheSet) GetSeenBlock() int64 {
	if r != nil {
		return r.SeenBlock
	}
	return 0
}

func (r *RelayCacheSet) GetAverageBlockTime() int64 {
	if r != nil {
		return r.AverageBlockTime
	}
	return 0
}

func (r *RelayCacheSet) GetIsNodeError() bool {
	if r != nil {
		return r.IsNodeError
	}
	return false
}

func (r *RelayCacheSet) GetStatusCode() int {
	if r != nil {
		return r.StatusCode
	}
	return 0
}

func (r *RelayCacheSet) GetBlocksHashesToHeights() []*BlockHashToHeight {
	if r != nil {
		return r.BlocksHashesToHeights
	}
	return nil
}

// CacheUsage reports hit and miss statistics for the relay cache.
type CacheUsage struct{}

// CacheHash is a composite key used when computing the cache lookup hash.
type CacheHash struct {
	Request *RelayPrivateData `json:"request"`
	ChainId string            `json:"chain_id"`
}

func (c *CacheHash) GetRequest() *RelayPrivateData {
	if c != nil {
		return c.Request
	}
	return nil
}

func (c *CacheHash) GetChainId() string {
	if c != nil {
		return c.ChainId
	}
	return ""
}

// EndpointObservationSet publishes one pod's successful poll of one upstream endpoint to the
// cache backend so peer pods can borrow it (the fleet tracker gate, MAG-2981). EndpointId is an
// opaque, stable digest of the endpoint URL computed by the router — the raw URL carries the
// provider API key and never crosses this wire. PodId names the publishing router instance so
// a reader can ignore its own observation (a pod's own poll must never suppress its next poll).
// Ttl bounds how long the server keeps the observation; the server stamps its own receipt time
// and never trusts a writer clock.
type EndpointObservationSet struct {
	ChainId      string `json:"chain_id"`
	ApiInterface string `json:"api_interface"`
	EndpointId   string `json:"endpoint_id"`
	PodId        string `json:"pod_id"`
	Block        int64  `json:"block"`
	TtlMs        int64  `json:"ttl_ms"`
}

// EndpointObservationGet asks the cache backend for the freshest peer observation of one endpoint.
type EndpointObservationGet struct {
	ChainId      string `json:"chain_id"`
	ApiInterface string `json:"api_interface"`
	EndpointId   string `json:"endpoint_id"`
}

// EndpointObservationReply carries a peer observation back to a router. Found=false means no
// live observation exists (never published, or expired). AgeMs is measured on the SERVER clock
// between the stored receipt stamp and this read, so two pods with skewed clocks still agree on
// how old the observation is.
type EndpointObservationReply struct {
	Found bool   `json:"found"`
	Block int64  `json:"block"`
	AgeMs int64  `json:"age_ms"`
	PodId string `json:"pod_id"`
}

func (r *EndpointObservationReply) GetPodId() string {
	if r != nil {
		return r.PodId
	}
	return ""
}

func (r *EndpointObservationReply) GetFound() bool {
	if r != nil {
		return r.Found
	}
	return false
}

func (r *EndpointObservationReply) GetBlock() int64 {
	if r != nil {
		return r.Block
	}
	return 0
}

func (r *EndpointObservationReply) GetAgeMs() int64 {
	if r != nil {
		return r.AgeMs
	}
	return 0
}

// StickySessionSet claims an upstream for one sticky session id so every router replica
// resolves that id to the same node (cross-pod stickiness). The claim is FIRST-WRITER-WINS:
// the server refuses to overwrite a live entry and replies with whatever is actually stored,
// so a pod that lost the race learns the winner from its own write and can adopt it without a
// second round trip. Last-write-wins was rejected deliberately — pods judge node health
// locally, so pods with different health views would otherwise overwrite each other in a loop.
//
// StickyId is an opaque digest of the client's session id computed by the router. The raw id is
// customer-supplied and may carry a user identifier, and the fleet only needs a stable key to
// agree on, so the plaintext never crosses this wire.
//
// Provider is the provider NAME — the router's routing identity for an upstream, validated
// unique per chain + api interface and identical on every pod running the same config. It is
// never a node URL: those carry credentials.
//
// Epoch is the router epoch the claim was made in. Epochs are derived from wall clock, so every
// pod computes the same number without coordinating, which lets a reader apply the same
// staleness rule the pod-local pin table applies. TtlMs is only a backstop so a claim cannot
// outlive its usefulness if nobody reads it; the epoch is what decides validity.
type StickySessionSet struct {
	ChainId      string `json:"chain_id"`
	ApiInterface string `json:"api_interface"`
	StickyId     string `json:"sticky_id"`
	Provider     string `json:"provider"`
	Epoch        uint64 `json:"epoch"`
	TtlMs        int64  `json:"ttl_ms"`
}

// StickySessionGet asks the cache backend which upstream the fleet has pinned for one sticky
// session id.
type StickySessionGet struct {
	ChainId      string `json:"chain_id"`
	ApiInterface string `json:"api_interface"`
	StickyId     string `json:"sticky_id"`
}

// StickySessionReply carries the EFFECTIVE pin — the entry the store actually holds after the
// call. On a read, Found=false means no live claim exists (never made, or expired). On a write
// it is never a miss: it is either the claim just accepted or the live claim that beat it, which
// is what makes a lost race resolvable inside the request that caused it.
type StickySessionReply struct {
	Found    bool   `json:"found"`
	Provider string `json:"provider"`
	Epoch    uint64 `json:"epoch"`
}

func (r *StickySessionReply) GetFound() bool {
	if r != nil {
		return r.Found
	}
	return false
}

func (r *StickySessionReply) GetProvider() string {
	if r != nil {
		return r.Provider
	}
	return ""
}

func (r *StickySessionReply) GetEpoch() uint64 {
	if r != nil {
		return r.Epoch
	}
	return 0
}
