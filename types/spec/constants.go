package spec

const (
	NOT_APPLICABLE  int64 = -1
	LATEST_BLOCK    int64 = -2
	EARLIEST_BLOCK  int64 = -3
	PENDING_BLOCK   int64 = -4
	SAFE_BLOCK      int64 = -5
	FINALIZED_BLOCK int64 = -6

	DEFAULT_PARSED_RESULT_INDEX = 0

	APIInterfaceJsonRPC       = "jsonrpc"
	APIInterfaceTendermintRPC = "tendermintrpc"
	APIInterfaceRest          = "rest"
	APIInterfaceGrpc          = "grpc"

	ParserArgLatest = "latest"

	// EncodingBase64/EncodingHex belong to BlockParser.Encoding and describe how
	// an extracted block HASH is represented — NOT the wire format of a body.
	EncodingBase64 = "base64"
	EncodingHex    = "hex"

	// CollectionEncodingCBOR is a CollectionData.Encoding value declaring that
	// this collection's bodies are CBOR, not JSON. An empty Encoding means JSON.
	// Used by IC-based chains, whose HTTP interface is CBOR end to end.
	CollectionEncodingCBOR = "cbor"
)

// IsFinalizedBlock returns true when the requested block is old enough to be
// considered finalized relative to the latest known block and the chain's
// finalization criteria (number of confirmations required).
func IsFinalizedBlock(requestedBlock, latestBlock, finalizationCriteria int64) bool {
	switch requestedBlock {
	case NOT_APPLICABLE:
		return false
	default:
		if requestedBlock < 0 {
			return false
		}
		if requestedBlock <= latestBlock-finalizationCriteria {
			return true
		}
	}
	return false
}
