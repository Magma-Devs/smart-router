package core

import (
	"bytes"
	"compress/gzip"
	"io"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	// CompressionThreshold is the minimum data size (in bytes) before gzip is attempted.
	CompressionThreshold = 1024
)

// Envelope is the stored form of a cached relay entry — the exact value the
// cache holds per (request hash, block) key. Finality determines the variant:
// a non-finalized entry retains the block Hash it was validated against, a
// finalized entry clears it (hash validation no longer applies), and the two
// variants may legitimately coexist under the same logical key in their
// respective namespaces.
type Envelope struct {
	Response         relaytypes.RelayReply
	Hash             []byte
	OptionalMetadata []relaytypes.Metadata
	SeenBlock        int64
	IsCompressed     bool
}

func (cv *Envelope) ToCacheReply() *relaytypes.CacheRelayReply {
	response := cv.Response
	if cv.IsCompressed && len(response.Data) > 0 {
		decompressed, err := decompressData(response.Data)
		if err != nil {
			utils.LavaFormatError("Failed to decompress cache data", err)
		} else {
			response.Data = decompressed
		}
	}
	return &relaytypes.CacheRelayReply{
		Reply:            &response,
		OptionalMetadata: cv.OptionalMetadata,
		SeenBlock:        cv.SeenBlock,
	}
}

func (cv *Envelope) Cost() int64 {
	return int64(len(cv.Response.Data))
}

// NewEnvelope builds the stored value for a SetRelay write. It mutates the
// passed response in place — the provider signature is zeroed (never served
// from cache) and Data is swapped for its gzip form when compression pays.
func NewEnvelope(response *relaytypes.RelayReply, hash []byte, finalized bool, optionalMetadata []relaytypes.Metadata, seenBlock int64) Envelope {
	response.Sig = []byte{}

	compressed, isCompressed, err := compressData(response.Data)
	if err != nil {
		utils.LavaFormatWarning("Failed to compress cache data, storing uncompressed", err)
	} else if isCompressed {
		response.Data = compressed
	}

	if !finalized {
		return Envelope{
			Response:         *response,
			Hash:             hash,
			OptionalMetadata: optionalMetadata,
			SeenBlock:        seenBlock,
			IsCompressed:     isCompressed,
		}
	}
	return Envelope{
		Response:         *response,
		Hash:             nil,
		OptionalMetadata: optionalMetadata,
		SeenBlock:        seenBlock,
		IsCompressed:     isCompressed,
	}
}

// compressData gzip-compresses data if it exceeds CompressionThreshold.
// Returns (compressed, true, nil) on success, (original, false, nil) if below threshold.
func compressData(data []byte) ([]byte, bool, error) {
	if len(data) < CompressionThreshold {
		return data, false, nil
	}
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return data, false, err
	}
	if err := w.Close(); err != nil {
		return data, false, err
	}
	compressed := buf.Bytes()
	// Only use compression if it actually saves space.
	if len(compressed) >= len(data) {
		return data, false, nil
	}
	return compressed, true, nil
}

// decompressData gunzips data compressed by compressData.
func decompressData(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
