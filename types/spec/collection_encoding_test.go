package spec

import (
	"strings"
	"testing"
)

// CollectionData.Encoding declares the wire format of a collection's bodies.
// Empty means JSON, which is what every pre-existing chain relies on — a
// regression here would silently change behaviour for every REST chain.
func TestCollectionDataIsCBOR(t *testing.T) {
	tests := []struct {
		name     string
		encoding string
		want     bool
	}{
		{name: "empty encoding means JSON (every existing chain)", encoding: "", want: false},
		{name: "cbor is recognised", encoding: CollectionEncodingCBOR, want: true},
		{name: "literal cbor string is recognised", encoding: "cbor", want: true},
		{name: "unknown encoding is not treated as CBOR", encoding: "protobuf", want: false},
		{name: "case sensitive: CBOR is not cbor", encoding: "CBOR", want: false},
		// BlockParser.Encoding values describe block-hash representation, not a
		// body format. If one is ever pasted into CollectionData by mistake it must
		// not silently enable CBOR transcoding.
		{name: "base64 (a BlockParser encoding) is not a body format", encoding: EncodingBase64, want: false},
		{name: "hex (a BlockParser encoding) is not a body format", encoding: EncodingHex, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := CollectionData{ApiInterface: APIInterfaceRest, Encoding: tt.encoding}
			if got := cd.IsCBOR(); got != tt.want {
				t.Fatalf("IsCBOR() with encoding %q = %v, want %v", tt.encoding, got, tt.want)
			}
		})
	}
}

// The String() form feeds sorting and error messages; encoding must be visible
// there so a misconfigured collection is diagnosable from logs alone.
func TestCollectionDataStringIncludesEncoding(t *testing.T) {
	cd := CollectionData{ApiInterface: APIInterfaceRest, Type: "POST", Encoding: CollectionEncodingCBOR}
	got := cd.String()
	if want := "encoding:cbor"; !strings.Contains(got, want) {
		t.Fatalf("String() = %q, want it to contain %q", got, want)
	}
}
