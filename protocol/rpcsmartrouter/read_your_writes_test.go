package rpcsmartrouter

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/require"
)

// Signed with go-ethereum's own typed signers (core/types, v1.17.5) from one throwaway key, so
// the decoder here is checked against an independent implementation rather than against itself.
const rywTestSender = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23"

var rywGethVectors = []struct {
	name string
	raw  string
	hash string
}{
	{"legacy, EIP-155 on Sepolia", "0xf86707847735940082520894111111111111111111111111111111111111111101808401546d72a066ba29e2c438955acd839b433724a8fc1e5048aa61d14abaf0d9618371b14555a0559ef7e5780f96fb3f4b453687d1d9705980f6e803e6bfc9b9fb5177692c847d", "0x43c820df3d0b5062002c50f25a9766b0a60b418a2bbd96d835e5c372f7abe0ab"},
	{"legacy, EIP-155 on a chain id wider than v's byte (Celo Sepolia)", "0xf868038505d21dba00825208941111111111111111111111111111111111111111058084015408bba0badaa7a33bd7c7d314d23f7bafe06433e7757637b880eeaddd87d15d70d29964a03e0f2b12e233076dd1e2d55f7e850c5ae9cc9096b00ccf9c809a528f3a954127", "0x4d5385d83944098bee8d473fbeb21cebf2375dc1e8feab4317e4a2b74ee9564f"},
	{"legacy, no chain id", "0xf86380843b9aca0082520894111111111111111111111111111111111111111180801ba0c596ea61f924b3fdf7595311062b5bbd97511004cf3bccc90566732a5dab3483a01833964c0ec7802a06ba34c36d8045911c8d046b3a5238391c0e2d04a7553e4e", "0x5158c605b3c49523d322693f400294a03a1e1b7e5b6cca99779ed1eee214afa0"},
	{"EIP-2930 access list", "0x01f8a183aa36a70b84b2d05e008275309411111111111111111111111111111111111111110280f838f7941111111111111111111111111111111111111111e1a0010000000000000000000000000000000000000000000000000000000000000001a0d6f69f4dfff5d430e4d1115e9fed236f635c43d2f62e5a43bbd60a431f2e0323a04467a5eeddf405d5188532c03e14d17b5c1188102b269c8b20a7a968af50e336", "0xfc855a8147866fd16074ac241bbc464b73a4f18f19bb82b12856f0447fc58352"},
	{"EIP-1559 dynamic fee", "0x02f87083aa36a72a843b9aca008506fc23ac008252089411111111111111111111111111111111111111110382deadc001a0935a33c493797cd521e879f863d1be180fa3544e98055951c4d4ba99d9ebeb02a05df0cc262d953afd2870b22742c671444f3cddb2b2a26389957c3473291969c5", "0x2620d49b3fa2ff073b49bce2b5f0382521afd075691e695251929cd67be4f460"},
	{"EIP-4844 blob, canonical form", "0x03f89583aa36a705843b9aca008506fc23ac008252089411111111111111111111111111111111111111118080c0843b9aca00e1a0010200000000000000000000000000000000000000000000000000000000000080a04d402af4df3d062254b8dfdba59c7f4e5459d91bb7f3bbf157bc43e113133403a004de13d4d1d0ac351167621e0748f98f05bd9d16b1bcf3a4d5962eaea9bd5db4", "0xc1d29d1322eb072f0c16fc87b0616ea76c23022a175c9202a38d69f4756b9828"},
	{"EIP-7702 set code", "0x04f88d83aa36a709843b9aca008506fc23ac0082c3509411111111111111111111111111111111111111118080c0dedd83aa36a79411111111111111111111111111111111111111110a01010101a0f0d9c5ccbf67060a736ba57d76a542960e9e6c0a3300e5c421f4689e7adf8450a00d4b6156b60ab671b503139ffdc22a7aad535f33ac9d1454216b819bf20675ac", "0x7330b187b0eb52f787df55c91c8624c4bf8b8725d964d6c24746a4433996f6ce"},
}

// A fee-currency transaction from Celo mainnet (block 0x4b3a6fb), rebuilt from
// eth_getTransactionByHash; its keccak is the on-chain hash, so the bytes are exact. go-ethereum
// cannot decode this type at all — it is the case the envelope rule exists for.
const (
	rywCeloCIP64Raw    = "0x7bf8ca82a4ec820237840ebad408850e0a1217a0830391539448065fbbe25f71c9282ddf5e1cd6d6a887483d5e80b844a9059cbb0000000000000000000000009b5d6fabea612d29525ce4e56ee4c531686a278f00000000000000000000000000000000000000000000000000000000002bea30c0940e2a3e05bc9a16f5292a6170456a710cb89c6f7201a0caa90325bc9b48e6163f2a4656cfa3d8e5c69394edc25dcd1c7ea6d79ba7b1cca044e3efc2155dcd8d8ac062f881086ef50dc4c4157c54bfe365856af4f0f562aa"
	rywCeloCIP64Sender = "0x3ccc136177dfd4e0aaa7d643fa13db62997c957b"
	rywCeloCIP64Hash   = "0x9c5baec48dadcc23820dcd8071fba2195b89a514970720c460c99976f84491e9"
)

func TestEvmTxSenderAndHash_KnownTypes(t *testing.T) {
	for tcIndex, tc := range rywGethVectors {
		t.Run(tc.name, func(t *testing.T) {
			sender, hash, ok := evmTxSenderAndHash(hexutil.MustDecode(tc.raw))
			require.True(t, ok, "tc #%d", tcIndex)
			require.Equal(t, rywTestSender, sender, "tc #%d", tcIndex)
			require.Equal(t, tc.hash, hash, "tc #%d", tcIndex)
		})
	}

	t.Run("a chain type go-ethereum does not know: Celo fee currency", func(t *testing.T) {
		sender, hash, ok := evmTxSenderAndHash(hexutil.MustDecode(rywCeloCIP64Raw))
		require.True(t, ok)
		require.Equal(t, rywCeloCIP64Sender, sender)
		require.Equal(t, rywCeloCIP64Hash, hash)
	})

	t.Run("a blob transaction in the network form eth_sendRawTransaction carries", func(t *testing.T) {
		canonical := hexutil.MustDecode(rywGethVectors[5].raw)
		require.Equal(t, byte(0x03), canonical[0], "precondition: vector 5 is the blob transaction")
		// [transaction, blobs, commitments, proofs]. The sidecar's contents are opaque to the
		// decoder, so any bytes do; the hash must still be the canonical one, without them.
		wrapped, err := rlp.EncodeToBytes([]interface{}{
			rlp.RawValue(canonical[1:]),
			[][]byte{make([]byte, 64)},
			[][]byte{make([]byte, 48)},
			[][]byte{make([]byte, 48)},
		})
		require.NoError(t, err)
		sender, hash, ok := evmTxSenderAndHash(append([]byte{0x03}, wrapped...))
		require.True(t, ok)
		require.Equal(t, rywTestSender, sender)
		require.Equal(t, rywGethVectors[5].hash, hash)
	})
}

func TestEvmTxSenderAndHash_RejectsWhatItCannotRead(t *testing.T) {
	dynamicFee := hexutil.MustDecode(rywGethVectors[4].raw)
	var items []rlp.RawValue
	require.NoError(t, rlp.DecodeBytes(dynamicFee[1:], &items))

	// The same transaction with its signature flipped to the high-s twin. It recovers a
	// different key, and a node refuses it, so it must not pin anyone.
	malleable := func() []byte {
		secp256k1N := crypto.S256().Params().N
		var s big.Int
		require.NoError(t, rlp.DecodeBytes(items[len(items)-1], &s))
		highS, err := rlp.EncodeToBytes(new(big.Int).Sub(secp256k1N, &s))
		require.NoError(t, err)
		flipped := append([]rlp.RawValue(nil), items...)
		flipped[len(flipped)-3] = rlp.RawValue{0x80} // yParity 1 -> 0
		flipped[len(flipped)-1] = highS
		body, err := rlp.EncodeToBytes(flipped)
		require.NoError(t, err)
		return append([]byte{0x02}, body...)
	}()

	legacyWithV := func(v uint64) []byte {
		legacy := hexutil.MustDecode(rywGethVectors[0].raw)
		var fields []rlp.RawValue
		require.NoError(t, rlp.DecodeBytes(legacy, &fields))
		encodedV, err := rlp.EncodeToBytes(v)
		require.NoError(t, err)
		fields[6] = encodedV
		out, err := rlp.EncodeToBytes(fields)
		require.NoError(t, err)
		return out
	}

	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"truncated typed envelope", dynamicFee[:20]},
		{"not RLP at all", []byte("not a transaction")},
		{"a type byte in the RLP string range", append([]byte{0x90}, dynamicFee[1:]...)},
		{"typed list too short to carry a signature", append([]byte{0x02}, 0xc3, 0x01, 0x02, 0x03)},
		{"legacy list of the wrong length", dynamicFee[1:]},
		{"legacy v that is neither 27/28 nor EIP-155", legacyWithV(30)},
		{"high-s signature", malleable},
	}
	for tcIndex, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sender, _, ok := evmTxSenderAndHash(tc.raw)
			require.False(t, ok, "tc #%d recovered %q", tcIndex, sender)
		})
	}
}

func TestReadYourWrites_OffIsANoOp(t *testing.T) {
	require.Nil(t, newReadYourWrites(0))
	require.Nil(t, newReadYourWrites(-time.Second))

	var off *readYourWrites
	chainParser := ethJsonRPCParser(t)
	write := ethProtocolMessage(t, chainParser, rywSendRawTx(rywGethVectors[4].raw), 0)
	require.NotPanics(t, func() { off.recordWrite(write, "vendor-a", rywReply(rywGethVectors[4].hash)) })
	require.Empty(t, off.pinFor(ethProtocolMessage(t, chainParser, rywNonceRead(rywTestSender, "pending"), 0)))
}

func TestReadYourWrites_WriteThenRead(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	now := time.Unix(1_700_000_000, 0)
	pins := newReadYourWrites(30 * time.Second)
	pins.now = func() time.Time { return now }

	dynamicFee := rywGethVectors[4]
	pins.recordWrite(ethProtocolMessage(t, chainParser, rywSendRawTx(dynamicFee.raw), 0), "vendor-a", rywReply(dynamicFee.hash))

	read := func(body string) string {
		return pins.pinFor(ethProtocolMessage(t, chainParser, body, 0))
	}
	checksummedSender := "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23"
	upperHash := "0x" + strings.ToUpper(dynamicFee.hash[2:])

	require.Equal(t, "vendor-a", read(rywNonceRead(checksummedSender, "pending")), "the sender's pending nonce, in any case")
	require.Equal(t, "vendor-a", read(rywByHash("eth_getTransactionByHash", dynamicFee.hash)))
	require.Equal(t, "vendor-a", read(rywByHash("eth_getTransactionReceipt", upperHash)))

	require.Empty(t, read(rywNonceRead(checksummedSender, "latest")), "a latest nonce moves with blocks, not with a mempool")
	require.Empty(t, read(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["`+checksummedSender+`"]}`), "no tag parses as latest")
	require.Empty(t, read(rywNonceRead("0x1111111111111111111111111111111111111111", "pending")), "another account")
	require.Empty(t, read(rywByHash("eth_getTransactionByHash", rywGethVectors[0].hash)), "another transaction")
	require.Empty(t, read(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+dynamicFee.hash+`",false]}`), "a block hash names no transaction")
	require.Empty(t, read(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["`+checksummedSender+`","pending"]}`))

	// A later write from the same sender moves the pin: the newest acceptance is the one the
	// next nonce read must see.
	now = now.Add(10 * time.Second)
	pins.recordWrite(ethProtocolMessage(t, chainParser, rywSendRawTx(rywGethVectors[0].raw), 0), "vendor-b", rywReply(rywGethVectors[0].hash))
	require.Equal(t, "vendor-b", read(rywNonceRead(checksummedSender, "pending")))
	require.Equal(t, "vendor-a", read(rywByHash("eth_getTransactionByHash", dynamicFee.hash)), "each hash keeps its own acceptor")

	// The window is counted from each pin's own write.
	now = now.Add(21 * time.Second)
	require.Empty(t, read(rywByHash("eth_getTransactionByHash", dynamicFee.hash)), "31s after its write")
	require.Equal(t, "vendor-b", read(rywNonceRead(checksummedSender, "pending")), "21s after its write")
	now = now.Add(10 * time.Second)
	require.Empty(t, read(rywNonceRead(checksummedSender, "pending")))
}

func TestReadYourWrites_WhatAWriteMustBeToPin(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	nonceRead := ethProtocolMessage(t, chainParser, rywNonceRead(rywTestSender, "pending"), 0)
	dynamicFee := rywGethVectors[4]

	t.Run("a raw transaction the rule cannot read still pins its hash from the reply", func(t *testing.T) {
		pins := newReadYourWrites(time.Minute)
		pins.recordWrite(ethProtocolMessage(t, chainParser, rywSendRawTx("0x02f8"), 0), "vendor-a", rywReply(dynamicFee.hash))
		require.Equal(t, "vendor-a", pins.pinFor(ethProtocolMessage(t, chainParser, rywByHash("eth_getTransactionByHash", dynamicFee.hash), 0)))
		require.Empty(t, pins.pinFor(nonceRead))
	})
	t.Run("a reply without a hash still pins what the transaction names", func(t *testing.T) {
		pins := newReadYourWrites(time.Minute)
		pins.recordWrite(ethProtocolMessage(t, chainParser, rywSendRawTx(dynamicFee.raw), 0), "vendor-a", []byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
		require.Equal(t, "vendor-a", pins.pinFor(nonceRead))
		require.Equal(t, "vendor-a", pins.pinFor(ethProtocolMessage(t, chainParser, rywByHash("eth_getTransactionByHash", dynamicFee.hash), 0)))
	})
	t.Run("only eth_sendRawTransaction pins", func(t *testing.T) {
		pins := newReadYourWrites(time.Minute)
		pins.recordWrite(ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x1111111111111111111111111111111111111111","data":"`+dynamicFee.raw+`"},"latest"]}`, 0), "vendor-a", rywReply(dynamicFee.hash))
		require.Empty(t, pins.pinFor(nonceRead))
	})
	t.Run("a write nobody is named as having accepted pins nothing", func(t *testing.T) {
		pins := newReadYourWrites(time.Minute)
		pins.recordWrite(ethProtocolMessage(t, chainParser, rywSendRawTx(dynamicFee.raw), 0), "", rywReply(dynamicFee.hash))
		require.Empty(t, pins.pinFor(nonceRead))
	})
}

func TestReadYourWrites_TableIsBounded(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	pins := newReadYourWrites(time.Minute)
	pins.now = func() time.Time { return now }

	for i := 0; i < rywMaxPins; i++ {
		pins.pin([]string{fmt.Sprintf("tx:%d", i)}, "vendor-a")
	}
	require.Len(t, pins.pins, rywMaxPins)

	pins.pin([]string{"tx:over"}, "vendor-a")
	_, admitted := pins.pins["tx:over"]
	require.False(t, admitted, "a full table drops the new pin rather than growing")

	pins.pin([]string{"tx:0"}, "vendor-b")
	require.Equal(t, "vendor-b", pins.pins["tx:0"].provider, "a full table still refreshes a pin it holds")

	// Once the window has passed, the next write sweeps what expired and is admitted.
	now = now.Add(2 * time.Minute)
	pins.pin([]string{"tx:after"}, "vendor-a")
	require.Len(t, pins.pins, 1)
	require.Equal(t, "vendor-a", pins.pins["tx:after"].provider)
}

func rywSendRawTx(raw string) string {
	return `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["` + raw + `"]}`
}

func rywNonceRead(address, tag string) string {
	return `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["` + address + `","` + tag + `"]}`
}

func rywByHash(method, hash string) string {
	return `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":["` + hash + `"]}`
}

func rywReply(hash string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"result":"` + hash + `"}`)
}
