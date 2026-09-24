package rpcsmartrouter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// solanaGetBlockReplyBytes is the size of the evening getBlock reply the Solana peak-load
// profile measured (MAG-3843): 5.9 MB of jsonParsed block, almost all of it transactions.
const solanaGetBlockReplyBytes = 5_900_000

// BenchmarkExtractSolanaContextSlot measures the Solana tip harvest on the reply shapes that
// matter: a multi-MB getBlock reply, which never carries result.context and so is a miss; a
// multi-MB reply whose result is an array (getProgramAccounts without withContext), answered from
// result's first byte; a small getBalance reply, which is a hit; and a multi-MB
// reply that does carry a context (getMultipleAccounts, getProgramAccounts withContext), where
// the error check still has to get past the whole value.
//
//	go test ./protocol/rpcsmartrouter/ -bench SolanaContextSlot -benchmem -run=^$ -count=3
func BenchmarkExtractSolanaContextSlot(b *testing.B) {
	for _, tc := range []struct {
		name   string
		reply  []byte
		wantOK bool
	}{
		{"getBlock_~5.9MB/miss", solanaGetBlockReply(solanaGetBlockReplyBytes), false},
		{"getProgramAccounts_~5.9MB/miss", solanaArrayResultReply(solanaGetBlockReplyBytes), false},
		{"getBalance/hit", []byte(`{"jsonrpc":"2.0","result":{"context":{"apiVersion":"2.2.7","slot":341197053},"value":1000000},"id":1}`), true},
		{"getMultipleAccounts_~5.9MB/hit", solanaLargeContextReply(solanaGetBlockReplyBytes), true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if !json.Valid(tc.reply) {
				b.Fatal("fixture is not valid JSON")
			}
			if _, ok := extractSolanaContextSlot(tc.reply); ok != tc.wantOK {
				b.Fatalf("extractSolanaContextSlot ok = %v, want %v", ok, tc.wantOK)
			}
			b.SetBytes(int64(len(tc.reply)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				extractSolanaContextSlot(tc.reply)
			}
		})
	}
}

// BenchmarkTipBlockFromRelay_SolanaGetBlock measures what a served getBlock relay costs the
// tip harvest end to end, through the same classification the relay path runs.
func BenchmarkTipBlockFromRelay_SolanaGetBlock(b *testing.B) {
	rpcss := ethTipServer(b, "SOLANA")
	chainMessage := &mockChainMessage{api: &spectypes.Api{Name: "getBlock"}, requestedBlock: 341197053}
	reply := &pairingtypes.RelayReply{Data: solanaGetBlockReply(solanaGetBlockReplyBytes)}
	if _, ok := rpcss.tipBlockFromRelay(chainMessage, reply); ok {
		b.Fatal("a getBlock reply must not yield a tip")
	}
	b.SetBytes(int64(len(reply.Data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rpcss.tipBlockFromRelay(chainMessage, reply)
	}
}

// solanaGetBlockReply builds a jsonParsed getBlock reply of at least targetBytes. The block's own
// members come in the order a Solana node sends them, alphabetical, so the transactions are last
// (checked against a mainnet getBlock reply on 2026-09-24). Like every real getBlock reply, the
// result is the block itself, with no result.context. Each transaction is a representative
// jsonParsed shape, not a byte-exact one.
func solanaGetBlockReply(targetBytes int) []byte {
	var buf bytes.Buffer
	buf.Grow(targetBytes + 4096)
	fmt.Fprintf(&buf, `{"jsonrpc":"2.0","result":{"blockHeight":319512345,"blockTime":1758700000,"blockhash":"%s","parentSlot":341197052,"previousBlockhash":"%s","rewards":[{"commission":null,"lamports":12345678,"postBalance":987654321,"pubkey":"%s","rewardType":"Fee"}],"transactions":[`,
		fakeBase58(2, 44), fakeBase58(1, 44), fakeBase58(3, 44))
	writeSolanaParsedTransactions(&buf, targetBytes)
	buf.WriteString(`]},"id":1}`)
	return buf.Bytes()
}

// solanaArrayResultReply builds a reply of at least targetBytes whose result is an array, as
// getProgramAccounts (without withContext), getSignaturesForAddress and getBlocks return.
func solanaArrayResultReply(targetBytes int) []byte {
	var buf bytes.Buffer
	buf.Grow(targetBytes + 4096)
	buf.WriteString(`{"jsonrpc":"2.0","result":[`)
	for i := 0; buf.Len() < targetBytes; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"account":{"data":["%s","base64"],"executable":false,"lamports":%d,"owner":"%s","rentEpoch":18446744073709551615,"space":165},"pubkey":"%s"}`,
			fakeBase58(uint64(i), 220), 2_039_280+i, fakeBase58(9, 44), fakeBase58(uint64(i)+7, 44))
	}
	buf.WriteString(`],"id":1}`)
	return buf.Bytes()
}

// solanaLargeContextReply builds a reply of at least targetBytes that does carry
// result.context: an RpcResponse whose value is large, as getMultipleAccounts or
// getProgramAccounts withContext return. The bulk reuses the transaction shape; only its size
// matters here.
func solanaLargeContextReply(targetBytes int) []byte {
	var buf bytes.Buffer
	buf.Grow(targetBytes + 4096)
	buf.WriteString(`{"jsonrpc":"2.0","result":{"context":{"apiVersion":"2.2.7","slot":341197053},"value":[`)
	writeSolanaParsedTransactions(&buf, targetBytes)
	buf.WriteString(`]},"id":1}`)
	return buf.Bytes()
}

// writeSolanaParsedTransactions appends comma-separated transactions until buf holds
// targetBytes.
func writeSolanaParsedTransactions(buf *bytes.Buffer, targetBytes int) {
	for i := 0; buf.Len() < targetBytes; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		writeSolanaParsedTransaction(buf, i)
	}
}

// writeSolanaParsedTransaction appends one jsonParsed transaction of about 2 KB: signatures,
// parsed account keys and instructions, and a meta with balances and log messages.
func writeSolanaParsedTransaction(buf *bytes.Buffer, i int) {
	seed := uint64(i)*16 + 100
	fmt.Fprintf(buf, `{"transaction":{"signatures":["%s"],"message":{"accountKeys":[`, fakeBase58(seed, 88))
	for k := uint64(0); k < 8; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, `{"pubkey":"%s","writable":%t,"signer":%t,"source":"transaction"}`, fakeBase58(seed+k+1, 44), k < 3, k == 0)
	}
	fmt.Fprintf(buf, `],"recentBlockhash":"%s","instructions":[`, fakeBase58(seed+9, 44))
	for k := uint64(0); k < 2; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, `{"program":"system","programId":"11111111111111111111111111111111","parsed":{"type":"transfer","info":{"source":"%s","destination":"%s","lamports":%d}},"stackHeight":null}`,
			fakeBase58(seed+10+k, 44), fakeBase58(seed+12+k, 44), 1_000_000+i)
	}
	buf.WriteString(`],"addressTableLookups":[]}},"meta":{"err":null,"status":{"Ok":null},"fee":5000,"preBalances":[`)
	for k := 0; k < 8; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, "%d", 2_000_000_000+i*8+k)
	}
	buf.WriteString(`],"postBalances":[`)
	for k := 0; k < 8; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, "%d", 1_999_995_000+i*8+k)
	}
	buf.WriteString(`],"innerInstructions":[],"logMessages":[`)
	for k := 0; k < 4; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`"Program 11111111111111111111111111111111 invoke [1]","Program 11111111111111111111111111111111 success"`)
	}
	buf.WriteString(`],"preTokenBalances":[],"postTokenBalances":[],"rewards":[],"computeUnitsConsumed":150},"version":0}`)
}

// fakeBase58 returns a deterministic string of n base58 characters derived from seed, the shape
// of a Solana public key (44) or signature (88).
func fakeBase58(seed uint64, n int) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	out := make([]byte, n)
	x := seed*6364136223846793005 + 1442695040888963407
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = alphabet[(x>>33)%uint64(len(alphabet))]
	}
	return string(out)
}
