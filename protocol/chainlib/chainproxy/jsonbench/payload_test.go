package jsonbench

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Realistic debug_traceTransaction (default opcode tracer) response generator.
// The default tracer is the heaviest JSON an EVM node emits: one structLog per
// executed opcode, each carrying the full stack, grown memory, and touched
// storage. A heavy Base tx produces tens of thousands of these — multi-MB to
// hundreds-of-MB bodies — which is exactly the payload class this PR's JSON
// paths are meant to survive.
//
// The bytes are shaped to match a real node's output (member order, 0x-hex
// words, "0x" prefixes) so a marshaler sees the same work it would on the wire;
// only the values are deterministic from the opcode index.

type structLog struct {
	Pc      uint64            `json:"pc"`
	Op      string            `json:"op"`
	Gas     uint64            `json:"gas"`
	GasCost uint64            `json:"gasCost"`
	Depth   int               `json:"depth"`
	Stack   []string          `json:"stack"`
	Memory  []string          `json:"memory"`
	Storage map[string]string `json:"storage,omitempty"`
}

type traceResult struct {
	Gas         uint64      `json:"gas"`
	Failed      bool        `json:"failed"`
	ReturnValue string      `json:"returnValue"`
	StructLogs  []structLog `json:"structLogs"`
}

type traceEnvelope struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  traceResult `json:"result"`
}

var opcodes = []string{"PUSH1", "PUSH32", "MSTORE", "SLOAD", "SSTORE", "DUP2", "SWAP1", "ADD", "MUL", "CALL", "JUMPI", "SHA3", "CALLDATALOAD", "RETURN"}

func word(seed uint64) string {
	var b strings.Builder
	b.WriteString("0x")
	for i := 0; i < 8; i++ {
		fmt.Fprintf(&b, "%08x", uint32(seed*2654435761+uint64(i)*40503))
	}
	return b.String()
}

// buildTrace returns a typed trace value with n opcode steps.
func buildTrace(n int) traceEnvelope {
	logs := make([]structLog, n)
	for i := range logs {
		stackDepth := 4 + i%12
		stack := make([]string, stackDepth)
		for s := range stack {
			stack[s] = word(uint64(i*31 + s))
		}
		memWords := 8 + (i%24)*2
		memory := make([]string, memWords)
		for m := range memory {
			memory[m] = word(uint64(i*17 + m*7))
		}
		var storage map[string]string
		if i%6 == 0 {
			storage = map[string]string{word(uint64(i)): word(uint64(i * 3))}
		}
		logs[i] = structLog{
			Pc:      uint64(i),
			Op:      opcodes[i%len(opcodes)],
			Gas:     uint64(3_000_000 - i*3),
			GasCost: uint64(3 + i%40),
			Depth:   1 + i%3,
			Stack:   stack,
			Memory:  memory,
			Storage: storage,
		}
	}
	return traceEnvelope{
		Jsonrpc: "2.0",
		ID:      1,
		Result:  traceResult{Gas: 2_998_877, Failed: false, ReturnValue: "", StructLogs: logs},
	}
}

// sizeFor picks the opcode count for a named size, honoring JSONBENCH_HUGE for
// the largest case (kept behind an env so CI stays fast, mirroring the existing
// compression benchmark's hugeBenchEnvVar convention).
func sizeFor(name string) (int, bool) {
	switch name {
	case "small":
		return 500, true
	case "heavy":
		return 4000, true
	case "huge":
		return 40000, os.Getenv("JSONBENCH_HUGE") != ""
	default:
		return 0, false
	}
}

func mustAtoiEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
