package chainlib

import (
	"github.com/magma-Devs/smart-router/protocol/parser"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// requestedBlockFold folds the blocks of a request's elements — a JSON-RPC or Tendermint batch's
// calls, a GraphQL operation's root fields — into the request's latest and earliest requested
// blocks, by CompareRequestedBlockInBatch and CombineEarliestRequestedBlock.
//
// An element whose api names no block (apiNamesNoBlock) takes no part (MAG-4381). Its block parses
// to NOT_APPLICABLE, and folded in, that value read as "an element nobody could parse", which makes
// the whole request unjudgeable: `{ epoch { epochId } chainIdentifier }` and a batch of eth_chainId
// with eth_blockNumber were NOT_APPLICABLE, so they were never cached, and a pinned block beside a
// block-less element never reached the archive rule. So "latest" beside "none" stays latest, a
// pinned block stays that block, and a request none of whose elements names a block has none.
//
// A block that failed to parse comes out as NOT_APPLICABLE too, and that one still folds in and
// keeps the request unjudgeable: a pinned read whose block could not be read must not be cached or
// routed as a latest read. The two share the value, which is why the fold asks the element's api
// instead of reading the value alone.
type requestedBlockFold struct {
	latest, earliest int64
	folded           bool
}

// add folds one element in. parsedBlock is the element's requested block, and elementEarliest what
// it adds to the earliest block: its own block, or EARLIEST_BLOCK for a batch element at block 0.
// parsedInput carries a range lower bound the element's parsers read, if any. namesNoBlock is true
// when the element's api names no block and nothing else, such as a header override, gave it one.
func (fold *requestedBlockFold) add(parsedBlock, elementEarliest int64, parsedInput *parser.ParsedInput, namesNoBlock bool) {
	if namesNoBlock && parsedBlock == spectypes.NOT_APPLICABLE {
		return
	}
	if !fold.folded {
		fold.latest = parsedBlock
		fold.earliest = foldParsedEarliest(elementEarliest, parsedInput)
		fold.folded = true
		return
	}
	fold.latest, _ = CompareRequestedBlockInBatch(fold.latest, fold.earliest, parsedBlock)
	fold.earliest = foldParsedEarliest(CombineEarliestRequestedBlock(fold.earliest, elementEarliest), parsedInput)
}

// blocks returns the request's latest and earliest requested blocks: NOT_APPLICABLE for both when no
// element names a block, which is what a lone block-less element always parsed to.
func (fold *requestedBlockFold) blocks() (latest, earliest int64) {
	if !fold.folded {
		return spectypes.NOT_APPLICABLE, spectypes.NOT_APPLICABLE
	}
	return fold.latest, fold.earliest
}

// apiNamesNoBlock reports whether api has no way to name a block: no block parser
// (PARSER_FUNC_EMPTY), no default block, and no generic parser that reads a block, a range bound, a
// block hash or a default. Such an api parses to NOT_APPLICABLE because it has no block — Sui's
// chainIdentifier, eth_chainId — not because a block failed to parse.
func apiNamesNoBlock(api *spectypes.Api) bool {
	if api == nil || api.BlockParsing.ParserFunc != spectypes.PARSER_FUNC_EMPTY || api.BlockParsing.DefaultValue != "" {
		return false
	}
	for _, genericParser := range api.Parsers {
		switch genericParser.ParseType {
		case spectypes.PARSER_TYPE_BLOCK_LATEST, spectypes.PARSER_TYPE_BLOCK_EARLIEST, spectypes.PARSER_TYPE_BLOCK_HASH, spectypes.PARSER_TYPE_DEFAULT_VALUE:
			return false
		}
	}
	return true
}
