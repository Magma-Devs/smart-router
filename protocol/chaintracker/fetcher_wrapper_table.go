package chaintracker

// FetcherWrapperKind names a family of IChainFetcherWrapper. A chain with no entry in
// fetcherWrapperKindByChainID uses DefaultChainTrackerFetcher, which polls through the
// ChainFetcher's own FetchLatestBlockNum and so records its poll observation there.
type FetcherWrapperKind string

// FetcherWrapperSVM is the Solana-family wrapper (SVMChainTracker). It polls via
// CustomMessage, which bypasses the ChainFetcher's FetchLatestBlockNum, and records the
// poll through the PollObserver hook instead.
const FetcherWrapperSVM FetcherWrapperKind = "svm"

// fetcherWrapperKindByChainID is the table newCustomChainTracker picks the wrapper from.
// Every chain that polls through anything but the default wrapper is listed here. The
// MAG-2221 canary (protocol/endpointstate, TestEndpointMonitor_RealPoll_EveryChain_RecordsPollFailure)
// ranges over these keys as well as over specs/, so a new wrapper is driven the moment its
// chains are added, and one that records a poll through neither path fails that test.
//
// TODO: a spec field naming the wrapper would replace this table.
var fetcherWrapperKindByChainID = map[string]FetcherWrapperKind{
	"SOLANA":  FetcherWrapperSVM,
	"SOLANAT": FetcherWrapperSVM,
	"KOII":    FetcherWrapperSVM,
	"KOIIT":   FetcherWrapperSVM,
}

// CustomFetcherWrapperChainIDs returns a copy of the wrapper table: every chain ID that
// uses a non-default fetcher wrapper, with the kind it uses. Tests range over it;
// production code reads the table directly.
func CustomFetcherWrapperChainIDs() map[string]FetcherWrapperKind {
	out := make(map[string]FetcherWrapperKind, len(fetcherWrapperKindByChainID))
	for chainID, kind := range fetcherWrapperKindByChainID {
		out[chainID] = kind
	}
	return out
}
