package chaintracker

// BlocksQueueSnapshot returns a copy of a ChainTracker's block-hash queue, for the external
// tests that check what fork detection compares against. Any other tracker yields nil.
func BlocksQueueSnapshot(t IChainTracker) []BlockStore {
	cs, ok := t.(*ChainTracker)
	if !ok {
		return nil
	}
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	return append([]BlockStore(nil), cs.blocksQueue...)
}
