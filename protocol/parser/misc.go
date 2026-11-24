package parser

import "github.com/magma-Devs/smart-router/utils"

// CapStringLen caps a log field at 250 bytes: a longer string keeps its first 150 and last 100
// bytes around "...Truncated...", cut on rune boundaries by utils.CutMiddle. utils.FormatLongString
// makes the same cut for a budget its caller picks, split two thirds and one third, with its own
// marker; the two stay separate functions because their output is what existing log lines show.
func CapStringLen(inp string) string {
	if len(inp) > 250 {
		return utils.CutMiddle(inp, 150, 100, "...Truncated...")
	}
	return inp
}
