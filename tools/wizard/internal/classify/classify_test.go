package classify
import (
	"testing"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/docs"
)
func TestDocsDriven(t *testing.T) {
	chains, _, err := catalog.Load("../../../specs")
	if err != nil { t.Skip("no catalog:", err) }
	b := Classify(chains)
	for _, f := range Order {
		if len(b[f]) == 0 { t.Errorf("family %s is empty", f) }
		t.Logf("%s: %d", f, len(b[f]))
	}
	cat := docs.Load()
	byIndex := map[string]catalog.Chain{}
	for _, c := range chains { byIndex[c.Index] = c }
	check := func(idx string, want Family) {
		if c, ok := byIndex[idx]; ok {
			if got := Of(c, cat, byIndex); got != want { t.Errorf("%s -> %s, want %s", idx, got, want) }
		}
	}
	check("ETH1", EVM); check("BTC", BTC); check("COSMOSHUB", Cosmos)
}
