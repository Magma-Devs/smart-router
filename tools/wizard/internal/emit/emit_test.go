package emit

import "testing"

func TestRender(t *testing.T) {
	c := &Config{
		Metrics: "0.0.0.0:7779", Cache: true,
		Listeners: []Listener{{ChainID: "ETH1", Iface: "jsonrpc", Port: 3360}},
		Primary: []Upstream{{
			Name: "eth-lava", ChainID: "ETH1", Iface: "jsonrpc",
			URLs:   []string{"https://eth1.lava.build", "wss://eth1.lava.build/websocket"},
			Addons: []string{"debug", "trace"},
		}, {
			Name: "eth-keyed", ChainID: "ETH1", Iface: "jsonrpc",
			URLs: []string{"https://eth.example.com"},
			Auth: &Auth{Var: "RPC_KEY_ETH1", Kind: "header:x-api-key"},
		}},
		Backup: []Upstream{{Name: "eth-bk", ChainID: "ETH1", Iface: "jsonrpc", URLs: []string{"https://backup"}}},
	}
	tpl := c.YAML()
	// schema essentials. Each upstream is ONE provider; its capabilities are extra
	// addon-tagged node-urls under that single provider (base http url + ws url +
	// one addon-tagged url per capability). A separate addon provider would be
	// excluded at real boot on a subscription-tagged spec (the router demands a ws
	// url on every jsonrpc provider), and an addon on the same provider as the ws
	// url is exactly what makes the base "||" collection available to the addon
	// router. See writeRPCBlock for the live-verified reasoning.
	must := []string{
		`cache-be: "cache:20100"`, `chain-id: "ETH1"`,
		`- url: "wss://eth1.lava.build/websocket"`, `addons: ["debug"]`, `addons: ["trace"]`,
		`- name: "eth-lava"`, `- name: "eth-keyed"`,
		`auth-config:`, `x-api-key: "${RPC_KEY_ETH1}"`, "backup-direct-rpc:",
	}
	for _, m := range must {
		if !contains(tpl, m) {
			t.Errorf("YAML missing %q\n---\n%s", m, tpl)
		}
	}
	// Capabilities are NOT separate providers — there must be no "<name>-<addon>"
	// provider entry. (Older layout emitted eth-lava-debug / eth-lava-trace.)
	for _, bad := range []string{`- name: "eth-lava-debug"`, `- name: "eth-lava-trace"`} {
		if contains(tpl, bad) {
			t.Errorf("YAML must NOT split capabilities into providers, found %q\n---\n%s", bad, tpl)
		}
	}
	// The ws url appears exactly once — under the single eth-lava provider.
	if n := countOf(tpl, `- url: "wss://eth1.lava.build/websocket"`); n != 1 {
		t.Errorf("ws url must appear once, got %d\n---\n%s", n, tpl)
	}
	// The http url appears 3× under eth-lava: the base node-url + the debug
	// addon node-url + the trace addon node-url — all in one provider.
	if n := countOf(tpl, `- url: "https://eth1.lava.build"`); n != 3 {
		t.Errorf("eth-lava http url should appear 3× (base + debug + trace node-urls), got %d\n---\n%s", n, tpl)
	}
	// One provider per upstream: eth-lava + eth-keyed (primary) + eth-bk (backup),
	// plus the single listener → 4 api-interface lines total.
	if n := countOf(tpl, `    api-interface: "jsonrpc"`); n != 4 {
		// 1 listener + 2 primary providers + 1 backup provider = 4 occurrences
		t.Errorf("expected 4 api-interface lines (1 listener + 2 primary + 1 backup), got %d\n---\n%s", n, tpl)
	}
	// env vars
	if vs := c.EnvVars(); len(vs) != 1 || vs[0] != "RPC_KEY_ETH1" {
		t.Errorf("env vars = %v", vs)
	}
	// render expands the placeholder
	rendered := Render(tpl, map[string]string{"RPC_KEY_ETH1": "secret123"})
	if !contains(rendered, `x-api-key: "secret123"`) {
		t.Errorf("render didn't expand:\n%s", rendered)
	}
	// lint: clean once rendered
	if p := c.Lint(rendered); len(p) != 0 {
		t.Errorf("lint should be clean, got %v", p)
	}
	// lint: flags missing upstream
	c2 := &Config{Listeners: []Listener{{ChainID: "BASE", Iface: "jsonrpc", Port: 3361}}}
	if p := c2.Lint(c2.YAML()); len(p) == 0 {
		t.Error("lint should flag missing upstream")
	}
}

// TestArchiveWsEmitsNoSkip: every node url is verified over its own connection, so an
// archive url beside a ws url boots with its own archive check — the emitter skips none.
func TestArchiveWsEmitsNoSkip(t *testing.T) {
	withWs := (&Config{
		Listeners: []Listener{{ChainID: "ETH1", Iface: "jsonrpc", Port: 3360}},
		Primary: []Upstream{{
			Name: "eth", ChainID: "ETH1", Iface: "jsonrpc",
			URLs:   []string{"https://ethereum-rpc.publicnode.com", "wss://ethereum-rpc.publicnode.com"},
			Addons: []string{"archive", "debug"},
		}},
	}).YAML()
	if contains(withWs, `skip-verifications:`) {
		t.Errorf("archive+ws must not emit skip-verifications\n---\n%s", withWs)
	}
	if n := countOf(withWs, `addons: ["archive"]`); n != 1 {
		t.Errorf("archive should get its own node url once, got %d\n---\n%s", n, withWs)
	}
}

// TestAuthOnEveryNodeUrl: each node url is verified and dialed on its own, so a keyed
// upstream's ws url and every addon copy carry the key too.
func TestAuthOnEveryNodeUrl(t *testing.T) {
	y := (&Config{
		Listeners: []Listener{{ChainID: "ETH1", Iface: "jsonrpc", Port: 3360}},
		Primary: []Upstream{{
			Name: "eth", ChainID: "ETH1", Iface: "jsonrpc",
			URLs:   []string{"https://eth.example.com", "wss://eth.example.com"},
			Addons: []string{"archive", "debug"},
			Auth:   &Auth{Var: "RPC_KEY_ETH1", Kind: "header:x-api-key"},
		}},
	}).YAML()
	if n := countOf(y, `x-api-key: "${RPC_KEY_ETH1}"`); n != 4 {
		t.Errorf("want the key on all 4 node urls (base, ws, archive, debug), got %d\n---\n%s", n, y)
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (indexOf(s, sub) >= 0) }
func countOf(s, sub string) int {
	n := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			n++
		}
	}
	return n
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
