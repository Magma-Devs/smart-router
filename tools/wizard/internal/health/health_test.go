package health

import "testing"

func strptr(s string) *string { return &s }

// TestWSLive verifies the ws-row-specific verdict used by the wizard's ws gate.
// A base+ws config is probed to verify a ws url; WSLive must judge the ws row
// alone, so a failing http base (e.g. one the user force-added) can't masquerade
// as a ws failure, and a config with no ws row reports not-live rather than a
// vacuous pass.
func TestWSLive(t *testing.T) {
	t.Run("ws row passes, failing base ignored", func(t *testing.T) {
		env := &Envelope{Results: []Result{
			{Transport: "http", OK: false, Error: "base force-added, did not verify"},
			{Transport: "ws", OK: true, LatestBlock: 25386591},
		}}
		ok, detail := env.WSLive()
		if !ok {
			t.Fatalf("ws row OK should make WSLive true (got false: %q)", detail)
		}
		if detail != "block=25386591" {
			t.Errorf("detail = %q, want block=25386591", detail)
		}
	})

	t.Run("ws row fails", func(t *testing.T) {
		env := &Envelope{Results: []Result{
			{Transport: "http", OK: true, LatestBlock: 100},
			{Transport: "ws", OK: false, Error: "dial wss: connection refused"},
		}}
		ok, detail := env.WSLive()
		if ok {
			t.Fatal("a failing ws row must make WSLive false")
		}
		if detail != "dial wss: connection refused" {
			t.Errorf("detail = %q, want the ws row's error", detail)
		}
	})

	t.Run("no ws row present", func(t *testing.T) {
		env := &Envelope{Results: []Result{
			{Transport: "http", OK: true, LatestBlock: 100},
		}}
		ok, detail := env.WSLive()
		if ok {
			t.Fatal("no ws row → WSLive must be false (not a vacuous pass)")
		}
		if detail != "no websocket endpoint was probed" {
			t.Errorf("detail = %q, want the no-ws-row message", detail)
		}
	})

	t.Run("envelope-level error", func(t *testing.T) {
		env := &Envelope{Error: strptr("spec load failed")}
		ok, detail := env.WSLive()
		if ok || detail != "spec load failed" {
			t.Errorf("WSLive() = (%v, %q), want (false, spec load failed)", ok, detail)
		}
	})

	t.Run("nil / empty", func(t *testing.T) {
		if ok, _ := (*Envelope)(nil).WSLive(); ok {
			t.Error("nil envelope must be not-live")
		}
		if ok, _ := (&Envelope{}).WSLive(); ok {
			t.Error("empty envelope must be not-live")
		}
	})
}
