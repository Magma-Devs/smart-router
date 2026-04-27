//go:build testing

package licensing

import "testing"

func TestKeyTestIsRegistered(t *testing.T) {
	if _, ok := Resolve("key_test"); !ok {
		t.Fatal("key_test not registered under //go:build testing — Stage C wiring is broken")
	}
}
