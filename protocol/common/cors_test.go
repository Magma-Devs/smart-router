package common

import "testing"

func TestCORSAllowOrigin(t *testing.T) {
	cases := []struct {
		name          string
		configured    string
		requestOrigin string
		wantValue     string
		wantSend      bool
		wantVary      bool
	}{
		{"wildcard default", "*", "https://app.example", "*", true, false},
		{"single origin is sent as configured", "https://app.example", "https://other.example", "https://app.example", true, false},
		{"empty value is sent as configured", "", "https://app.example", "", true, false},
		{"list echoes a listed origin", "https://a.example,https://b.example", "https://b.example", "https://b.example", true, true},
		{"list ignores spaces around entries", "https://a.example, https://b.example", "https://b.example", "https://b.example", true, true},
		{"list matches the origin case-insensitively", "https://A.example,https://b.example", "https://a.example", "https://a.example", true, true},
		{"list refuses an unlisted origin", "https://a.example,https://b.example", "https://evil.example", "", false, true},
		{"list refuses a request with no origin", "https://a.example,https://b.example", "", "", false, true},
		{"wildcard inside a list allows everyone", "https://a.example,*", "https://evil.example", "*", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, send, vary := CORSAllowOrigin(c.configured, c.requestOrigin)
			if value != c.wantValue || send != c.wantSend || vary != c.wantVary {
				t.Errorf("CORSAllowOrigin(%q, %q) = (%q, %v, %v), want (%q, %v, %v)",
					c.configured, c.requestOrigin, value, send, vary, c.wantValue, c.wantSend, c.wantVary)
			}
		})
	}
}
