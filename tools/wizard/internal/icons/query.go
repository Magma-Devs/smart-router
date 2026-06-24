package icons

import (
	"os"
	"strings"
	"time"

	"golang.org/x/term"
)

// queryKittyGraphics is a thin wrapper for the diagnostic (kitty only).
func queryKittyGraphics() bool { k, _ := queryGraphics(); return k }

// queryGraphics asks the terminal, in one round-trip, whether it supports the
// Kitty graphics protocol and/or Sixel. It sends a tiny Kitty graphics query
// (a=q) plus a Primary Device Attributes request (\x1b[c, answered by every
// terminal). From the reply:
//   - kitty  = an APC "\x1b_G…;OK\x1b\\" response is present.
//   - sixel  = the DA1 reply "\x1b[?…c" lists capability "4".
// Bounded by a timeout so a silent terminal can't hang us. TTY-only.
func queryGraphics() (kitty, sixel bool) {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		return false, false
	}
	old, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return false, false
	}
	defer term.Restore(int(os.Stdin.Fd()), old)

	const graphicsQuery = "\x1b_Gi=1,a=q,f=24,s=1,v=1,t=d;AAAA\x1b\\"
	const daRequest = "\x1b[c"
	if _, err := os.Stdout.WriteString(graphicsQuery + daRequest); err != nil {
		return false, false
	}

	ch := make(chan string, 1)
	go func() {
		var buf []byte
		tmp := make([]byte, 256)
		for {
			n, err := os.Stdin.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
				if strings.Contains(string(buf), "\x1b[?") && strings.ContainsRune(string(buf), 'c') {
					break
				}
			}
			if err != nil {
				break
			}
		}
		ch <- string(buf)
	}()

	var resp string
	select {
	case resp = <-ch:
	case <-time.After(350 * time.Millisecond):
		return false, false
	}

	kitty = strings.Contains(resp, "\x1b_G") && strings.Contains(resp, "OK")
	sixel = da1HasSixel(resp)
	return kitty, sixel
}

// da1HasSixel parses the DA1 reply "\x1b[?<n>;<n>;…c" for the sixel capability
// code "4" as a distinct, semicolon/?-delimited field.
func da1HasSixel(resp string) bool {
	i := strings.Index(resp, "\x1b[?")
	if i < 0 {
		return false
	}
	rest := resp[i+3:]
	if end := strings.IndexByte(rest, 'c'); end >= 0 {
		rest = rest[:end]
	}
	for _, f := range strings.Split(rest, ";") {
		if f == "4" {
			return true
		}
	}
	return false
}
