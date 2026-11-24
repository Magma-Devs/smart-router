package common

import "strings"

// CORSAllowOrigin decides the Access-Control-Allow-Origin a reply carries for a request whose
// Origin header is requestOrigin, given the --cors-origin value configured.
//
// One configured value, "*" or a single origin, is sent exactly as configured, whatever the
// request's Origin. A comma-separated list cannot be sent as it is: the header holds one origin,
// and a browser refuses a reply that names several. So a list answers with the request's own
// Origin when the list names it, and with no header at all when it does not, which is how a
// browser is told the caller is not allowed. A "*" in the list allows every origin.
//
// send reports whether to set the header, and varyOnOrigin whether the answer depends on the
// request's Origin, in which case the reply must carry Vary: Origin so a shared cache does not
// hand one origin's answer to another.
func CORSAllowOrigin(configured, requestOrigin string) (value string, send bool, varyOnOrigin bool) {
	if !strings.Contains(configured, ",") {
		return configured, true, false
	}
	for _, allowed := range strings.Split(configured, ",") {
		allowed = strings.TrimSpace(allowed)
		if allowed == "*" {
			return "*", true, false
		}
		if requestOrigin != "" && strings.EqualFold(allowed, requestOrigin) {
			return requestOrigin, true, true
		}
	}
	return "", false, true
}
