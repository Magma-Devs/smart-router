package licensing

import (
	"fmt"
	"os"
	"strings"
)

// LoadFromFile reads a license envelope from path, trims surrounding whitespace,
// and returns the contents. The returned string is suitable for passing to
// Validate. Returns an error if the path is empty, the file is missing,
// unreadable, or has no non-whitespace contents.
//
// This function does NOT validate the envelope's structure (that's Validate's
// job) — it's a thin file-I/O primitive so the call site can compose precedence
// chains (e.g. flag > env var > default path).
func LoadFromFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("license file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read license file %q: %w", path, err)
	}
	envelope := strings.TrimSpace(string(data))
	if envelope == "" {
		return "", fmt.Errorf("license file %q is empty", path)
	}
	return envelope, nil
}

// LoadFromEnvOrFile reads a license envelope from one of two sources:
//
//  1. If envVar is set to a non-empty (post-trim) value, it's treated as the
//     file path. fromEnv is true on return.
//  2. Otherwise, fallbackPath is used. fromEnv is false on return.
//
// Either way, resolvedPath is the actual path that was attempted, so the
// caller can log it.
//
// Important: an env var set to a *non-existent* file does NOT fall back to
// fallbackPath. Setting the env var is a deliberate signal; a missing file
// there is operator error and should fail loudly with the env path in the
// error message — silent fallback would mask "I set the env var but the
// router is still using the default" misconfiguration.
//
// An empty env var value is treated identically to an unset env var.
func LoadFromEnvOrFile(envVar, fallbackPath string) (envelope, resolvedPath string, fromEnv bool, err error) {
	envValue := strings.TrimSpace(os.Getenv(envVar))
	fromEnv = envValue != ""
	if fromEnv {
		resolvedPath = envValue
	} else {
		resolvedPath = fallbackPath
	}
	envelope, err = LoadFromFile(resolvedPath)
	return envelope, resolvedPath, fromEnv, err
}
