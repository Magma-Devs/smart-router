package e2egate

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// CloneTests checks out the tests/ directory of a repository's default branch
// into dir: a shallow, sparse clone that downloads that directory's files and
// no others. The token travels in an HTTP header set through git's environment
// configuration, so it appears in no command line, URL or error message.
func CloneTests(ctx context.Context, url, token, dir string) error {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if token != "" {
		header := "AUTHORIZATION: basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
		env = append(env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.extraHeader", "GIT_CONFIG_VALUE_0="+header)
	}
	git := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := git("clone", "--quiet", "--depth=1", "--filter=blob:none", "--sparse", url, dir); err != nil {
		return err
	}
	return git("-C", dir, "sparse-checkout", "set", "tests")
}
