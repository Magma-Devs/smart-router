---
name: can-i-merge
description: Use when asking whether a pull request is finished — "can we merge", "can i merge", "is this PR ready", "is it done", "ship check" — or before asking anyone to review or approve one. Also use before calling a ticket complete because its PR is green, and before reporting that a change is verified.
---

# Can I merge?

Green CI answers "does it build". It does not answer "did we do the work".

This skill does not own the merge decision. It produces a repeatable,
evidence-based answer, so the person making that decision is not relying on
anyone's memory — including the author's.

Readiness needs two kinds of evidence that want opposite things. **Session
evidence** lives in the working session — what was attempted, interrupted, or
skipped on purpose. None of it is in git, and a cold reader cannot recover it.
**Cold evidence** has to throw that context away: ticket, then diff, then PR
body, read by someone who never heard the author's reasoning. The author is the
worst judge of whether the author finished.

## Prerequisites — check these before the first gate

This skill cannot produce a verdict without three things. Check them up front;
finding out at gate 2 wastes the cold read.

**1. An agent that can dispatch subagents.** Gate 2 runs two context-isolated
subagents. In Claude Code that is the Agent tool. If you cannot dispatch
subagents, this skill stops at gate 2 and says so.

**2. `gh` authenticated.** `gh auth status` must succeed for the repo.

**3. Read access to the team Jira** (`https://magmadevs.atlassian.net`). Gate 2a
reads the MAG ticket the PR names. The Atlassian MCP server is NOT a must —
either of these works:

- **Option A — Atlassian MCP** (what the automation team runs). Needs `uv`
  installed (`uvx` ships with it) and a Jira API token. Create the token at
  id.atlassian.com → Security → "Create API token". Then register the server
  once:

  ```bash
  claude mcp add --scope user atlassian \
    --env JIRA_URL=https://magmadevs.atlassian.net \
    --env JIRA_USERNAME=<your-atlassian-email> \
    --env JIRA_API_TOKEN=<your-api-token> \
    -- uvx mcp-atlassian
  ```

  The gate 2a subagent then reads tickets with `mcp__atlassian__jira_get_issue`
  (loaded via ToolSearch). Keep the token out of the repo — it lives in your
  user-level Claude config, never in a tracked file.

- **Option B — plain REST, no MCP.** The same API token, used directly:

  ```bash
  curl -s -u "<your-atlassian-email>:<your-api-token>" \
    "https://magmadevs.atlassian.net/rest/api/2/issue/MAG-1234" \
    | jq '{summary: .fields.summary, description: .fields.description}'
  ```

  If you use this option, put this curl in the gate 2a subagent's brief as the
  ticket input instead of the MCP tool.

**The ticket itself.** Every PR here names its MAG ticket in the body —
`#Closes MAG-XXXX` or `Jira ticket: MAG-XXXX`. If your work has no ticket yet,
create one in the MAG project first (Jira UI → Create, or
`jira_create_issue` over MCP) — gate 2a blocks on a PR that names no ticket,
because nobody can say what the PR was for.

## When to use

- Before asking anyone to review, approve, or merge a PR.
- Before calling a ticket done because its PR is green.
- Before reporting that a change is verified.
- "can we merge" / "is this PR ready" / "is it done" / "ship check".

## The gates are a floor, not a ceiling

This skill was tested head to head on a real PR. One agent reviewed with the
skill, one without. **The agent without the skill found six broken files. The
agent with the skill found none.**

No gate was wrong. The agent holding the skill worked the gates faithfully and
never looked at anything the gates did not name. The agent without the skill had
no gates, so it read the diff and saw what was there. A checklist directs
attention, and directing attention is the same act as narrowing it. Two rules
follow, and both bind:

- **Passing every gate is the minimum, not the finish.** The gates name the ways
  this team has shipped a false ready before, not every way a PR can be broken.
- **"I finished the gates" is not a verdict.** Before writing `READY`, name at
  least one thing you looked at that no gate asked for. If you cannot, you did
  not review the PR — you filled in a form. The report has a line for this.

### How much effort a review deserves

Scale it to the change, and measure it in files opened, not minutes spent.

- **Any size:** every changed file opened and read in full, before gate 0.
- **Changes a shared name:** every caller the PR did not touch, opened one by
  one — gate 5.
- **A mechanical rename, or a generated file:** read the rule the change
  follows, then open every file where that rule did not apply cleanly.
- **Over ~20 files or ~500 changed lines:** budget a second pass. A large diff
  hides more, not less, and the first pass is where attention runs out.

If you opened fewer files than the diff changed, you are not finished. Running
out of time is an honest result: name the files you did not open, and the verdict
is `INSUFFICIENT EVIDENCE`, not `READY`.

## Step 1 — read the change cold, before any gate

**Stop reading this file here.** Go and read the change, come back, and only then
read the gates. That order is the point of the section, and reading on first
defeats it — you cannot un-read a checklist. Do this before the ticket and before
the PR body too.

```bash
BASE=$(git merge-base origin/main HEAD)
git diff "$BASE...HEAD" --stat     # how big is it
git diff "$BASE...HEAD"            # all of it, top to bottom
```

Read every file the change touches — not the summary, and not only the parts a
gate would ask about. Then write down, before anything else:

1. What this change does, in two sentences, in your own words.
2. Anything that stands out — surprising, out of place, half-finished, or that
   you do not understand.
3. Any file you skimmed instead of reading, named.

Item 2 is the output that matters, and it has no fixed shape on purpose. If it is
empty on a change of more than a few lines, you skimmed. Go back.

**Why the order is fixed.** A checklist read first fills your attention, and
whatever fills attention also excludes. Run the gates first and they find exactly
what they name, and nothing else. The cold read is the only chance to notice the
thing nobody wrote a gate for — the six files above.

Keep the note. Gate 9 checks it against the PR body, and the report asks what you
found outside the gates.

Then run the gates. Gates 0, 1 and 3 to 9 run here, in the session that holds the
working context; gate 2 runs in two subagents that hold none. Both kinds of
evidence are required, and neither decides on its own — you do.

## The four verdicts

| Verdict | Means |
|---|---|
| `READY` | no unresolved blockers |
| `READY WITH DISCLOSED DEVIATIONS` | intentional divergence exists, and is documented where a reviewer sees it |
| `NOT READY` | a concrete blocker exists |
| `INSUFFICIENT EVIDENCE` | something required cannot actually be proven |

The overall verdict is the worst one present. One unproven item makes the whole
answer `INSUFFICIENT EVIDENCE`, even when nothing is failing.

**`NOT READY` and `INSUFFICIENT EVIDENCE` are different answers.** The first is a
proven negative: you checked, the answer is bad. The second is the absence of an
answer. Without the second, an agent turns "I cannot verify this ran" into
"probably fine" or into "it did not run" — both invented, and they point at
opposite next actions.

Evidence is insufficient, not negative, when:

| Situation | Why it is not a proven negative |
|---|---|
| A run was interrupted | Its passes are real; its silence is not a result |
| The result came from a handoff, a findings doc, or another agent | Someone else's measurement — name whose and when, or take it again |
| The checking tool may itself be broken | A search that finds nothing looks identical to a clean tree |
| The result predates the current code | A pass at an older commit does not describe this one |
| The infrastructure is unreachable right now | Unreachable is a scheduling fact, not a verdict |

Every unproven item names the one thing that would settle it. If you cannot tell
"never ran" from "ran and the result was lost", say exactly that — they need
different next actions.

## Gate 0 — what evidence would make this look ready when it isn't

Answer in writing; it reframes every later gate. *Given this specific PR, what
could be true right now that would make every other gate report green while the
change is actually unverified?* The seed rows below come from this team's own
history — extend the table as your repo ships its own false readies:

| Looks like | Actually |
|---|---|
| CI green | CI ran unit tests; the changed behaviour only shows on a deployed router, and nothing deployed ran it |
| The binary was built and copied to the server | the Helm chart still passes a flag this binary does not know — the pod CrashLoops on the next rollout |
| The deployed version string matches the release | the version subcommand prints the semver tag, not the commit; only `strings <binary> | grep <commit-hash>` confirms the running build |
| The binary was replaced in place on the pod | pods do not pick up an in-place replacement; until a rollout restart, the old code is still serving |
| `go test ./...` passed | an assertion silently did nothing — a mock that always answers, an empty set compared to an empty set (`make test` runs `-count=1`, so at least caching is not the trap) |
| "All tests pass" in the PR body | an interrupted run was forgotten between writing that line and now |
| The ticket's checkbox is done | the change is merged but not deployed; nothing is live |
| A failure is labelled pre-existing | nobody compared against the fork point |
| A search reported zero hits | the pattern was wrong; prove it on something known-present first |

Write a short list of "this would look ready if …" for the PR in front of you,
then check each one. A gate you did not think to run cannot fail.

## Gate 1 — work completeness

```bash
git status --short                       # empty
git log origin/<branch>..HEAD --oneline  # empty
```

Also: unresolved reviewer threads, and whether each resolved one has a commit that
answers it — a thread can be resolved by clicking. Match bot logins without regard
to case, and **print the raw thread count before the filtered one**; a filter
matching nothing looks exactly like a clean PR. A TODO left in the diff counts
here, not as style but as scope: a TODO is deferred work, so it also needs gate 7.

## Gate 2 — two context-isolated subagents

**Both fire on every run. They are part of the method, not options.** Dispatch
them as soon as the cold read is done; they work while you run the other gates.

Isolation is the property that matters in both, not the model: a fresh context
cannot inherit what you have already decided is important.

Gate 2 **requires** ticket access and the ability to dispatch subagents — both
are in Prerequisites at the top of this file. If either is unavailable, this
skill cannot produce a verdict: say which input is missing, and stop. Do not
substitute the PR body for the ticket — the PR body is the author's account of
the ticket, and that account is what is being checked. Do not substitute
yourself for either isolated reader. A degraded gate 2 is worse than no gate 2,
because it produces a verdict that looks like the real one.

### 2a — obligations

Model: haiku. Not for cost — this is a bounded comparison rather than open
exploration, so the cheap model is the right tool.

Dispatch with the Agent tool, `model: "haiku"`, and give it **only** this:

```
Evaluate ticket satisfaction using only the ticket, the current PR diff, and the
PR body. You have no history with this work.

Do not infer intent from the implementation. Do not accept an implementation
rationale that is not visible in the PR body or the ticket — if the reasoning
is not there, a reviewer cannot see it either.

Inputs:
  ticket   mcp__atlassian__jira_get_issue for each MAG key (ToolSearch it
           first), or the curl from this skill's Prerequisites if no MCP
  diff     git -C <worktree> diff <merge-base>...HEAD
  PR body  gh pr view <N> --json body --jq .body

Enumerate every obligation the ticket expresses. An obligation is any sentence
telling someone to do, delete, add, or check something; split compound
sentences. Then classify each one:

  implemented          the diff does it
  explicitly deviated  the diff does not, AND the PR body or ticket says why
  missing              the diff does not, and nothing reviewer-visible says why
  cannot determine     you cannot tell from these three inputs alone

| Obligation (quote it) | Classification | Where in the diff, or what is missing |

Then, in one or two sentences each:
  1. List every `missing` obligation.
  2. Does the diff do anything substantial the ticket did not ask for? Name it.
  3. Does the PR body claim anything the diff does not support?

Answer only from the three inputs. If you could not read part of the diff, say
which part rather than guessing.
```

`missing` is the finding — a reviewer would merge believing it was done.
`explicitly deviated` is fine, and belongs in the report as a disclosed deviation.
Verify the subagent's factual claims before repeating them; it is a cheap model
reading a large diff and it will get details wrong.

**A PR body that names no MAG ticket is itself a blocker** — nobody can say what
the PR was for. (If this repo adopts an explicit no-ticket label, that label
counts as an answer.)

### 2b — the adversary

A second subagent whose only job is to argue this PR should not merge. It gets no
checklist and no boundary, because the gates already cover what a checklist finds;
this one exists to find what they do not. Run it on the model you are using
yourself — open exploration, not bounded comparison, so the cheap model is the
wrong tool here.

Dispatch with the Agent tool and give it **only** this:

```
Argue that PR <N> in <repo> should not be merged.

You have no history with this work, and no checklist. Look wherever you like —
the diff, the files it changed, the files it did NOT change, the tests, the
config, the PR body, the ticket, the git log. Nothing is out of scope.

  BASE=$(git merge-base origin/main HEAD)
  git -C <worktree> diff "$BASE...HEAD"
  gh pr view <N> --json title,body,files

Find the strongest reasons this should not merge, ordered by how much each one
matters. For each: what is wrong, the file and line that shows it, and what a
reviewer would see fail if this merged as it stands.

Rules:
- Every claim names a file and a line you actually opened. No claim from the
  diff summary alone.
- Include reasons the change is incomplete, not only reasons it is wrong: a
  caller nobody updated, a test that asserts nothing, a case the code does not
  handle, a name the change deleted that something else still uses.
- If your strongest objection is style or taste, say so and rank it last.
- If you cannot find a real objection, say that plainly and list where you
  looked. "I found nothing, here is where I looked" is an acceptable answer.
  An invented objection is not.
```

Check every objection yourself before it enters the report. A confirmed one becomes
a Blocking or Non-blocking line. **An objection you checked and disproved still gets
one line**, saying you checked it — that is how a reader tells "looked at, and it is
fine" from "never looked".

## Gate 3 — verification evidence

Two questions, and the second is the one usually skipped.

### 3a. Which changed tests ran, where, and what came back

```bash
git diff <merge-base>...HEAD --name-only -- '*_test.go'
```

Every file gets a row, in the shape
`changed behaviour → required verification → environment → result`. Not "tests
pass". The named environments here are `make test` (`go test ./... -count=1`),
`make test-short` (the rpcsmartrouter subset), `make lint` (`go vet ./...`), and
a deployed router when the behaviour only exists live. Three outcomes need
separating, because they need different next actions:

| What happened | State | Next action |
|---|---|---|
| Never executed | proven negative — blocker | run it |
| Ran, passed, at an older commit | unproven | re-run at this commit |
| Ran partway, or ran and the result was lost | unproven | re-run and capture per-test names |

### 3b. Does the evidence test the behaviour that CHANGED

Stronger than "which tests ran". Map
`ticket obligation → implementation → the specific test that proves it`. Any
obligation with no behavioural proof is unproven, even inside a green run. A
ticket that changes failover selection is not proven by a green
`go test ./protocol/...` — it is proven by a test that fails a primary and
watches the backup serve.

Also re-run the PR body's own "How to verify" commands where possible, and report
how many you actually confirmed.

## Gate 4 — regression: whose failures are these

Every remaining failure gets exactly one label:

`introduced` · `pre-existing` · `environment` · `unclassified`

`pre-existing` requires a run at the **fork point**, not a claim from a handoff
or an earlier session. `unclassified` **blocks** when the failure sits in
behaviour this PR touched.

## Gate 5 — integration, the diff, and the code around it

Define the diff helper once; everything in this gate uses it.

```bash
BASE=$(git merge-base origin/main HEAD)
d() { git diff "$BASE...HEAD" "$@"; }   # a function, not a string — a string
                                        # does not word-split in zsh, so every
                                        # check below prints nothing, which reads
                                        # exactly like clean
```

### Does what landed meanwhile interact with this

Not "is the branch behind main" but "do the two changes touch the same files".

```bash
git rev-list --count HEAD..origin/main
comm -12 <(git diff --name-only HEAD...origin/main | sort) \
         <(git diff --name-only origin/main...HEAD | sort)
```

Twenty commits behind that touch nothing you touched is irrelevant. One commit
touching a file you also changed matters, and the two have never run together.

### What else is in the diff that should not be

Each is one line.

```bash
# debugging leftovers and temporary skips
d -U0 | grep -nE '^\+.*(fmt\.Print|println\(|spew\.|TODO|FIXME|XXX|HACK)'
d -U0 | grep -nE '^\+.*(t\.Skip\(|t\.SkipNow\(|//nolint)'

# endpoints, hosts and credentials written into the diff
d -U0 | grep -nE '^\+.*(https?://[a-z0-9.-]+\.[a-z]{2,}|[0-9]{1,3}(\.[0-9]{1,3}){3})'
d -U0 | grep -niE '^\+.*(api[_-]?key|secret|token|password|Bearer )'

# generated or derived files changed without their source changing
d --name-only | grep -E '(_mock\.go$|\.pb\.go$|^go\.sum$|\.bak|\.orig|\.log|^artifacts/)'

# scope: anything outside what the ticket describes
d --name-only

# prove the scan runs at all before believing a clean result
d --name-only | wc -l    # must match the PR's file count
```

What blocks: a stray `fmt.Println` or `println(` used for debugging, always
(read the hit — `fmt.Print` in a CLI command's output path is legitimate).
Anything credential-shaped, until proven a fake fixture value. A new `t.Skip`
that carries no ticket key or that the PR body never mentions — without a ticket
key a skip is a silent permanent green. A new `//nolint` with no reason on the
same line. A URL or IP that names a real host rather than a placeholder. A
`.pb.go` or `_mock.go` that changed with no matching change to the `.proto` or
the interface it mocks — a local regeneration the author did not intend; a
`go.sum` that changed with no `go.mod` change is the same smell. Files outside
the ticket's area the PR body does not explain.

### What this change means for code it did not touch

The checks above ask what is inside the diff. This one asks the opposite, and it
is the question the head-to-head test was lost on: **what does this change mean
for code that is not in it?** The six files that agent missed were callers the
diff never opened.

**Step 0 — let the compiler do the part it can do.**

```bash
go build ./... && go vet ./...
```

In Go, a deleted or renamed name, a changed argument count, and a broken
interface implementation are compile errors. A clean build proves every caller
**inside this module** still compiles. That is the floor, and it is real — but
it is only the floor. Three kinds of contract change survive a clean build:

- **Behaviour moved under an unchanged signature.** A function that returned an
  error for an unknown input now panics, or returns a zero value; a default
  timeout constant changed. Every call site compiles; the answers changed.
- **A name referenced from outside Go.** A CLI flag (`Flags().String(...)`), a
  config key in a `yaml:"..."` or `mapstructure:"..."` struct tag, a Prometheus
  metric name, a log line a dashboard matches on. The compiler never sees the
  Helm chart, the values files, the Grafana boards, or the docs. This team has
  shipped exactly this: a chart passing a flag the new binary did not know, and
  the pod CrashLooped on rollout.
- **A deleted test.** Nothing calls a test, so nothing misses it at compile
  time. A silently dropped test is a silently dropped guarantee.

**Step 1 — list what moved, what left the language, and what tests disappeared.**

```bash
# functions whose body or signature this diff touched (generated code excluded)
moved() {
  d -U0 -- '*.go' ':!*.pb.go' ':!*_mock.go' \
    | grep -E '^@@.*func |^[-+][[:space:]]*(func |return |panic\()' \
    | grep -oE 'func (\([^)]*\) )?[A-Za-z_][A-Za-z0-9_]*\(' \
    | sed -E 's/^func (\([^)]*\) )?//; s/\($//' \
    | grep -vE '^(_|Test|Benchmark|Fuzz)' | sort -u
}
# ^ the `_` filter matters: gRPC glue names its handlers `_Service_Method_Handler`,
#   and in this repo that glue lives in hand-named files (types/relay/grpc_service.go),
#   so a *.pb.go filename exclusion alone does not remove it
moved | wc -l   # positive control: zero, on a diff that changed Go, means
                # the scan broke — not that no contract moved

# flag names, config keys and metric names this diff added, removed or renamed
d -U0 -- '*.go' | grep -nE '^[-+].*(Flags\(\)\.[A-Za-z]+|yaml:"|mapstructure:"|Name:[[:space:]]*")'

# tests this diff removed and did not add back
GONE=$(mktemp)   # not a fixed path — two reviews running at once must not share it
d -U0 -- '*_test.go' | grep -E '^-func (Test|Benchmark|Fuzz)' \
  | grep -oE '(Test|Benchmark|Fuzz)[A-Za-z0-9_]*' | sort -u > "$GONE"
d -U0 -- '*_test.go' | grep -E '^\+func (Test|Benchmark|Fuzz)' \
  | grep -oE '(Test|Benchmark|Fuzz)[A-Za-z0-9_]*' | sort -u \
  | comm -23 "$GONE" -
```

`-U0` puts the enclosing `func` in most hunk headers, which is where `moved`
gets its names; the flag/key line over-reports on purpose — `Name:` also matches
things that are not metrics. Read its hits rather than counting them. A scan
that misses one looks exactly like a scan that found nothing.

**Step 2 — for every changed flag, config key or metric name: find who uses the
old name, in every file type, in every repo that consumes it.**

```bash
refs() { git grep -nw -e "$1" HEAD -- ':!*.pb.go' ':!.claude'; }
# ^ .claude excluded on purpose: this skill's own example text mentions real flag
#   names, and without the exclusion every future rename sweep hits it as noise

refs <old-flag-or-key>                    # this repo: docs, scripts, workflows, values
git -C <chart-checkout> grep -nw -e "<old-flag-or-key>"   # the chart that deploys this
                                          # (chart repo: Magma-Devs/smart-router-helm-chart)
```

**Search every file type, not only `*.go` — and not only this repo.** A flag
name lives in the Helm chart's container args; a config key lives in values
files; a metric name lives in dashboards and alerts. Those consumers are in
other repos, and `go build` is silent about all of them. Read `HEAD`, not the
working tree — a half-finished edit on disk is not what the PR proposes. `-w`
matches whole words, so `debug-address` does not also report
`debug-address-extra`.

**Step 3 — for every `moved` name whose behaviour changed: open every caller
this PR did not touch. This is the step that finds it.**

```bash
callers()   { git grep -lw -e "$1" HEAD -- '*.go' ':!*_test.go' ':!*.pb.go' | sed 's|^HEAD:||'; }
# ^ do not drop the sed: git grep prefixes every path with HEAD:, and without
#   stripping it untouched() subtracts nothing and sites() greps a pathspec that
#   matches no file — printing nothing, for every symbol, which reads exactly like clean
untouched() { comm -23 <(callers "$1" | sort) <(d --name-only | sort); }
sites()     { untouched "$1" | while read -r f; do git grep -n -A3 -w -e "$1" HEAD -- "$f"; done; }

sites <name>         # each call site with three lines of context: argument and guard
```

| What you see in the call site | Means | Verdict |
|---|---|---|
| `if err != nil` around a call that no longer returns that error | the caller's failure branch went dead, or a new failure now escapes it | blocks |
| `errors.Is` / `errors.As` on a sentinel or type this diff changed | the handler stopped matching | blocks |
| a constant or literal argument | does the new behaviour still accept that value? open the function and check | blocks if not |
| the old name inside a chart, values file, workflow or doc | nothing compiles it — it breaks at deploy time, or silently never matches | blocks |
| the result used the same way the new behaviour intends | unaffected | fine — name it in the report |

A blocker here is fixed in this PR, unless the PR body says why it is safe.
"I opened it and it is fine" is not the same as "I did not look", so name the
callers you cleared. A name with untouched callers nobody opened is
`INSUFFICIENT EVIDENCE`, not `READY`. A renamed flag or config key whose old
name still sits in the chart blocks outright.

## Gate 6 — operational delivery: does merging deliver anything

If merging completes delivery, nothing more is needed. If it does not — a deploy,
a chart change, or a manual migration is still required — then either it has
already happened, or the PR body records it as post-merge work with a named
owner. Anything else blocks.

GitHub cannot tell you this from a green PR. Merging this repo's main deploys
nothing: the binary must be built, synced to the cluster, and the pods rolled —
an in-place binary replacement without a rollout restart keeps serving the old
code. Some behaviour does not exist at all unless deployed with the right
switches: the debug server only starts when `--debug-address` is set. "Merged"
and "anything is live" are different claims; say which one you are making.

## Gate 7 — knowledge preservation

**If the work matters after this session, it needs a durable reference.**

A Jira key, a GitHub issue, or a line in the PR body. Not conversation memory,
and not a findings doc nobody will open. Anything deferred, any TODO, any
accepted gap — name where it lives. "Only in this chat" blocks anything that
matters.

## Gate 8 — reviewability

A PR can be correct and still unsafe to merge, because nobody can review it: a
large mechanical refactor mixed with a behavioural change, a surprising
implementation with no explanation, or noisy files burying the meaningful diff.
Scope larger than the ticket is gate 5's last check; here it matters for a
different reason — the reviewer stops reading.

## Gate 9 — PR truthfulness

The PR body is what the reviewer reads instead of the diff. Check it against the
diff as it stands **now**, not as it stood when the body was written: every push
can falsify a sentence in it, so a body claiming a green run that predates the last
two commits is a false claim even though it was true when written.

| Claim in the body | How to check it |
|---|---|
| "What changed" describes the diff | read both; a body written at commit 1 rarely survives commit 4 |
| "How to verify" commands | run them; report `<n>/<m>` confirmed |
| "all tests pass" / any result | does a run you can name back it, at this commit? |
| a named fix | is there a commit that makes that change |
| known limitations | are the gaps this skill just found actually stated |

Then check the body against your step 1 notes. A body that omits a limitation you
found is not merely incomplete — it is the reason the reviewer will not look for it.

## A search that returns zero may be broken, not clean

A broken search and a clean repo produce the same output. Every zero needs a
positive control: the same search, run against something known present, must
return non-zero.

```bash
<the search that found the problem>            # must return zero
<the same search, on something known present>  # must return non-zero
```

| Situation | Verdict |
|---|---|
| search returns zero, control returns non-zero | fixed |
| search returns zero, control also returns zero | **the search is broken** — unproven, not fixed |
| the PR fixed a list someone handed it | **unproven** — a list shows the problem exists, never how far it goes. Scan and compare |

Three searches in the session that produced this skill returned nothing while
broken: `\s` is not valid POSIX ERE, which is what `git grep -E` uses; a
`D="git diff …"` string does not word-split in zsh; a too-narrow pathspec
matched a fraction of the tree. One of them read a third of the repo's test
names and reported the branch had removed none, while it had removed six — so
**print the population count next to every set comparison.** A count far below
the repo's real size means the pattern is broken, not that the branch is clean.

The last table row is the one that bites: in that same session a fix was briefed
from a six-file list, and scanning the directory found twelve, two of which
nothing had flagged.

Same family: **compare against the merge base, not `origin/main`.** Against a main
that has moved, every test that landed meanwhile reads as one this branch deleted.

## The report

Compact. One screen. No narrative.

```
SHIP VERDICT: <READY | READY WITH DISCLOSED DEVIATIONS | NOT READY | INSUFFICIENT EVIDENCE>

Blocking
- <one line each, with what clears it>

Unproven
- <one line each, with the one thing that would settle it>

Declared deviations
- <obligation>. <kept/changed> intentionally; documented in PR: yes|no

Non-blocking
- <reviewer should know>

Evidence
- <environment>: <result>
- PR How-to-verify: <n>/<m> commands confirmed

Independent ticket review (gate 2a)
- <n> implemented
- <n> explicitly deviated
- <n> missing            <- unexplained; any number above zero blocks
- <n> cannot determine

Adversary review (gate 2b)
- <objection>: confirmed | checked and disproved
- <one line if it found nothing, naming where it looked>

Looked at beyond the gates
- <what the cold read flagged, and any file you opened that no gate named>
- <files this PR changed that you did NOT open, if any>
```

Rules for the report:

- **"Evidence" means you ran it.** Never list a result someone else produced, or
  one inherited from a document. Name the run, or move it to Unproven.
- An interrupted run is reported with its coverage — "killed at 89%, 11 failures,
  names not captured" — never as "not run", and never left out.
- Every Blocking and Unproven line ends in an action, not a description.
- No hedging. "probably", "should be", "no reason to think it would fail" are each
  an `INSUFFICIENT EVIDENCE` item wearing the wrong words.
- **"Looked at beyond the gates" must not be empty on a `READY`.** An empty one
  says the gates were filled in, not that the PR was reviewed.

## Don't

After the verdict, offer to close mechanical gaps only, waiting for a separate
yes on each:

unpushed commits → push · branch behind main → merge main in · a thread whose fix
is already pushed → reply and resolve · a check that died in setup → rerun

Never offer, and never do, without being asked each time: **approve, merge, write
to Jira, or edit the PR body.** Those belong to the human.
