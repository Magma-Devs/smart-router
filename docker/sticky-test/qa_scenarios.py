#!/usr/bin/env python3
"""Production-readiness scenarios for cross-pod sticky sessions.

Runs against an already-running falsegap stack. Each scenario is one named check with a
pass/fail line, so a QA engineer can read the output without knowing the internals.

Every request closes its connection: a reused connection pins to one replica through the
round-robin ingress, which would make several of these pass for the wrong reason.

Which upstream answered is read from the router's Lava-Provider-Address header, not from the
`servedBy` the fake stamps into the body. The two differ exactly when it matters: a reply from
the response cache carries the body of whichever upstream filled the cache, and the header says
Cached. A sticky request never reads the cache (MAG-4122), so a Cached sticky reply is reported
as that regression instead of being counted as the upstream its body names (MAG-4129).
"""
import argparse
import collections
import concurrent.futures
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request

INGRESS = "http://127.0.0.1:18081"
PRIMARIES = ["upstream-primary-0", "upstream-primary-1", "upstream-primary-2"]
# router-a, router-b and router-c behind the round-robin ingress (docker-compose.falsegap.yml).
REPLICAS = 3
# Lava-Provider-Address on a reply the router took from its response cache.
CACHED = "Cached"
# How many session ids a check asks with when it has to fail without the registry. Without it
# each replica picks for itself, and all three pick the same upstream at least one time in nine
# (the minimum for three upstreams, reached when selection is even): 2 of 24 single-id trials
# with the feature off. A one-id check passes a missing feature that often. Every one of six ids
# has to hold, so at that rate a feature-off run passes by chance about once in half a million.
SESSIONS = 6
# Where claims-before-restart leaves the claims that restarted-pod reads after the restart, in
# its own process. run-qa.sh points this at a file of its own.
STATE = os.environ.get("STICKY_QA_STATE") or os.path.join(tempfile.gettempdir(), "sticky-qa-claims.json")


class Reply:
    """One reply, with the headers that say which replica and which upstream produced it.

    A failure is returned, not raised: a pinned call the router refuses is an outcome these
    scenarios report, not a reason to stop the run.
    """

    def __init__(self, status, headers, raw):
        self.status = status
        self.pod = headers.get("X-Router-Pod", "?")
        self.provider = headers.get("Lava-Provider-Address", "?")
        self.retries = int(headers.get("Lava-Retries") or 0)
        try:
            self.payload = json.loads(raw)
        except ValueError:
            self.payload = {}

    @property
    def ok(self):
        return self.status == 200 and "error" not in self.payload

    @property
    def cached(self):
        return self.provider == CACHED

    @property
    def result(self):
        return self.payload.get("result")

    @property
    def error(self):
        error = self.payload.get("error")
        message = error.get("message") if isinstance(error, dict) else error
        return f"HTTP {self.status}: {message or 'no error message'}"

    def __str__(self):
        if self.ok:
            return f"{self.provider} (Lava-Retries {self.retries}, pod {self.pod})"
        return f"refused on pod {self.pod}, {self.error}"


def rpc(method, params, sticky=None, select=None, request_id=1, timeout=30):
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
    headers = {"Content-Type": "application/json", "Connection": "close"}
    if sticky:
        headers["lava-stickiness"] = sticky
    if select:
        headers["lava-select-provider"] = select
    req = urllib.request.Request(INGRESS, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return Reply(response.status, response.headers, response.read())
    except urllib.error.HTTPError as exc:
        return Reply(exc.code, exc.headers, exc.read())


def head(sticky=None):
    """The head call must carry the session id too — see s_id_required_on_every_call.

    Returns the head, or None when the call failed, and the reply.
    """
    reply = rpc("eth_blockNumber", [], sticky=sticky)
    return (int(reply.result, 16) if reply.ok else None), reply


def tip():
    """The pool's head, asked unpinned: only a starting point for the block numbers a scenario asks for."""
    number, reply = head()
    if number is None:
        sys.exit(f"  the ingress did not report a head: {reply.error}")
    return number


def fetch_block(number, sticky=None, select=None, request_id=1):
    return rpc("eth_getBlockByNumber", [hex(number), False], sticky=sticky, select=select, request_id=request_id)


def tally(replies):
    """The upstreams that answered, by header, with cached and failed replies kept apart: neither
    says which upstream the pin chose."""
    answered = collections.Counter(r.provider for r in replies if r.ok and not r.cached)
    cached = [r for r in replies if r.ok and r.cached]
    failed = [r for r in replies if not r.ok]
    return answered, cached, failed


def anomalies(cached, failed):
    """Report lines for replies an affinity count must not absorb. Either one fails the scenario."""
    lines = []
    if cached:
        lines.append(f"answered from the cache: {len(cached)} (must be 0: a sticky request never reads it, MAG-4122)")
    if failed:
        lines.append(f"failed: {len(failed)} (must be 0), first: {failed[0]}")
    return lines


def sessions_hold(prefix, send):
    """Ask with SESSIONS ids, send(id) returning each id's replies, and judge them together.

    Every run takes fresh ids. Re-run against a kept stack, a fixed id would adopt the claim the
    previous run left, and its first requests would race for nothing.

    Returns whether every id stayed on one upstream with no reply cached or failed, the report
    lines, and each id's upstream (the first that answered it).
    """
    run = int(time.time())
    split, lines, claims, everything = 0, [], {}, []
    for s in range(SESSIONS):
        sticky = f"{prefix}-{run}-{s}"
        replies = send(sticky)
        servers, _, _ = tally(replies)
        split += len(servers) != 1
        lines.append(f"  {sticky}: {dict(servers)}")
        first = next((r.provider for r in replies if r.ok and not r.cached), None)
        if first:
            claims[sticky] = first
        everything += replies
    _, cached, failed = tally(everything)
    lines += [
        f"replicas that served: {len({r.pod for r in everything})}",
        f"ids on more than one upstream: {split} (must be 0)",
        *anomalies(cached, failed),
    ]
    return split == 0 and not cached and not failed, lines, claims


class Round:
    """One round of the customer's sequence: ask for the head, then ask for that block."""

    def __init__(self, index, height, head_reply, fetch_reply):
        self.index = index
        self.height = height
        self.head = head_reply
        self.fetch = fetch_reply

    @property
    def refused(self):
        """The router failed a call of the round, so the caller saw an error rather than a gap."""
        return not self.head.ok or not self.fetch.ok

    @property
    def gap(self):
        return not self.refused and self.fetch.result is None

    @property
    def split(self):
        """Head and fetch named different upstreams: the pin did not hold across the two calls."""
        return not self.refused and self.head.provider != self.fetch.provider

    def __str__(self):
        if not self.head.ok:
            return f"round {self.index}: head {self.head}"
        fetch = f"from {self.fetch}" if self.fetch.ok else str(self.fetch)
        return f"round {self.index}: head {self.height} from {self.head}; fetch {fetch}"


def ingestion_round(index, sticky=None, pin_head=True):
    """The customer's own sequence: ask for the head, then ask for that block.

    pin_head=False reproduces the half-pinned mistake: the fetch carries the session id but
    the head call does not, so the head can come from an upstream the fetch never goes to.
    """
    height, head_reply = head(sticky=sticky if pin_head else None)
    fetch_reply = head_reply if height is None else fetch_block(height, sticky=sticky, request_id=index)
    return Round(index, height, head_reply, fetch_reply)


def pinned_gap_cause(round_):
    """Which known path produced a false gap in a round that carried the id on both calls, read
    from the two replies' headers, so a failure names the ticket that owns it (MAG-4121)."""
    if round_.head.cached or round_.fetch.cached:
        return "answered from the cache, which a sticky request never is (MAG-4122)"
    if round_.head.provider != round_.fetch.provider:
        if round_.head.pod == round_.fetch.pod:
            return "one replica sent head and fetch to different upstreams: a pinned call moved (MAG-4123)"
        return ("two replicas sent head and fetch to different upstreams: they disagree on the claim (MAG-3136),"
                " or a pinned call moved (MAG-4123)")
    return "one upstream answered null for a block it gave as its head: no known cause"


def pinned_verdict(rounds):
    """Judge rounds that carried the id on both calls: whether they held, and the lines to print.

    A false gap needs the head and the fetch on different upstreams, so that is asserted
    directly. It catches the pin failing in every round it fails, not only in those where the
    fetch's upstream is also behind: with the feature off, 23 of 36 rounds split and 9 of them
    gapped. Ten rounds that all agree by chance is then about 0.36^10, one in 25,000; asserting
    gaps alone let a feature-off run pass about one time in 20. With the feature on, 0 of 40 split.
    """
    answered = [r for r in rounds if not r.refused]
    split = [r for r in answered if r.split]
    gaps = [r for r in answered if r.gap]
    # Refused rounds are left out, so they shrink the sample a split is looked for in. With the
    # feature off a round agrees by chance 13 times in 36, so one answered round out of ten would
    # pass about one run in three. At least half the rounds have to be answered. With the feature
    # on and about one pinned head call in ten refused, ten rounds fall short of that about once
    # in 7,000 runs.
    enough = 2 * len(answered) >= len(rounds)
    lines = [
        f"head and fetch on different upstreams: {len(split)} of {len(answered)} answered rounds (must be 0)",
        f"false gaps: {len(gaps)} of {len(answered)} answered rounds (must be 0)",
        *(f"  {r}" + (f" -> false gap: {pinned_gap_cause(r)}" if r.gap else "") for r in answered if r.split or r.gap),
        *refusals(rounds),
    ]
    if not enough:
        lines.append(f"answered rounds: {len(answered)} of {len(rounds)} (must be at least half, or too few are left to judge the pin)")
    return enough and not split and not gaps, lines


def refusals(rounds):
    """Report lines for rounds the router refused. They are not false gaps and fail nothing.

    Since MAG-4123 a pinned call fails rather than move to another upstream when the router
    believes the pinned one is more than 10 blocks behind the tip. These fakes advance a block a
    second, faster than the router polls their heads, so an upstream 0-2 blocks behind can look
    11-15 behind for a moment, and a pinned `latest` call is refused. The caller sees an error,
    which is exactly what a silent gap is not.
    """
    refused = [r for r in rounds if r.refused]
    if not refused:
        return []
    return [f"refused, not counted as gaps: {len(refused)} of {len(rounds)} rounds, first: {refused[0]}"]


def report(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    for line in detail:
        print(f"         {line}")
    return ok


# --------------------------------------------------------------------------------------

def s_customer_sequence(rounds=12, block_time=1.0):
    """The reported bug: head from one node, block from another that is behind."""
    results = []
    for i in range(rounds):
        if i:
            time.sleep(block_time)
        results.append(ingestion_round(i, sticky=f"ingest-{i}"))
    held, verdict = pinned_verdict(results)
    pods = {r.head.pod for r in results} | {r.fetch.pod for r in results}
    servers = collections.Counter(r.fetch.provider for r in results if not r.refused)
    return report("Customer sequence across replicas", held and len(pods) > 1, [
        f"{rounds} head-then-fetch rounds, one session id each",
        f"replicas that served: {len(pods)}",
        f"upstreams used: {dict(servers)}",
        *verdict,
    ])


def s_concurrency(parallel=24):
    """Many requests for ONE session id at once must not split across upstreams.

    One id cannot tell the feature from its absence (see SESSIONS): the replicas send their
    shares of the 24 almost entirely to one upstream each, so more requests per id barely help.

    Every id asks for the same blocks: a sticky request never reads the cache, and one that did
    would say Cached and fail the check, not pass it.
    """
    h = tip()
    blocks = [h - 1 - i for i in range(parallel)]

    def send(sticky):
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
            return list(pool.map(lambda ib: fetch_block(ib[1], sticky, None, ib[0]), enumerate(blocks)))

    ok, lines, _ = sessions_hold("concurrent", send)
    return report("Concurrent requests on one session id", ok, [
        f"{SESSIONS} session ids, {parallel} simultaneous requests each",
        *lines,
    ])


def s_select_provider_wins():
    """lava-select-provider is the more specific ask and must beat a sticky claim."""
    h = tip()
    # Establish a claim first.
    claim = fetch_block(h - 100, sticky="both-headers", request_id=1)
    other = next(p for p in PRIMARIES if p != claim.provider)
    served = fetch_block(h - 101, sticky="both-headers", select=other, request_id=2)
    return report("lava-select-provider beats a sticky claim", claim.ok and served.ok and served.provider == other, [
        f"claim resolved to: {claim}",
        f"explicitly asked for: {other}",
        f"served by: {served} (must be the explicitly named one)",
    ])


def s_claims_before_restart():
    """Claims hold across the replicas before one restarts, and are recorded for the check after.

    Each id is asked once per replica, which the round-robin ingress spreads one to each, so every
    replica holds the claim when router-a goes down and only the restarted one has to adopt it.
    The upstream each id settled on is written to STATE for s_restarted_pod_adopts.
    """
    h = tip()
    ok, lines, claims = sessions_hold(
        "survives-restart", lambda sticky: [fetch_block(h - 200, sticky=sticky, request_id=i) for i in range(REPLICAS)])
    with open(STATE, "w") as state:
        json.dump(claims, state)
    return report("Claims hold before a replica restarts", ok, [
        f"{SESSIONS} session ids, one request per replica each",
        *lines,
    ])


def s_restarted_pod_adopts():
    """A replica with an empty local table must adopt the fleet's existing claim.

    The claims are the ones claims-before-restart made before run-qa.sh restarted router-a. An id
    first used after the restart would only be claimed afresh, which says nothing about adoption.
    Every id must stay on the upstream it had before.
    """
    try:
        with open(STATE) as state:
            claims = json.load(state)
    except (OSError, ValueError):
        claims = {}
    if not claims:
        return report("Restarted replica adopts the existing claim", False, [
            f"no claims recorded in {STATE}: run claims-before-restart before restarting a replica",
        ])
    h = tip()
    moved, lines, everything = 0, [], []
    for sticky, before in claims.items():
        replies = [fetch_block(h - 201 - i, sticky=sticky, request_id=i) for i in range(9)]
        servers, _, _ = tally(replies)
        moved += set(servers) != {before}
        lines.append(f"  {sticky}: before {before}, after {dict(servers)}")
        everything += replies
    _, cached, failed = tally(everything)
    return report("Restarted replica adopts the existing claim", moved == 0 and not cached and not failed, [
        f"{len(claims)} claims made before the restart, 9 requests each after it",
        *lines,
        f"ids that left their upstream: {moved} (must be 0)",
        *anomalies(cached, failed),
    ])


def s_cache_down_fails_closed():
    """With the registry unreachable, sticky traffic fails and plain traffic does not."""
    h_ok, plain_err = None, None
    try:
        h_ok, plain = head()  # plain traffic, no sticky header
        if h_ok is None:
            plain_err = plain.error
    except Exception as exc:
        plain_err = repr(exc)

    sticky_failed, sticky_detail = False, ""
    try:
        reply = fetch_block((h_ok or 21000000) - 300, sticky="cache-is-down", request_id=1)
        if reply.ok:
            sticky_detail = f"sticky request SUCCEEDED from {reply} (it must fail while the registry is unreachable)"
        else:
            sticky_failed, sticky_detail = True, f"sticky request failed with HTTP {reply.status}, as intended"
    except Exception as exc:
        sticky_failed, sticky_detail = True, f"sticky request failed: {type(exc).__name__}"

    return report("Registry unreachable: fail closed, plain traffic unaffected",
                  sticky_failed and plain_err is None,
                  [sticky_detail,
                   f"plain (no header) request: {'served normally' if plain_err is None else 'FAILED: ' + plain_err}"])


def s_cache_recovers():
    """After the registry comes back, sticky traffic works again."""
    h = tip()
    ok, lines, _ = sessions_hold(
        "after-recovery", lambda sticky: [fetch_block(h - 400 - i, sticky=sticky, request_id=i) for i in range(8)])
    return report("Registry restored: sticky sessions work again", ok, [
        f"{SESSIONS} session ids, 8 requests each",
        *lines,
    ])


def s_id_required_on_every_call(rounds=10, block_time=1.0, fetch_only_rounds=30):
    """The session id has to be on EVERY call in the sequence, not just the fetch.

    This is a usage requirement rather than a defect, and it is easy to get wrong: if the head
    call goes out unpinned it can be answered by an upstream that is ahead, while the pinned
    fetch goes to the claimed one, which may be behind. The result is the original bug, with
    the header apparently in use. The check proves both halves.

    A fetch-only round gaps only when its head came from an upstream ahead of the one its fetch
    is pinned to: 19 of 60 rounds measured, so a fixed 10 rounds found none about one run in
    fifty and failed the scenario for nothing. That half runs until its first gap instead, usually
    within three rounds, and gives up after fetch_only_rounds: at 19 in 60 the chance of finding
    none in 30 is about one in a hundred thousand.
    """
    both = []
    for i in range(rounds):
        if i:
            time.sleep(block_time)
        both.append(ingestion_round(i, sticky=f"both-{i}", pin_head=True))
    held, verdict = pinned_verdict(both)

    fetch_only, tried = None, 0
    while fetch_only is None and tried < fetch_only_rounds:
        if tried:
            time.sleep(block_time)
        round_ = ingestion_round(tried, sticky=f"fetchonly-{tried}", pin_head=False)
        tried += 1
        if round_.gap:
            fetch_only = round_
    ok = held and fetch_only is not None
    return report("Session id required on every call in the sequence", ok, [
        "id on BOTH calls:",
        *(f"  {line}" for line in verdict),
        f"id on the FETCH call only -> {'a' if fetch_only else 'no'} false gap in {tried} rounds"
        " (must be one; proves the requirement is real)",
        *([f"  {fetch_only}"] if fetch_only else []),
        "Customers must send the header on the head request too, not only the block fetch.",
    ])


SCENARIOS = {
    "id-on-every-call": s_id_required_on_every_call,
    "customer": s_customer_sequence,
    "concurrency": s_concurrency,
    "select-provider": s_select_provider_wins,
    "claims-before-restart": s_claims_before_restart,
    "restarted-pod": s_restarted_pod_adopts,
    "cache-down": s_cache_down_fails_closed,
    "cache-recovered": s_cache_recovers,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    args = parser.parse_args()
    sys.exit(0 if SCENARIOS[args.scenario]() else 1)
