# Sui's own GraphQL end-to-end queries

`queries.json` holds the 538 GraphQL queries of Sui's transactional end-to-end tests. They come
from [MystenLabs/sui](https://github.com/MystenLabs/sui), directory
`crates/sui-indexer-alt-e2e-tests/tests/graphql`, at commit
`a4f70b9e6639587a2bac63a1684097b74bbdcefd`. The file records the commit too.

`TestParseGraphQLMsgServesEverySuiE2EQuery` checks that the router serves every one of them. This is
the guard MAG-4351's decision asks for. The router refuses a query it cannot parse, so its parser
must not be stricter than Sui's: if Sui runs a query and the router refuses it, a customer can no
longer send a valid request.

## What is in it

- Each `//# run-graphql` query of the 174 `.move` tests under `graphql/`. It is unwrapped from its
  `/* ... */` block, and from its optional ```` ```graphql ```` fence, the way Sui's runner does it
  (`unwrap_graphql_query` in `crates/sui-transactional-test-runner/src/test_adapter.rs`).
- Each query is kept exactly as Sui wrote it. Placeholders such as `@{obj_1_0}` sit inside string
  literals, so they don't change how the document parses.
- A query is left out if the test's `.snap` records `GRAPHQL_PARSE_FAILED` as Sui's reply. None is
  at this commit.
- The tests driven from Rust (execute and simulate transaction, subscriptions) are not included.
  They build their queries in Rust code, not in a test file.

## Licence

The queries are Sui's and stay under Sui's licence: Apache-2.0, Copyright (c) Mysten Labs, Inc.
`LICENSE` beside this file is Sui's licence file from the same commit. They are test data here and
are not part of the router's build.

## Regenerating

From a checkout of MystenLabs/sui (a sparse checkout of
`crates/sui-indexer-alt-e2e-tests/tests` is enough):

```sh
go test ./protocol/chainlib/chainproxy/rpcInterfaceMessages/ \
  -run TestParseGraphQLMsgServesEverySuiE2EQuery -count=1 \
  -args -update-sui-e2e-queries="$SUI_CHECKOUT" \
  -sui-e2e-queries-commit="$(git -C "$SUI_CHECKOUT" rev-parse HEAD)"
```

The test pins the number of queries. A regeneration that changes the number fails until the pin is
updated, so a lost query can't go unnoticed.
