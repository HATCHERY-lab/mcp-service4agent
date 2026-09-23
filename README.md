# mcp-service4agent

A shared foundation for agent-tool services that expose an
[MCP](https://modelcontextprotocol.io) (Model Context Protocol) surface over
HTTP: a JSON-RPC 2.0 server with a size budget, a route-recording wrapper
around [`axum`](https://docs.rs/axum), and two optional layers for the rest
of a typical service's plumbing.

## Layers (features)

### core (always compiled)

- **`mcp`** — `McpServer<S>`, a fluent builder for a JSON-RPC-2.0-over-HTTP
  MCP server. Handles `initialize`, `ping`, `tools/list`, `tools/call`,
  batching, and notifications per the MCP wire contract, and mounts as a
  real `axum::Router<S>` via `into_router()`. Ships a `Budget` type so a
  server can assert its own `tools/list` stays within a size law (tool
  count, per-tool bytes, total bytes, `instructions` length) as a one-line
  test.
- **`router`** — `DocRouter<S>`, a wrapper around `axum::Router` that
  records `(method, path, description, auth, ...)` as routes are added.
  axum has no public way to enumerate a built router's routes; `DocRouter`
  makes the description a *required* argument of every route-adding call,
  so a route added without one is a compile error, not a manifest with a
  silent gap.

### `sqlite`

Single-writer SQLite storage: `Db` (the one writer connection, WAL +
foreign_keys), `DbPool` (N read-only connections, each pragma'd
`query_only = ON` so an accidental write fails loudly instead of racing the
writer), `BatchWriter` (one dedicated worker thread, batched transactions
with a `SAVEPOINT` per operation, a `barrier()`/`barrier_async()`
durability point, and live stats), and `Migration`/`MigrationRunner` (a
versioned `schema_migrations` ledger, idempotent re-runs).

### `nemo`

Wire types and a push-registration client for a service supervisor's
registry endpoint (`PUT /registry/:name`, bearer-authenticated). This
feature integrates with **d-watchdog**, a self-hosted service supervisor
used internally by this crate's own authors — most consumers outside that
environment can leave it off entirely. Registration never fails or delays a
service's own startup: every attempt returns a `RegisterOutcome` value, and
a missing token or an unreachable supervisor is logged once, not treated as
a startup error.

## Minimal example

```rust,no_run
use axum::Router;
use mcp_service4agent::mcp::{McpServer, Tool, ToolOutcome};
use serde_json::json;

#[tokio::main]
async fn main() {
    let mcp = McpServer::<()>::new("example-service", "0.1.0").tool(
        Tool::new(
            "ping",
            "Answers pong.",
            json!({ "type": "object", "properties": {} }),
        ),
        |_state, _ctx, _args| async { ToolOutcome::ok(json!({ "pong": true })) },
    );

    let app: Router<()> = mcp.into_router().with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
