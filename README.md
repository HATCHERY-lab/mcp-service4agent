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

Single-writer SQLite storage:

- `Db` — the one writer connection (WAL + foreign_keys, plus an explicit
  writer pragma budget applied at open: busy timeout, cache size,
  `synchronous = NORMAL`, a WAL size limit, and a WAL auto-checkpoint
  backstop).
- `ReadPool` — a bounded set of independent read connections to the same
  WAL file, checked out through a semaphore, with a shared page-cache
  budget divided across the pool rather than multiplied per connection.
  Every read carries a deadline (interrupting a runaway statement without
  corrupting the connection) and a slow-query log with per-call-site
  metrics.
- `Checkpointer` — a dedicated connection for `PRAGMA wal_checkpoint`, kept
  off the writer's own queue so a busy writer never blocks a checkpoint and
  a checkpoint never blocks the writer.
- `BatchWriter` — one dedicated worker thread, batched transactions with a
  `SAVEPOINT` per operation, a `barrier()`/`barrier_async()` durability
  point, and live stats including a slow-batch counter.
- `plan_guard` — an `EXPLAIN QUERY PLAN` test helper that panics on a full
  table scan, for pinning a hot query to its index.
- `Migration`/`MigrationRunner` — a versioned `schema_migrations` ledger,
  idempotent re-runs.

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

## Changelog

### 0.2.0

- **Breaking (`sqlite` feature):** `DbPool` is replaced by `ReadPool` — a
  bounded set of independent read connections (rather than a fixed-size
  `Vec` behind one lock) with a shared, size-derived page-cache budget,
  per-call read deadlines, and a slow-query log. Callers of `DbPool::open`
  / `DbPool::read` move to `ReadPool::open` / `ReadPool::read` (or
  `ReadPoolConfig` for the full set of tuning knobs).
- `Db::open` now applies an explicit writer-side pragma budget by default
  (`Db::open_with_writer_pragmas` to override); `BatchWriter` gained a
  slow-batch counter.
- Added `Checkpointer` (dedicated WAL checkpoint connection) and
  `plan_guard` (an `EXPLAIN QUERY PLAN` test helper), both under the
  `sqlite` feature.

### 0.1.0

Initial release.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
