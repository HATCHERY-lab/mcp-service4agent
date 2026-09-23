//! `mcp-service4agent` — a shared foundation for agent-tool services that
//! expose an MCP (Model Context Protocol) surface.
//!
//! # Layers (features)
//!
//! - **core** (always compiled) — [`mcp`], a JSON-RPC-2.0-over-HTTP MCP
//!   server built with a fluent API and mounted as a real `axum::Router`,
//!   and [`router`] (`DocRouter`) — a route-recording wrapper around
//!   `axum::Router` that makes a route's description a required argument
//!   instead of an afterthought.
//! - **`sqlite`** — [`storage`]: a single-writer SQLite engine (`Db` +
//!   `BatchWriter`) plus a read-only connection pool (`DbPool`) and
//!   versioned migrations.
//! - **`nemo`** — [`manifest`] (wire types for d-watchdog's service
//!   registry) and [`register`] (push self-registration against `PUT
//!   /registry/:name`). This feature integrates with d-watchdog, a
//!   self-hosted service supervisor used by the crate's own authors; most
//!   consumers outside that environment can leave it off.
//!
//! # Example
//!
//! ```no_run
//! use axum::Router;
//! use mcp_service4agent::mcp::{McpServer, Tool, ToolOutcome};
//! use serde_json::json;
//!
//! # async fn boot() {
//! let mcp = McpServer::<()>::new("example-service", "0.1.0").tool(
//!     Tool::new(
//!         "ping",
//!         "Answers pong.",
//!         json!({ "type": "object", "properties": {} }),
//!     ),
//!     |_state, _ctx, _args| async { ToolOutcome::ok(json!({ "pong": true })) },
//! );
//!
//! let app: Router<()> = mcp.into_router().with_state(());
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
//! axum::serve(listener, app).await.expect("serve");
//! # }
//! ```

pub mod mcp;
pub mod router;

#[cfg(feature = "sqlite")]
pub mod storage;

#[cfg(feature = "nemo")]
pub mod manifest;

#[cfg(feature = "nemo")]
pub mod register;
