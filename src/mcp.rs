//! A shared MCP (Model Context Protocol) server: JSON-RPC 2.0 over HTTP.
//!
//! # Wire contract
//!
//! JSON-RPC 2.0 over `POST <path>` (default `/mcp`): a single request
//! object or a batch array. A request with no `id` is a *notification* —
//! it gets no response at all; a batch that is entirely notifications
//! answers `204 No Content`, same as a single notification would. `DELETE
//! <path>` always answers `204` — this server is stateless (`initialize`
//! records nothing), so there is nothing a session-end could tear down.
//! `initialize` echoes the client's own `protocolVersion` when it is one of
//! [`SUPPORTED_PROTOCOL_VERSIONS`], else falls back to
//! [`DEFAULT_PROTOCOL_VERSION`]. `ping` answers `{}`. Errors: malformed
//! JSON is `-32700`, an unknown method is `-32601`, a malformed `tools/call`
//! (missing `name`, unknown tool) is `-32602`. A tool that ran and refused
//! is NOT a JSON-RPC error — it is a normal result with `isError: true` (the
//! MCP spec's own distinction between "could not be invoked" and "ran and
//! reported failure").
//!
//! # Design choices
//!
//! - A tool result is `{"content":[{"type":"text","text": ..}],"isError":
//!   bool}` — a [`Value`] body serialises compactly (never pretty-printed)
//!   into `text`; there is no `structuredContent` duplicate, so a result
//!   never doubles itself in the calling agent's context.
//! - A result is capped at [`McpServer::max_result_bytes`] (default
//!   [`DEFAULT_MAX_RESULT_BYTES`]); over the cap, `text` is cut on a UTF-8
//!   char boundary and ends with one line naming how many bytes were cut
//!   (see [`cap_text`]). A handler can still return less on its own.
//! - `initialize` sends no `instructions` unless [`McpServer::instructions`]
//!   was called, and that builder refuses anything over
//!   [`MAX_INSTRUCTIONS_BYTES`] outright (panics — this is an author-time
//!   config mistake, not a runtime condition; see its own doc).
//!
//! # Mounting — `into_router` + `route_docs`
//!
//! [`McpServer::into_router`] returns a real `axum::Router<S>` — a host
//! applies auth with the same primitive it already reaches for on any other
//! route it does not want threaded through [`crate::router::DocRouter`]'s
//! doc-required builder chain: plain `axum::Router::route_layer`, before
//! merging (`DocRouter` exposes the identical method for its own routes, so
//! this is not a second idiom to learn):
//!
//! ```no_run
//! # use axum::Router;
//! # fn auth_layer() -> tower_layer::Identity { tower_layer::Identity::new() }
//! # async fn example<S: Clone + Send + Sync + 'static>(mcp: mcp_service4agent::mcp::McpServer<S>, mut endpoints: Vec<mcp_service4agent::router::Endpoint>, base: Router<S>, state: S) {
//! endpoints.extend(mcp.route_docs()); // borrows `mcp` — call before `into_router` consumes it
//! let mcp_router = mcp.into_router().route_layer(auth_layer());
//! let router = base.merge(mcp_router).with_state(state);
//! # let _: Router<()> = router;
//! # }
//! ```
//!
//! [`McpServer::route_docs`] returns [`crate::router::Endpoint`] directly —
//! the same shape [`crate::router::DocRouter::into_parts`] produces — so a
//! host `.extend()`s the one list it is already building instead of
//! learning a second endpoint shape.
//!
//! # Budget
//!
//! [`McpServer::check_budget`] serialises `tools/list` compactly and names
//! every offender ([`BudgetOffender`]) against a [`Budget`] —
//! [`Budget::WORKSPACE`] is a conservative default profile: at most 8
//! tools, `tools/list` at most 8 KB compact, no single tool over 1.5 KB,
//! `instructions` at most 300 B. The one-line test a server can add:
//!
//! ```
//! use mcp_service4agent::mcp::{Budget, McpServer, Tool, ToolOutcome};
//! use serde_json::json;
//!
//! let server = McpServer::<()>::new("example", "0.1.0").tool(
//!     Tool::new("ping_tool", "Answers pong.", json!({"type": "object", "properties": {}})),
//!     |_state, _ctx, _args| async { ToolOutcome::ok(json!({"pong": true})) },
//! );
//! assert!(server.check_budget(Budget::WORKSPACE).is_ok());
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::router::Endpoint;

/// Echoed back from `initialize` when the client's own `protocolVersion` is
/// not one this server has been reviewed against.
pub const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
/// Every protocol revision this server has been reviewed against.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-03-26", "2025-06-18"];
/// Default cap on a rendered tool result's `content[0].text`, in bytes —
/// see [`McpServer::max_result_bytes`].
pub const DEFAULT_MAX_RESULT_BYTES: usize = 8192;
/// Hard cap on [`McpServer::instructions`], enforced at build time.
pub const MAX_INSTRUCTIONS_BYTES: usize = 300;

const DEFAULT_PATH: &str = "/mcp";

const JSONRPC_PARSE_ERROR: i64 = -32700;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;
const JSONRPC_INVALID_PARAMS: i64 = -32602;

// ---------------------------------------------------------------------------
// JSON-RPC envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RpcRequest {
    /// Absent on a JSON-RPC *notification* — a request with no `id` gets no
    /// response at all, per JSON-RPC 2.0.
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorBody>,
}

#[derive(Debug, Serialize)]
struct RpcErrorBody {
    code: i64,
    message: String,
}

impl RpcResponse {
    fn ok(id: Value, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, result: Some(result), error: None }
    }

    fn err(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self { jsonrpc: "2.0", id, result: None, error: Some(RpcErrorBody { code, message: message.into() }) }
    }
}

// ---------------------------------------------------------------------------
// Tool schema
// ---------------------------------------------------------------------------

/// One `tools/list` entry. `input_schema` is a JSON Schema object, serialised
/// under the wire name `inputSchema`.
#[derive(Clone, Debug, Serialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

impl Tool {
    pub fn new(name: impl Into<String>, description: impl Into<String>, input_schema: Value) -> Self {
        Self { name: name.into(), description: description.into(), input_schema }
    }
}

// ---------------------------------------------------------------------------
// Call context
// ---------------------------------------------------------------------------

/// What a tool handler is handed alongside its own `S` state and its
/// deserialised `arguments`. Carries only what the current HTTP request
/// actually provided — this server invents no session state, so
/// `client_name` is `None` unless the client's `initialize` call rode in the
/// SAME JSON-RPC batch as the `tools/call` being answered (some clients do
/// send every call this way; most send `initialize` alone, in which case a
/// later `tools/call` genuinely carries no client name to hand back — that
/// is the honest answer for a stateless server, not a bug in this type).
#[derive(Clone, Debug)]
pub struct CallContext {
    /// The request's own headers — a host reads these for identity/audit
    /// (e.g. which bearer key called), the same headers it already reads on
    /// its plain HTTP doors.
    pub headers: HeaderMap,
    /// The calling client's own declared name, if its `initialize` call was
    /// present in this same HTTP request.
    pub client_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Tool outcome
// ---------------------------------------------------------------------------

/// The body of a [`ToolOutcome::ok`] result: either a [`Value`] (serialised
/// compactly, never pretty-printed) or plain text used verbatim.
#[derive(Clone, Debug)]
pub enum ToolBody {
    Json(Value),
    Text(String),
}

impl From<Value> for ToolBody {
    fn from(value: Value) -> Self {
        ToolBody::Json(value)
    }
}

impl From<String> for ToolBody {
    fn from(value: String) -> Self {
        ToolBody::Text(value)
    }
}

impl From<&str> for ToolBody {
    fn from(value: &str) -> Self {
        ToolBody::Text(value.to_string())
    }
}

/// What a tool handler returns. Always a normal JSON-RPC *result* — even
/// [`ToolOutcome::error`] — carrying the MCP tool-result envelope
/// (`{"content":[...],"isError":bool}`); see this module's doc, "Wire
/// contract", for why a refusal is never a JSON-RPC error.
pub struct ToolOutcome {
    body: ToolBody,
    is_error: bool,
}

impl ToolOutcome {
    /// A result the caller should treat as success (`isError: false`).
    pub fn ok(body: impl Into<ToolBody>) -> Self {
        Self { body: body.into(), is_error: false }
    }

    /// A result the caller should treat as a refusal (`isError: true`).
    /// `message` should name the offending field and the rule that refused
    /// it. See [`Self::invalid_argument`] for the common shape.
    pub fn error(message: impl Into<String>) -> Self {
        Self { body: ToolBody::Text(message.into()), is_error: true }
    }

    /// Shorthand for the common refusal: an argument that is missing, the
    /// wrong shape, or fails a rule the JSON Schema cannot express. Renders
    /// as `"unknown/invalid argument {field}: {rule}"`.
    pub fn invalid_argument(field: &str, rule: impl fmt::Display) -> Self {
        Self::error(format!("unknown/invalid argument {field}: {rule}"))
    }

    /// Render into the MCP tool-call result envelope, capping
    /// `content[0].text` at `max_result_bytes` (see [`cap_text`]).
    fn render(self, max_result_bytes: usize) -> Value {
        let text = match self.body {
            ToolBody::Json(value) => serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()),
            ToolBody::Text(text) => text,
        };
        let text = cap_text(&text, max_result_bytes);
        json!({
            "content": [{ "type": "text", "text": text }],
            "isError": self.is_error,
        })
    }
}

/// Truncates `text` to fit within `max_bytes`, cutting on a UTF-8 char
/// boundary and appending one line naming how many bytes were cut. A
/// handler that already returns less than the cap is untouched.
fn cap_text(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let original_len = text.len();
    let mut keep = max_bytes.min(original_len);
    loop {
        while keep > 0 && !text.is_char_boundary(keep) {
            keep -= 1;
        }
        let cut = original_len - keep;
        let note = format!("\n\u{2026}[cut {cut} bytes; narrow the request]");
        if keep == 0 || keep + note.len() <= max_bytes {
            let mut out = String::with_capacity(keep + note.len());
            out.push_str(&text[..keep]);
            out.push_str(&note);
            return out;
        }
        let overflow = (keep + note.len()) - max_bytes;
        keep = keep.saturating_sub(overflow.max(1));
    }
}

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

/// A size law for `tools/list`, checked by [`McpServer::check_budget`].
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    pub max_tools: usize,
    pub max_total_bytes: usize,
    pub max_tool_bytes: usize,
    pub max_instructions_bytes: usize,
}

impl Budget {
    /// A conservative default profile: at most 8 tools, `tools/list` at
    /// most 8 KB compact, no single tool over 1.5 KB, `instructions` at
    /// most 300 B.
    pub const WORKSPACE: Budget =
        Budget { max_tools: 8, max_total_bytes: 8192, max_tool_bytes: 1536, max_instructions_bytes: 300 };
}

/// One offender named by [`McpServer::check_budget`].
#[derive(Clone, Debug)]
pub struct BudgetOffender {
    /// What is over budget, e.g. `"tool count"` or `"tool \"m4a_mail_send\""`.
    pub what: String,
    pub actual: usize,
    pub max: usize,
}

impl fmt::Display for BudgetOffender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is {} (max {})", self.what, self.actual, self.max)
    }
}

/// Returned by [`McpServer::check_budget`] on failure — every offender,
/// named, not just the first.
#[derive(Clone, Debug, Default)]
pub struct BudgetReport {
    pub offenders: Vec<BudgetOffender>,
}

impl BudgetReport {
    pub fn is_empty(&self) -> bool {
        self.offenders.is_empty()
    }
}

impl fmt::Display for BudgetReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, offender) in self.offenders.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{offender}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

type ToolHandler<S> =
    Arc<dyn Fn(S, CallContext, Value) -> Pin<Box<dyn Future<Output = ToolOutcome> + Send>> + Send + Sync>;

struct ToolEntry<S> {
    tool: Tool,
    handler: ToolHandler<S>,
}

/// The registry a running server dispatches against — everything
/// [`McpServer::into_router`] needs, held behind one `Arc` so every request
/// clones a pointer, not the tool list.
struct Inner<S> {
    name: String,
    version: String,
    instructions: Option<String>,
    max_result_bytes: usize,
    tools: Vec<ToolEntry<S>>,
}

fn tools_list_value<S>(tools: &[ToolEntry<S>]) -> Value {
    let list: Vec<&Tool> = tools.iter().map(|entry| &entry.tool).collect();
    json!({ "tools": list })
}

/// A JSON-RPC-over-HTTP MCP server, built with a fluent API and mounted with
/// [`into_router`](Self::into_router). See this module's doc for the wire
/// contract and the mounting example.
pub struct McpServer<S> {
    name: String,
    version: String,
    instructions: Option<String>,
    max_result_bytes: usize,
    path: String,
    tools: Vec<ToolEntry<S>>,
}

impl<S> McpServer<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            instructions: None,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            path: DEFAULT_PATH.to_string(),
            tools: Vec::new(),
        }
    }

    /// Sets `initialize`'s `instructions` field. Refuses (panics) anything
    /// over [`MAX_INSTRUCTIONS_BYTES`] — this is an author-time config
    /// mistake caught at server-construction time, not a runtime condition
    /// a caller triggered, the same class of eager check a malformed regex
    /// literal or an out-of-range header value gets elsewhere in the
    /// ecosystem. Omit this call entirely (the common case) and
    /// `initialize` sends no `instructions` at all.
    pub fn instructions(mut self, text: impl Into<String>) -> Self {
        let text = text.into();
        let bytes = text.len();
        if bytes > MAX_INSTRUCTIONS_BYTES {
            panic!(
                "McpServer::instructions: {bytes} bytes exceeds the {MAX_INSTRUCTIONS_BYTES}-byte cap — \
                 shorten the instructions or drop this call; a fixed tool list plus named refusals \
                 teaches an agent the rest"
            );
        }
        self.instructions = Some(text);
        self
    }

    /// Caps a rendered tool result's `content[0].text`, in bytes. Default
    /// [`DEFAULT_MAX_RESULT_BYTES`].
    pub fn max_result_bytes(mut self, max: usize) -> Self {
        self.max_result_bytes = max;
        self
    }

    /// Overrides the mount path (default `/mcp`) — read by
    /// [`route_docs`](Self::route_docs) and [`into_router`](Self::into_router).
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Registers one tool. `handler` runs on every `tools/call` naming
    /// `tool.name`, receiving a clone of the host's own state, this
    /// request's [`CallContext`], and the deserialised `arguments` object
    /// (`{}` when the caller omits `arguments` entirely).
    pub fn tool<F, Fut>(mut self, tool: Tool, handler: F) -> Self
    where
        F: Fn(S, CallContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ToolOutcome> + Send + 'static,
    {
        let handler: ToolHandler<S> = Arc::new(move |state, ctx, args| Box::pin(handler(state, ctx, args)));
        self.tools.push(ToolEntry { tool, handler });
        self
    }

    /// The exact JSON `tools/list` would answer.
    pub fn tools_list_json(&self) -> Value {
        tools_list_value(&self.tools)
    }

    /// Compact-serialised byte length of [`tools_list_json`](Self::tools_list_json).
    pub fn tools_list_bytes(&self) -> usize {
        serde_json::to_vec(&self.tools_list_json()).map(|bytes| bytes.len()).unwrap_or(0)
    }

    /// Checks this server against `budget`, naming every offender — tool
    /// count, any single tool over `max_tool_bytes`, the compact
    /// `tools/list` total, and `instructions` if set. See this module's doc
    /// for the one-line test every server adds.
    pub fn check_budget(&self, budget: Budget) -> Result<(), BudgetReport> {
        let mut offenders = Vec::new();

        let tool_count = self.tools.len();
        if tool_count > budget.max_tools {
            offenders.push(BudgetOffender { what: "tool count".to_string(), actual: tool_count, max: budget.max_tools });
        }

        for entry in &self.tools {
            let bytes = serde_json::to_vec(&entry.tool).map(|v| v.len()).unwrap_or(0);
            if bytes > budget.max_tool_bytes {
                offenders.push(BudgetOffender {
                    what: format!("tool {:?}", entry.tool.name),
                    actual: bytes,
                    max: budget.max_tool_bytes,
                });
            }
        }

        let total = self.tools_list_bytes();
        if total > budget.max_total_bytes {
            offenders.push(BudgetOffender { what: "tools/list total".to_string(), actual: total, max: budget.max_total_bytes });
        }

        if let Some(instructions) = &self.instructions {
            let bytes = instructions.len();
            if bytes > budget.max_instructions_bytes {
                offenders.push(BudgetOffender {
                    what: "instructions".to_string(),
                    actual: bytes,
                    max: budget.max_instructions_bytes,
                });
            }
        }

        if offenders.is_empty() {
            Ok(())
        } else {
            Err(BudgetReport { offenders })
        }
    }

    /// The two [`Endpoint`]s this server serves (`POST`/`DELETE` at this
    /// server's path) — fold into the same `Vec<Endpoint>` a host's
    /// `DocRouter::into_parts()` already produced. See this module's doc,
    /// "Mounting", for why this returns `Endpoint` rather than `RouteDoc`.
    /// `auth` defaults to `"bearer"` — overwrite the field on the returned
    /// entries if a host genuinely serves this unauthenticated.
    pub fn route_docs(&self) -> Vec<Endpoint> {
        let description = format!("MCP JSON-RPC 2.0 endpoint ({} tools). Single request or batch.", self.tools.len());
        vec![
            Endpoint {
                method: "POST".to_string(),
                path: self.path.clone(),
                description,
                auth: "bearer".to_string(),
                key_ref: None,
                public: false,
                body_schema: None,
            },
            Endpoint {
                method: "DELETE".to_string(),
                path: self.path.clone(),
                description: "MCP session end. Stateless server: always 204.".to_string(),
                auth: "bearer".to_string(),
                key_ref: None,
                public: false,
                body_schema: None,
            },
        ]
    }

    /// Consumes this builder into a real `axum::Router<S>` serving
    /// `POST`/`DELETE` at this server's path (default `/mcp`). See this
    /// module's doc, "Mounting", for how a host applies its own auth and
    /// merges this into its own router.
    pub fn into_router(self) -> Router<S> {
        let path = self.path;
        let inner = Arc::new(Inner {
            name: self.name,
            version: self.version,
            instructions: self.instructions,
            max_result_bytes: self.max_result_bytes,
            tools: self.tools,
        });

        let method_router = routing::post(move |State(state): State<S>, headers: HeaderMap, body: Bytes| {
            let inner = inner.clone();
            async move { handle_post(inner, state, headers, body).await }
        })
        .delete(handle_delete);

        Router::new().route(&path, method_router)
    }
}

// ---------------------------------------------------------------------------
// HTTP door
// ---------------------------------------------------------------------------

/// `POST <path>`. Reads the body as raw [`Bytes`] rather than through an
/// `axum::Json` extractor, so a malformed body answers JSON-RPC `-32700`
/// instead of axum's own bare-400 rejection.
async fn handle_post<S>(inner: Arc<Inner<S>>, state: S, headers: HeaderMap, body: Bytes) -> Response
where
    S: Clone + Send + Sync + 'static,
{
    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return Json(RpcResponse::err(Value::Null, JSONRPC_PARSE_ERROR, format!("parse error: {e}"))).into_response();
        }
    };

    match value {
        Value::Array(items) => {
            if items.is_empty() {
                return Json(RpcResponse::err(Value::Null, JSONRPC_INVALID_PARAMS, "empty batch")).into_response();
            }
            let client_name = extract_client_name(&items);
            let mut responses = Vec::with_capacity(items.len());
            for item in items {
                let ctx = CallContext { headers: headers.clone(), client_name: client_name.clone() };
                if let Some(resp) = dispatch_one(&inner, &state, ctx, item).await {
                    responses.push(resp);
                }
            }
            if responses.is_empty() {
                // Every entry in the batch was a notification — nothing to
                // answer, per JSON-RPC 2.0's own batch rule.
                StatusCode::NO_CONTENT.into_response()
            } else {
                Json(responses).into_response()
            }
        }
        single => {
            let client_name = extract_client_name(std::slice::from_ref(&single));
            let ctx = CallContext { headers, client_name };
            match dispatch_one(&inner, &state, ctx, single).await {
                Some(resp) => Json(resp).into_response(),
                None => StatusCode::NO_CONTENT.into_response(),
            }
        }
    }
}

/// `DELETE <path>` — session end. This server is stateless (`initialize`
/// creates no session record), so there is nothing to tear down; always
/// `204`.
async fn handle_delete() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Reads `params.clientInfo.name` off an `initialize` entry, if one is
/// present among `items` — the only source [`CallContext::client_name`]
/// ever has, since this server keeps no session across HTTP calls.
fn extract_client_name(items: &[Value]) -> Option<String> {
    items.iter().find_map(|item| {
        if item.get("method").and_then(Value::as_str) != Some("initialize") {
            return None;
        }
        item.get("params")?.get("clientInfo")?.get("name")?.as_str().map(str::to_string)
    })
}

/// Dispatch one JSON-RPC request or notification. Returns `None` exactly
/// when nothing should be sent back — either the input had no `id`
/// (notification) or it was `notifications/initialized` specifically
/// (fire-and-forget regardless of a stray `id`, since accepting it cannot
/// fail).
async fn dispatch_one<S>(inner: &Inner<S>, state: &S, ctx: CallContext, raw: Value) -> Option<RpcResponse>
where
    S: Clone + Send + Sync + 'static,
{
    let req: RpcRequest = match serde_json::from_value(raw) {
        Ok(r) => r,
        Err(e) => return Some(RpcResponse::err(Value::Null, JSONRPC_PARSE_ERROR, format!("parse error: {e}"))),
    };
    let id = req.id.clone().unwrap_or(Value::Null);
    let is_notification = req.id.is_none();

    if req.method == "notifications/initialized" {
        return None;
    }

    let outcome: Result<Value, (i64, String)> = match req.method.as_str() {
        "initialize" => Ok(handle_initialize(inner, &req.params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools_list_value(&inner.tools)),
        "tools/call" => handle_tools_call(inner, state, ctx, &req.params).await,
        other => Err((JSONRPC_METHOD_NOT_FOUND, format!("unknown method {other:?}"))),
    };

    if is_notification {
        return None;
    }

    Some(match outcome {
        Ok(value) => RpcResponse::ok(id, value),
        Err((code, message)) => RpcResponse::err(id, code, message),
    })
}

/// `initialize` — echoes the client's own `protocolVersion` when it is one
/// of [`SUPPORTED_PROTOCOL_VERSIONS`], else falls back to
/// [`DEFAULT_PROTOCOL_VERSION`].
fn handle_initialize<S>(inner: &Inner<S>, params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let protocol_version = match requested {
        Some(v) if SUPPORTED_PROTOCOL_VERSIONS.contains(&v) => v.to_string(),
        _ => DEFAULT_PROTOCOL_VERSION.to_string(),
    };

    let mut result = serde_json::Map::new();
    result.insert("protocolVersion".to_string(), Value::String(protocol_version));
    result.insert("capabilities".to_string(), json!({ "tools": {} }));
    result.insert("serverInfo".to_string(), json!({ "name": inner.name, "version": inner.version }));
    if let Some(instructions) = &inner.instructions {
        result.insert("instructions".to_string(), Value::String(instructions.clone()));
    }
    Value::Object(result)
}

async fn handle_tools_call<S>(inner: &Inner<S>, state: &S, ctx: CallContext, params: &Value) -> Result<Value, (i64, String)>
where
    S: Clone + Send + Sync + 'static,
{
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| (JSONRPC_INVALID_PARAMS, "tools/call missing string \"name\"".to_string()))?;
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

    let entry = inner
        .tools
        .iter()
        .find(|entry| entry.tool.name == name)
        .ok_or_else(|| (JSONRPC_INVALID_PARAMS, format!("unknown tool {name:?}")))?;

    let outcome = (entry.handler)(state.clone(), ctx, args).await;
    Ok(outcome.render(inner.max_result_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    /// A server with two tools: `echo` (ok, mirrors its `text` argument)
    /// and `boom` (always a named refusal) — enough to exercise every
    /// dispatch path without a real host state type.
    fn test_router() -> Router<()> {
        McpServer::<()>::new("mcp-service4agent-test", "0.0.0")
            .tool(
                Tool::new(
                    "echo",
                    "Echoes its text argument back.",
                    json!({ "type": "object", "required": ["text"], "properties": { "text": { "type": "string" } } }),
                ),
                |_state: (), _ctx: CallContext, args: Value| async move {
                    let text = args.get("text").and_then(Value::as_str).unwrap_or_default().to_string();
                    ToolOutcome::ok(json!({ "echo": text }))
                },
            )
            .tool(
                Tool::new("boom", "Always refuses.", json!({ "type": "object", "properties": {} })),
                |_state: (), _ctx: CallContext, _args: Value| async move {
                    ToolOutcome::invalid_argument("anything", "boom always refuses")
                },
            )
            .into_router()
            .with_state(())
    }

    async fn call(router: Router<()>, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).expect("test body serialises")))
            .expect("test request builds");
        let response = router.oneshot(request).await.expect("router never returns Err");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1_000_000).await.expect("response body reads");
        let value = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).expect("response body is JSON") };
        (status, value)
    }

    async fn call_raw(router: Router<()>, raw: &[u8]) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(raw.to_vec()))
            .expect("test request builds");
        let response = router.oneshot(request).await.expect("router never returns Err");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1_000_000).await.expect("response body reads");
        let value = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).expect("response body is JSON") };
        (status, value)
    }

    #[tokio::test]
    async fn initialize_echoes_a_supported_protocol_version() {
        let (status, resp) = call(
            test_router(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": "2025-03-26" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(resp["result"]["serverInfo"]["name"], "mcp-service4agent-test");
        assert!(resp["result"].get("instructions").is_none(), "no instructions were set: {resp}");
    }

    #[tokio::test]
    async fn initialize_falls_back_to_default_for_an_unsupported_protocol_version() {
        let (_, resp) = call(
            test_router(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": "1999-01-01" } }),
        )
        .await;
        assert_eq!(resp["result"]["protocolVersion"], DEFAULT_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn ping_answers_empty_object() {
        let (status, resp) = call(test_router(), json!({ "jsonrpc": "2.0", "id": 1, "method": "ping" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["result"], json!({}));
    }

    #[tokio::test]
    async fn tools_list_returns_both_registered_tools() {
        let (_, resp) = call(test_router(), json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })).await;
        let tools = resp["result"]["tools"].as_array().expect("tools is an array");
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().expect("name is a string")).collect();
        assert_eq!(names, vec!["echo", "boom"]);
        for tool in tools {
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
    }

    #[tokio::test]
    async fn tools_call_ok_returns_iserror_false() {
        let (_, resp) = call(
            test_router(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "echo", "arguments": { "text": "hi" } } }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], false, "{resp}");
        let text = resp["result"]["content"][0]["text"].as_str().expect("text is a string");
        let parsed: Value = serde_json::from_str(text).expect("text is compact JSON");
        assert_eq!(parsed, json!({ "echo": "hi" }));
    }

    #[tokio::test]
    async fn tools_call_unknown_tool_is_invalid_params() {
        let (_, resp) = call(
            test_router(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "bogus", "arguments": {} } }),
        )
        .await;
        assert_eq!(resp["error"]["code"], JSONRPC_INVALID_PARAMS);
        assert!(resp.get("result").is_none());
    }

    #[tokio::test]
    async fn a_tool_refusal_is_iserror_true_not_a_jsonrpc_error() {
        let (_, resp) = call(
            test_router(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "boom", "arguments": {} } }),
        )
        .await;
        assert!(resp.get("error").is_none(), "a tool refusal must not be a JSON-RPC error: {resp}");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().expect("text is a string");
        assert!(text.contains("unknown/invalid argument anything"), "{text}");
        assert!(text.contains("boom always refuses"), "{text}");
    }

    #[tokio::test]
    async fn unknown_method_is_method_not_found() {
        let (_, resp) = call(test_router(), json!({ "jsonrpc": "2.0", "id": 1, "method": "bogus/method" })).await;
        assert_eq!(resp["error"]["code"], JSONRPC_METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn batch_with_a_notification_answers_only_the_request_with_an_id() {
        let (status, resp) = call(
            test_router(),
            json!([
                { "jsonrpc": "2.0", "id": 1, "method": "ping" },
                { "jsonrpc": "2.0", "method": "notifications/initialized" }
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let responses = resp.as_array().expect("batch answers with an array");
        assert_eq!(responses.len(), 1, "the notification must get no response: {resp}");
        assert_eq!(responses[0]["id"], 1);
    }

    #[tokio::test]
    async fn a_batch_of_only_notifications_answers_204() {
        let (status, _resp) = call(test_router(), json!([{ "jsonrpc": "2.0", "method": "notifications/initialized" }])).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn empty_batch_is_invalid_params() {
        let (_, resp) = call(test_router(), json!([])).await;
        assert_eq!(resp["error"]["code"], JSONRPC_INVALID_PARAMS);
    }

    #[tokio::test]
    async fn malformed_body_is_parse_error() {
        let (_, resp) = call_raw(test_router(), b"{not json").await;
        assert_eq!(resp["error"]["code"], JSONRPC_PARSE_ERROR);
    }

    #[tokio::test]
    async fn oversized_result_is_capped_with_a_cut_line() {
        let router = McpServer::<()>::new("cap-test", "0.0.0")
            .max_result_bytes(64)
            .tool(Tool::new("big", "Returns a large text body.", json!({ "type": "object", "properties": {} })), |_, _, _| async move {
                ToolOutcome::ok(ToolBody::Text("a".repeat(500)))
            })
            .into_router()
            .with_state(());

        let (_, resp) = call(
            router,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "big", "arguments": {} } }),
        )
        .await;
        let text = resp["result"]["content"][0]["text"].as_str().expect("text is a string");
        assert!(text.len() <= 64, "capped text must fit the budget: {} bytes: {text:?}", text.len());
        assert!(text.contains("cut "), "{text}");
        assert!(text.ends_with(']'), "{text}");
    }

    #[tokio::test]
    async fn delete_always_answers_204() {
        let router = test_router();
        let request = Request::builder().method("DELETE").uri("/mcp").body(Body::empty()).expect("test request builds");
        let response = router.oneshot(request).await.expect("router never returns Err");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[test]
    #[should_panic(expected = "exceeds the 300-byte cap")]
    fn instructions_over_300_bytes_is_refused() {
        let _ = McpServer::<()>::new("panic-test", "0.0.0").instructions("x".repeat(301));
    }

    #[test]
    fn instructions_at_300_bytes_is_accepted() {
        let server = McpServer::<()>::new("ok-test", "0.0.0").instructions("x".repeat(300));
        assert!(server.check_budget(Budget::WORKSPACE).is_ok());
    }

    #[test]
    fn check_budget_passes_within_the_workspace_law() {
        let server = test_router_builder();
        assert!(server.check_budget(Budget::WORKSPACE).is_ok());
    }

    #[test]
    fn check_budget_names_the_tool_count_offender_when_over() {
        let mut server = McpServer::<()>::new("over-budget", "0.0.0");
        for i in 0..9 {
            server = server.tool(Tool::new(format!("tool_{i}"), "A trivial tool.", json!({ "type": "object", "properties": {} })), |_, _, _| async move {
                ToolOutcome::ok(json!({}))
            });
        }
        let report = server.check_budget(Budget::WORKSPACE).expect_err("9 tools must exceed max_tools: 8");
        assert!(
            report.offenders.iter().any(|o| o.what == "tool count" && o.actual == 9),
            "report must name the tool-count offender: {report}"
        );
    }

    #[test]
    fn check_budget_names_an_oversized_tool_by_name() {
        let server = McpServer::<()>::new("oversized-tool", "0.0.0").tool(
            Tool::new("huge_tool", "x".repeat(2000), json!({ "type": "object", "properties": {} })),
            |_, _, _| async move { ToolOutcome::ok(json!({})) },
        );
        let report = server.check_budget(Budget::WORKSPACE).expect_err("a 2000-byte description must exceed max_tool_bytes: 1536");
        assert!(
            report.offenders.iter().any(|o| o.what.contains("huge_tool")),
            "report must name the offending tool: {report}"
        );
    }

    /// A fresh, unconsumed [`McpServer`] with the same two tools
    /// [`test_router`] serves, for tests that need to call a builder method
    /// (like [`McpServer::check_budget`]) instead of dispatching through
    /// the HTTP door.
    fn test_router_builder() -> McpServer<()> {
        McpServer::<()>::new("mcp-service4agent-test", "0.0.0")
            .tool(
                Tool::new(
                    "echo",
                    "Echoes its text argument back.",
                    json!({ "type": "object", "required": ["text"], "properties": { "text": { "type": "string" } } }),
                ),
                |_state: (), _ctx: CallContext, args: Value| async move {
                    let text = args.get("text").and_then(Value::as_str).unwrap_or_default().to_string();
                    ToolOutcome::ok(json!({ "echo": text }))
                },
            )
            .tool(
                Tool::new("boom", "Always refuses.", json!({ "type": "object", "properties": {} })),
                |_state: (), _ctx: CallContext, _args: Value| async move {
                    ToolOutcome::invalid_argument("anything", "boom always refuses")
                },
            )
    }

    #[test]
    fn cap_text_leaves_short_text_untouched() {
        assert_eq!(cap_text("hello", 100), "hello");
    }

    #[test]
    fn cap_text_cuts_on_a_char_boundary_and_names_the_cut() {
        let text = "é".repeat(50); // 2 bytes per char — forces a boundary decision
        let capped = cap_text(&text, 40);
        assert!(capped.len() <= 40, "{} bytes: {capped:?}", capped.len());
        assert!(capped.contains("cut "));
        assert!(capped.is_char_boundary(capped.len()), "must end on a char boundary: {capped:?}");
    }

    #[test]
    fn cap_text_never_panics_when_the_cut_line_alone_exceeds_max_bytes() {
        // Degenerate config (max_bytes smaller than the note itself): the
        // note is still appended in full rather than truncated further —
        // documented in `cap_text`'s own doc as the one case this helper
        // does not guarantee fitting the cap.
        let capped = cap_text(&"a".repeat(100), 5);
        assert!(capped.contains("cut 100 bytes"), "{capped:?}");
    }
}
