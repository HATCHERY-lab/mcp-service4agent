//! A route-recording wrapper around `axum::Router`.
//!
//! axum exposes no public way to enumerate the routes of a built `Router` —
//! there is no iterator over it. [`DocRouter`] works around that by
//! recording `(method, path, description)` as routes are added, the same
//! moment they are handed to the underlying `axum::Router`. [`into_parts`]
//! then returns both the real `Router` and the recorded [`Endpoint`] list —
//! everything a host needs to describe its own routes truthfully, because
//! the list was built from the same calls that built the router, not
//! retyped by hand into a separate file that can drift out of sync with it.
//!
//! The description is a required argument of every route-adding method
//! (`impl Into<RouteDoc>`, not `Option<RouteDoc>`) — a route added without
//! one is a compile error, not a manifest that silently ships with a gap.
//!
//! [`into_parts`]: DocRouter::into_parts
//!
//! ```no_run
//! use mcp_service4agent::router::{DocRouter, RouteDoc};
//!
//! async fn health() -> &'static str { "ok" }
//! async fn status() -> &'static str { "{}" }
//!
//! # async fn build() {
//! let api: DocRouter<()> = DocRouter::new()
//!     .get("/status", status, RouteDoc::bearer("Fleet status snapshot."));
//!
//! let (_router, endpoints) = DocRouter::new()
//!     .get("/health", health, "Liveness probe. No auth.")
//!     .nest("/api", api)
//!     .into_parts();
//! assert_eq!(endpoints.len(), 2);
//! # }
//! ```

use axum::Router;
use axum::extract::Request;
use axum::handler::Handler;
use axum::response::IntoResponse;
use axum::routing::{self, Route};
use serde::{Deserialize, Serialize};

/// The auth class of one route, as recorded in [`Endpoint::auth`] —
/// `none | bearer | session | api_key`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Auth {
    None,
    Bearer,
    Session,
    ApiKey,
}

impl Auth {
    fn wire(self) -> &'static str {
        match self {
            Auth::None => "none",
            Auth::Bearer => "bearer",
            Auth::Session => "session",
            Auth::ApiKey => "api_key",
        }
    }
}

/// What a route-adding call on [`DocRouter`] must supply: a description,
/// plus the same optional fields [`Endpoint`] carries. Construct with
/// [`RouteDoc::new`] (auth `none`) or [`RouteDoc::bearer`]/[`RouteDoc::session`]/
/// [`RouteDoc::api_key`], or pass a bare `&str`/`String` — both convert via
/// `Into<RouteDoc>` to a `none`-auth doc, the common case for a health check.
#[derive(Clone, Debug)]
pub struct RouteDoc {
    pub description: String,
    pub auth: Auth,
    pub key_ref: Option<String>,
    pub public: bool,
    pub body_schema: Option<String>,
}

impl RouteDoc {
    pub fn new(description: impl Into<String>) -> Self {
        Self { description: description.into(), auth: Auth::None, key_ref: None, public: false, body_schema: None }
    }

    pub fn bearer(description: impl Into<String>) -> Self {
        Self { auth: Auth::Bearer, ..Self::new(description) }
    }

    pub fn session(description: impl Into<String>) -> Self {
        Self { auth: Auth::Session, ..Self::new(description) }
    }

    pub fn api_key(description: impl Into<String>) -> Self {
        Self { auth: Auth::ApiKey, ..Self::new(description) }
    }

    pub fn public(mut self) -> Self {
        self.public = true;
        self
    }

    pub fn key_ref(mut self, key_ref: impl Into<String>) -> Self {
        self.key_ref = Some(key_ref.into());
        self
    }

    pub fn body_schema(mut self, schema: impl Into<String>) -> Self {
        self.body_schema = Some(schema.into());
        self
    }
}

impl From<&str> for RouteDoc {
    fn from(description: &str) -> Self {
        RouteDoc::new(description)
    }
}

impl From<String> for RouteDoc {
    fn from(description: String) -> Self {
        RouteDoc::new(description)
    }
}

/// One route as recorded by [`DocRouter`] (or returned by
/// `McpServer::route_docs` — see [`crate::mcp`]): `(method, path,
/// description)` plus the same optional metadata [`RouteDoc`] carries.
///
/// Field-compatible with `crate::manifest::Endpoint` under the `nemo`
/// feature (see that module's `From` conversion), but this type itself has
/// no dependency on `nemo` — it lives in core so `DocRouter` and the MCP
/// server need nothing beyond axum + serde to compile.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    pub method: String,
    pub path: String,
    pub description: String,
    /// none | bearer | session | api_key
    pub auth: String,
    pub key_ref: Option<String>,
    pub public: bool,
    pub body_schema: Option<String>,
}

/// Join a `nest` prefix with a recorded route path, matching how
/// `axum::Router::nest` composes the paths it actually serves (prefix,
/// then the nested path with its own leading slash) — so the recorded
/// [`Endpoint::path`] always matches what a client can actually reach.
fn join_path(prefix: &str, path: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{path}")
    }
}

/// A route-recording wrapper around `axum::Router<S>`. See the module doc.
pub struct DocRouter<S = ()> {
    router: Router<S>,
    endpoints: Vec<Endpoint>,
}

impl<S> Default for DocRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> DocRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self { router: Router::new(), endpoints: Vec::new() }
    }

    fn record(&mut self, method: &'static str, path: &str, doc: RouteDoc) {
        self.endpoints.push(Endpoint {
            method: method.to_string(),
            path: path.to_string(),
            description: doc.description,
            auth: doc.auth.wire().to_string(),
            key_ref: doc.key_ref,
            public: doc.public,
            body_schema: doc.body_schema,
        });
    }

    /// Mount a `GET` handler at `path`. Calling this again for a `path`
    /// already carrying another method (e.g. `.put(path, ..).delete(path,
    /// ..)`) merges under the hood exactly like two plain
    /// `axum::Router::route` calls on the same path do — axum merges method
    /// routers registered separately at the same path, it does not require
    /// the `get(h).post(h2)` chained form.
    pub fn get<H, T>(mut self, path: &str, handler: H, doc: impl Into<RouteDoc>) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.record("GET", path, doc.into());
        self.router = self.router.route(path, routing::get(handler));
        self
    }

    pub fn post<H, T>(mut self, path: &str, handler: H, doc: impl Into<RouteDoc>) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.record("POST", path, doc.into());
        self.router = self.router.route(path, routing::post(handler));
        self
    }

    pub fn put<H, T>(mut self, path: &str, handler: H, doc: impl Into<RouteDoc>) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.record("PUT", path, doc.into());
        self.router = self.router.route(path, routing::put(handler));
        self
    }

    pub fn patch<H, T>(mut self, path: &str, handler: H, doc: impl Into<RouteDoc>) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.record("PATCH", path, doc.into());
        self.router = self.router.route(path, routing::patch(handler));
        self
    }

    pub fn delete<H, T>(mut self, path: &str, handler: H, doc: impl Into<RouteDoc>) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.record("DELETE", path, doc.into());
        self.router = self.router.route(path, routing::delete(handler));
        self
    }

    /// Nest `other` under `prefix`, the same way `axum::Router::nest` does —
    /// every recorded endpoint of `other` is re-recorded here with `prefix`
    /// joined onto its path (see [`join_path`]), so the list a caller gets
    /// from [`into_parts`](Self::into_parts) names the paths that are
    /// actually reachable through this router, not the pre-nest ones.
    pub fn nest(mut self, prefix: &str, other: DocRouter<S>) -> Self {
        self.router = self.router.nest(prefix, other.router);
        for mut ep in other.endpoints {
            ep.path = join_path(prefix, &ep.path);
            self.endpoints.push(ep);
        }
        self
    }

    /// Merge `other` at its own paths, unchanged — the `merge` counterpart
    /// of [`nest`](Self::nest).
    pub fn merge(mut self, other: DocRouter<S>) -> Self {
        self.router = self.router.merge(other.router);
        self.endpoints.extend(other.endpoints);
        self
    }

    /// Attach middleware to the whole router — passes through to
    /// `axum::Router::layer` unchanged (same bounds) and never touches the
    /// recorded endpoint list: a layer does not add or remove a route.
    pub fn layer<L>(mut self, layer: L) -> Self
    where
        L: tower_layer::Layer<Route> + Clone + Send + Sync + 'static,
        L::Service: tower_service::Service<Request> + Clone + Send + Sync + 'static,
        <L::Service as tower_service::Service<Request>>::Response: IntoResponse + 'static,
        <L::Service as tower_service::Service<Request>>::Error: Into<std::convert::Infallible> + 'static,
        <L::Service as tower_service::Service<Request>>::Future: Send + 'static,
    {
        self.router = self.router.layer(layer);
        self
    }

    /// Attach middleware to the matched routes ONLY — passes through to
    /// `axum::Router::route_layer` unchanged (same bounds), and like
    /// [`Self::layer`] never touches the recorded endpoint list.
    ///
    /// The difference from `layer` is the fallback: `layer` wraps it too,
    /// `route_layer` does not. For an auth guard that is the difference
    /// between an unmapped path answering 404 (`route_layer` — the path
    /// simply does not exist) and 401 (`layer` — the guard fires before the
    /// router finds out there is nothing there), which also tells an
    /// unauthenticated prober which paths are real. An auth guard belongs
    /// here, not on `layer`.
    pub fn route_layer<L>(mut self, layer: L) -> Self
    where
        L: tower_layer::Layer<Route> + Clone + Send + Sync + 'static,
        L::Service: tower_service::Service<Request> + Clone + Send + Sync + 'static,
        <L::Service as tower_service::Service<Request>>::Response: IntoResponse + 'static,
        <L::Service as tower_service::Service<Request>>::Error: Into<std::convert::Infallible> + 'static,
        <L::Service as tower_service::Service<Request>>::Future: Send + 'static,
    {
        self.router = self.router.route_layer(layer);
        self
    }

    /// Bake in application state — passes through to `axum::Router::with_state`
    /// unchanged. The recorded endpoint list is state-independent and carries
    /// over as-is.
    pub fn with_state<S2>(self, state: S) -> DocRouter<S2> {
        DocRouter { router: self.router.with_state(state), endpoints: self.endpoints }
    }

    /// Consume this wrapper, returning the real `axum::Router` to serve and
    /// the endpoint list recorded while it was built.
    pub fn into_parts(self) -> (Router<S>, Vec<Endpoint>) {
        (self.router, self.endpoints)
    }
}
