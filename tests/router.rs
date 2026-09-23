//! `DocRouter` must record exactly what it mounts: every recorded
//! `(method, path)` dispatches on the real router, `nest` rewrites recorded
//! paths the same way `axum::Router::nest` rewrites reachable ones, two
//! methods registered separately at the same path merge into one route
//! without a panic and without losing either recorded entry, and `layer`/
//! `with_state` pass through without perturbing the endpoint list.

use axum::body::{Body, to_bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Request, StatusCode};
use mcp_service4agent::router::{DocRouter, RouteDoc};
use std::sync::Arc;
use tower::ServiceExt;

async fn ok() -> &'static str {
    "ok"
}

async fn created() -> StatusCode {
    StatusCode::CREATED
}

#[tokio::test]
async fn recorded_endpoints_match_what_is_actually_mounted() {
    let (router, endpoints) = DocRouter::new()
        .get("/health", ok, "Liveness check.")
        .post("/items", created, RouteDoc::bearer("Create an item."))
        .into_parts();

    assert_eq!(endpoints.len(), 2);
    assert_eq!((endpoints[0].method.as_str(), endpoints[0].path.as_str()), ("GET", "/health"));
    assert_eq!(endpoints[0].auth, "none");
    assert_eq!((endpoints[1].method.as_str(), endpoints[1].path.as_str()), ("POST", "/items"));
    assert_eq!(endpoints[1].auth, "bearer");

    let resp = router
        .clone()
        .oneshot(Request::get("/health").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .oneshot(Request::post("/items").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn nest_prefixes_recorded_paths_to_match_the_mounted_router() {
    let api: DocRouter<()> = DocRouter::new()
        .get("/status", ok, "Status.")
        .put("/config", ok, RouteDoc::bearer("Update config."));

    let (router, endpoints) = DocRouter::new()
        .get("/health", ok, "Liveness.")
        .nest("/api", api)
        .into_parts();

    let mut paths: Vec<&str> = endpoints.iter().map(|e| e.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, vec!["/api/config", "/api/status", "/health"]);

    let resp = router
        .clone()
        .oneshot(Request::get("/api/status").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .oneshot(Request::put("/api/config").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Two methods registered separately (`.put(path, ..).delete(path, ..)`, not
/// axum's own `put(h).delete(h2)` chained `MethodRouter`) on the same path.
/// axum merges separate `.route()` calls at the same path; this proves it
/// holds through `DocRouter` too, and that the endpoint list keeps both
/// entries rather than one overwriting the other.
#[tokio::test]
async fn two_methods_on_the_same_path_merge_without_losing_either_entry() {
    let (router, endpoints) = DocRouter::new()
        .put("/thing/{id}", ok, RouteDoc::bearer("Replace the thing."))
        .delete("/thing/{id}", ok, RouteDoc::bearer("Remove the thing."))
        .into_parts();

    assert_eq!(endpoints.len(), 2, "one entry per method — no merge, no duplicate");
    let methods: Vec<&str> = endpoints.iter().map(|e| e.method.as_str()).collect();
    assert_eq!(methods, vec!["PUT", "DELETE"]);
    assert!(endpoints.iter().all(|e| e.path == "/thing/{id}"));

    let resp = router
        .clone()
        .oneshot(Request::put("/thing/7").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK, "PUT must still dispatch after DELETE was added");

    let resp = router
        .oneshot(Request::delete("/thing/7").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK, "DELETE must dispatch on the merged route");
}

#[tokio::test]
async fn merge_combines_endpoints_and_routes_from_both_routers() {
    let a: DocRouter<()> = DocRouter::new().get("/a", ok, "A.");
    let b: DocRouter<()> = DocRouter::new().get("/b", ok, "B.");

    let (router, endpoints) = a.merge(b).into_parts();

    let mut paths: Vec<&str> = endpoints.iter().map(|e| e.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, vec!["/a", "/b"]);

    for path in ["/a", "/b"] {
        let resp = router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("build request"))
            .await
            .expect("Router::call is infallible");
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
    }
}

#[derive(Clone)]
struct Label(Arc<str>);

async fn show_label(State(label): State<Label>) -> String {
    label.0.to_string()
}

/// `layer` and `with_state` pass through to the real `axum::Router`
/// unchanged and never touch the recorded endpoint list.
#[tokio::test]
async fn layer_and_state_pass_through_without_perturbing_recorded_endpoints() {
    let (router, endpoints) = DocRouter::new()
        .get("/label", show_label, "Show the configured label.")
        .layer(DefaultBodyLimit::max(1024))
        .with_state(Label(Arc::from("hello")))
        .into_parts();

    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].path, "/label");

    let resp = router
        .oneshot(Request::get("/label").body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.expect("read response body");
    assert_eq!(&body[..], b"hello");
}

/// A guard that refuses every request — stands in for a bearer check.
async fn deny_all(
    _req: axum::extract::Request,
    _next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    Err(StatusCode::UNAUTHORIZED)
}

async fn status_of(router: axum::Router, path: &str) -> StatusCode {
    router
        .oneshot(Request::get(path).body(Body::empty()).expect("build request"))
        .await
        .expect("Router::call is infallible")
        .status()
}

/// `route_layer` guards the mapped routes and leaves the fallback alone: a
/// real route answers the guard's 401, an unmapped path answers 404. With
/// `layer` the same guard also wraps the fallback, so the unmapped path
/// answers 401 too — which is both a behaviour change and a way for an
/// unauthenticated caller to tell real paths from absent ones.
#[tokio::test]
async fn route_layer_guards_routes_but_not_the_fallback_unlike_layer() {
    let (guarded, endpoints) = DocRouter::new()
        .get("/secret", ok, RouteDoc::bearer("Guarded route."))
        .route_layer(axum::middleware::from_fn(deny_all))
        .into_parts();
    assert_eq!(endpoints.len(), 1, "route_layer must not add or drop a recorded endpoint");
    assert_eq!(status_of(guarded.clone(), "/secret").await, StatusCode::UNAUTHORIZED);
    assert_eq!(status_of(guarded, "/nope").await, StatusCode::NOT_FOUND);

    let (wrapped, _) = DocRouter::new()
        .get("/secret", ok, RouteDoc::bearer("Guarded route."))
        .layer(axum::middleware::from_fn(deny_all))
        .into_parts();
    assert_eq!(status_of(wrapped.clone(), "/secret").await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        status_of(wrapped, "/nope").await,
        StatusCode::UNAUTHORIZED,
        "layer wraps the fallback — the reason route_layer exists"
    );
}
