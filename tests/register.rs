#![cfg(feature = "nemo")]
//! `register_once_with`/`spawn_with` against a local fake watchdog on an
//! ephemeral port — every branch of `RegisterOutcome` reachable from a real
//! HTTP exchange (2xx, a refusal status, connection failure), plus the
//! filesystem-free, env-only credential contract (`RegisterConfig`
//! constructed directly here — no env var is read by these tests, `from_env`
//! itself is exercised separately by construction alone, not by mutating
//! process-wide env vars that would race across parallel tests).

use axum::Router;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::put;
use mcp_service4agent::manifest::{ServiceManifest, ServiceMeta};
use mcp_service4agent::register::{self, RegisterConfig, RegisterOutcome};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

fn sample_manifest(name: &str) -> ServiceManifest {
    ServiceManifest {
        service: ServiceMeta {
            name: name.to_string(),
            kind: "service".to_string(),
            display_name: None,
            description: None,
            version: None,
            key_ref: None,
        },
        links: Default::default(),
        endpoints: Vec::new(),
        manage_actions: Vec::new(),
        dependencies: Default::default(),
        health: None,
    }
}

/// Spawn a fake watchdog that always answers `status`/`body` on `PUT
/// /registry/{name}`, counting calls and recording the last `Authorization`
/// header it saw. The server task is detached — it dies with the test
/// process, same as any other background task a test leaves running.
async fn spawn_fake_watchdog(
    status: StatusCode,
    body: &'static str,
) -> (String, Arc<AtomicUsize>, Arc<Mutex<Option<String>>>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let last_auth = Arc::new(Mutex::new(None));

    let calls_h = calls.clone();
    let last_auth_h = last_auth.clone();
    let app: Router = Router::new().route(
        "/registry/{name}",
        put(move |Path(_name): Path<String>, headers: HeaderMap| {
            let calls = calls_h.clone();
            let last_auth = last_auth_h.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
                *last_auth.lock().unwrap_or_else(|p| p.into_inner()) = auth;
                (status, body)
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{addr}"), calls, last_auth)
}

#[tokio::test]
async fn registered_on_2xx_and_sends_the_bearer_token() {
    let (watchdog_url, calls, last_auth) = spawn_fake_watchdog(StatusCode::OK, "").await;
    let config = RegisterConfig { watchdog_url, token: Some("s3cr3t".to_string()) };
    let manifest = sample_manifest("demo-service");

    let outcome = register::register_once_with(&manifest, &config).await;

    assert!(outcome.is_registered(), "{outcome:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let auth = last_auth.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert_eq!(auth.as_deref(), Some("Bearer s3cr3t"));
}

#[tokio::test]
async fn refused_reports_status_on_401() {
    let (watchdog_url, _calls, _auth) = spawn_fake_watchdog(StatusCode::UNAUTHORIZED, "bad bearer").await;
    let config = RegisterConfig { watchdog_url, token: Some("wrong".to_string()) };
    let manifest = sample_manifest("demo-service");

    let outcome = register::register_once_with(&manifest, &config).await;

    match outcome {
        RegisterOutcome::Refused { status, body } => {
            assert_eq!(status, 401);
            assert_eq!(body, "bad bearer");
        }
        other => panic!("expected Refused, got {other:?}"),
    }
}

#[tokio::test]
async fn refused_reports_status_on_404() {
    let (watchdog_url, _calls, _auth) =
        spawn_fake_watchdog(StatusCode::NOT_FOUND, "service not known to this watchdog").await;
    let config = RegisterConfig { watchdog_url, token: Some("s3cr3t".to_string()) };
    let manifest = sample_manifest("demo-service");

    let outcome = register::register_once_with(&manifest, &config).await;

    match outcome {
        RegisterOutcome::Refused { status, body } => {
            assert_eq!(status, 404);
            assert_eq!(body, "service not known to this watchdog");
        }
        other => panic!("expected Refused, got {other:?}"),
    }
}

/// Nothing listens on this port at all — the client must report this as a
/// value, not hang or panic.
#[tokio::test]
async fn unreachable_watchdog_is_reported_not_panicked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener); // free the port; nothing answers on it now

    let config = RegisterConfig { watchdog_url: format!("http://{addr}"), token: Some("s3cr3t".into()) };
    let manifest = sample_manifest("demo-service");

    let outcome = register::register_once_with(&manifest, &config).await;

    match outcome {
        RegisterOutcome::WatchdogUnreachable { .. } => {}
        other => panic!("expected WatchdogUnreachable, got {other:?}"),
    }
}

/// No `D_WATCHDOG_REGISTER_TOKEN`/`D_WATCHDOG_KEY` resolved — `token: None`
/// must short-circuit before any network attempt, so the (deliberately
/// bogus, privileged) watchdog_url below is never actually dialed.
#[tokio::test]
async fn no_credentials_sends_nothing() {
    let config = RegisterConfig { watchdog_url: "http://127.0.0.1:1".to_string(), token: None };
    let manifest = sample_manifest("demo-service");

    let outcome = register::register_once_with(&manifest, &config).await;

    assert!(matches!(outcome, RegisterOutcome::NoCredentials), "{outcome:?}");
}

/// `spawn_with` must register at startup and keep reasserting on the given
/// interval for as long as it runs.
#[tokio::test]
async fn spawn_reasserts_on_the_configured_interval() {
    let (watchdog_url, calls, _auth) = spawn_fake_watchdog(StatusCode::OK, "").await;
    let config = RegisterConfig { watchdog_url, token: Some("s3cr3t".into()) };
    let manifest = sample_manifest("demo-service");

    let handle = register::spawn_with(manifest, Duration::from_millis(20), config);
    tokio::time::sleep(Duration::from_millis(130)).await;
    handle.abort();

    let seen = calls.load(Ordering::SeqCst);
    assert!(seen >= 3, "expected at least 3 registrations in ~130ms at a 20ms interval, saw {seen}");
}

/// `RegisterConfig::from_env` falls back to `register::DEFAULT_WATCHDOG_URL`
/// when `D_WATCHDOG_URL` is unset. Every other test in this file constructs
/// `RegisterConfig` directly instead of going through `from_env`, precisely
/// to avoid mutating process-wide env vars that would race across tests
/// running in parallel in the same process; this is the one test that reads
/// (never writes) the real environment, so it only asserts the fallback for
/// the case where the variable happens to be unset rather than asserting a
/// value for it either way.
#[tokio::test]
async fn from_env_defaults_to_the_documented_watchdog_url_when_unset() {
    let config = RegisterConfig::from_env();
    if std::env::var("D_WATCHDOG_URL").is_err() {
        assert_eq!(config.watchdog_url, register::DEFAULT_WATCHDOG_URL);
    }
}
