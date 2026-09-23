//! Push registration against d-watchdog's `PUT /registry/:name`.
//!
//! **Contract: this module can never fail a service's startup or hang its
//! event loop.** Every fallible path — no credentials, the watchdog down,
//! a non-2xx response — is a [`RegisterOutcome`] variant, never a
//! `Result` the caller has to propagate or an `unwrap`/`panic!` this module
//! could hit. A watchdog that is unreachable is not a reason a daemon
//! should fail to boot.
//!
//! No filesystem search for a key. [`RegisterConfig::from_env`] reads
//! environment variables only. A process that needs a key from a file must
//! export it into its own environment before this runs; that is the
//! process's own startup concern, not this module's.
//!
//! ```no_run
//! use mcp_service4agent::manifest::{ServiceManifest, ServiceMeta};
//! use mcp_service4agent::register;
//!
//! # async fn example() {
//! let manifest = ServiceManifest {
//!     service: ServiceMeta {
//!         name: "my-service".into(),
//!         kind: "http_api".into(),
//!         display_name: None,
//!         description: None,
//!         version: None,
//!         key_ref: None,
//!     },
//!     links: Default::default(),
//!     endpoints: Vec::new(),
//!     manage_actions: Vec::new(),
//!     dependencies: Default::default(),
//!     health: None,
//! };
//!
//! // One call at startup...
//! let _ = register::register_once(&manifest).await;
//! // ...or a background task that registers now and reasserts on a timer.
//! let _handle = register::spawn(manifest, register::DEFAULT_REASSERT_INTERVAL);
//! # }
//! ```

use crate::manifest::ServiceManifest;
use std::time::Duration;

/// `D_WATCHDOG_URL` default — the watchdog's fixed local address.
pub const DEFAULT_WATCHDOG_URL: &str = "http://127.0.0.1:18150";

/// Default reassertion interval for [`spawn`]. A well-behaved watchdog
/// marks a push registration stale only after a threshold on the order of
/// an hour without a fresh `PUT`; 5 minutes gives an order of magnitude of
/// headroom under that, so a healthy, periodically reasserting service is
/// never seen stale between beats even if one tick is delayed.
pub const DEFAULT_REASSERT_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Retry delay used after a [`RegisterOutcome::WatchdogUnreachable`] attempt,
/// in place of the full reassertion `interval` — see [`next_delay`]. A
/// restarted watchdog typically takes on the order of a minute to start
/// serving again; retrying this soon gives several attempts a chance to
/// land inside that window, instead of leaving the service stale for a
/// full `interval` (5 minutes under [`DEFAULT_REASSERT_INTERVAL`]) after the
/// first re-assert happened to land while the watchdog was still coming up.
const UNREACHABLE_RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// Whole-request timeout (connect + send + response) for a single `PUT
/// /registry/:name`. Short on purpose: a hung watchdog must not let calls
/// pile up on the reassertion timer — see [`spawn`].
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// Cap on how much of a refusal body is kept in [`RegisterOutcome::Refused`]
/// — enough to see a real error message, not enough to log an entire HTML
/// error page every reassertion.
const REFUSAL_BODY_LIMIT: usize = 500;

/// Where to reach the watchdog and what to authenticate with, resolved once
/// (typically via [`RegisterConfig::from_env`]) and reused across calls.
#[derive(Clone, Debug)]
pub struct RegisterConfig {
    pub watchdog_url: String,
    /// `None` when neither credential env var is set — [`register_once_with`]
    /// turns that into [`RegisterOutcome::NoCredentials`] without sending
    /// anything.
    pub token: Option<String>,
}

impl RegisterConfig {
    /// Resolve from the environment: `D_WATCHDOG_URL` (default
    /// [`DEFAULT_WATCHDOG_URL`]), and a bearer read in this order —
    /// `D_WATCHDOG_REGISTER_TOKEN` first (a narrower token scoped to
    /// self-registration only), then `D_WATCHDOG_KEY` (the general
    /// mutating-API key every other caller uses). An empty value for
    /// either variable is treated as unset.
    ///
    /// Never touches the filesystem — see the module doc.
    pub fn from_env() -> Self {
        let watchdog_url = non_empty_env("D_WATCHDOG_URL").unwrap_or_else(|| DEFAULT_WATCHDOG_URL.to_string());
        let token = non_empty_env("D_WATCHDOG_REGISTER_TOKEN").or_else(|| non_empty_env("D_WATCHDOG_KEY"));
        Self { watchdog_url, token }
    }
}

fn non_empty_env(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|v| !v.is_empty())
}

/// The result of one registration attempt. Never an `Err` a caller has to
/// handle — see the module doc's startup-never-fails contract.
#[derive(Debug)]
pub enum RegisterOutcome {
    /// The watchdog accepted the manifest (2xx).
    Registered,
    /// Neither `D_WATCHDOG_REGISTER_TOKEN` nor `D_WATCHDOG_KEY` is set in
    /// the environment. Nothing was sent.
    NoCredentials,
    /// The HTTP client itself failed to build (e.g. TLS backend init
    /// failure). Distinct from `WatchdogUnreachable` because nothing was
    /// even attempted over the network.
    ClientBuildFailed { error: String },
    /// The request could not complete within `REQUEST_TIMEOUT` — the
    /// watchdog is down, the wrong port, unreachable, or simply slow.
    WatchdogUnreachable { error: String },
    /// The watchdog answered with a non-2xx status: bad service name
    /// (400), unknown service (404), wrong bearer, or a server error.
    /// `body` is truncated to [`REFUSAL_BODY_LIMIT`] bytes at a UTF-8
    /// boundary.
    Refused { status: u16, body: String },
}

impl RegisterOutcome {
    /// True only for [`RegisterOutcome::Registered`].
    pub fn is_registered(&self) -> bool {
        matches!(self, RegisterOutcome::Registered)
    }

    /// A short, stable label used to detect a state *change* for
    /// change-only logging in [`spawn`] — not for display to a human.
    fn state_label(&self) -> &'static str {
        match self {
            RegisterOutcome::Registered => "registered",
            RegisterOutcome::NoCredentials => "no_credentials",
            RegisterOutcome::ClientBuildFailed { .. } => "client_build_failed",
            RegisterOutcome::WatchdogUnreachable { .. } => "unreachable",
            RegisterOutcome::Refused { .. } => "refused",
        }
    }
}

/// Truncate `s` to at most `max` bytes, respecting UTF-8 char boundaries.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Register `manifest` once, resolving the watchdog address and credentials
/// from the environment via [`RegisterConfig::from_env`]. See
/// [`register_once_with`] for the testable core and full outcome semantics.
pub async fn register_once(manifest: &ServiceManifest) -> RegisterOutcome {
    register_once_with(manifest, &RegisterConfig::from_env()).await
}

/// Register `manifest` once against `config`. Never panics and never
/// returns an `Err` — every failure mode is a [`RegisterOutcome`] variant.
pub async fn register_once_with(manifest: &ServiceManifest, config: &RegisterConfig) -> RegisterOutcome {
    let Some(token) = config.token.as_ref() else {
        return RegisterOutcome::NoCredentials;
    };

    let client = match reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => return RegisterOutcome::ClientBuildFailed { error: e.to_string() },
    };

    let url = format!(
        "{}/registry/{}",
        config.watchdog_url.trim_end_matches('/'),
        manifest.service.name
    );

    match client.put(&url).bearer_auth(token).json(manifest).send().await {
        Ok(resp) if resp.status().is_success() => RegisterOutcome::Registered,
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            RegisterOutcome::Refused { status, body: truncate(&body, REFUSAL_BODY_LIMIT) }
        }
        Err(e) => RegisterOutcome::WatchdogUnreachable { error: e.to_string() },
    }
}

/// Log an outcome at the appropriate level, one line per call — [`spawn`]
/// calls this only when the state actually changed since the previous tick
/// (see `state_label`), so a healthy service logs once and then stays
/// quiet.
fn log_outcome(service: &str, outcome: &RegisterOutcome) {
    match outcome {
        RegisterOutcome::Registered => {
            tracing::info!(service, "registered with d-watchdog");
        }
        RegisterOutcome::NoCredentials => {
            tracing::warn!(
                service,
                "no D_WATCHDOG_REGISTER_TOKEN or D_WATCHDOG_KEY in the environment — not registering"
            );
        }
        RegisterOutcome::ClientBuildFailed { error } => {
            tracing::warn!(service, error, "failed to build the watchdog HTTP client");
        }
        RegisterOutcome::WatchdogUnreachable { error } => {
            tracing::warn!(service, error, "d-watchdog unreachable — will keep retrying");
        }
        RegisterOutcome::Refused { status, body } => {
            tracing::warn!(service, status, body, "d-watchdog refused the registration");
        }
    }
}

/// How long [`reassert_loop`] waits before its next attempt, given the
/// outcome of the one it just made. A pure function of the outcome and the
/// caller's configured `interval`, kept separate from the loop itself so the
/// scheduling decision is unit-testable without a clock or a network.
///
/// Only [`RegisterOutcome::WatchdogUnreachable`] shortens the wait, to
/// [`UNREACHABLE_RETRY_INTERVAL`] — see that constant's doc for why. Every
/// other outcome, including [`RegisterOutcome::NoCredentials`] (retrying
/// sooner cannot help — nothing will appear in the environment between now
/// and the next tick), keeps the caller's own `interval`.
fn next_delay(outcome: &RegisterOutcome, interval: Duration) -> Duration {
    match outcome {
        RegisterOutcome::WatchdogUnreachable { .. } => UNREACHABLE_RETRY_INTERVAL,
        RegisterOutcome::Registered
        | RegisterOutcome::NoCredentials
        | RegisterOutcome::ClientBuildFailed { .. }
        | RegisterOutcome::Refused { .. } => interval,
    }
}

async fn reassert_loop(manifest: ServiceManifest, interval: Duration, config: RegisterConfig) {
    let mut last_state: Option<&'static str> = None;
    loop {
        let outcome = register_once_with(&manifest, &config).await;
        let state = outcome.state_label();
        if last_state != Some(state) {
            log_outcome(&manifest.service.name, &outcome);
            last_state = Some(state);
        }
        tokio::time::sleep(next_delay(&outcome, interval)).await;
    }
}

/// Register `manifest` at startup, then reassert every `interval` for as
/// long as the returned task runs — resolves the watchdog address and
/// credentials from the environment once, via [`RegisterConfig::from_env`].
/// See [`spawn_with`] for a variant that takes an explicit [`RegisterConfig`]
/// (used by tests, or a caller that already resolved one).
///
/// `interval` should sit well under d-watchdog's own staleness threshold —
/// [`DEFAULT_REASSERT_INTERVAL`] is a reasonable default.
///
/// An attempt that finds the watchdog unreachable is retried after
/// [`UNREACHABLE_RETRY_INTERVAL`] instead of the full `interval` — see
/// [`next_delay`]. Every other outcome, including a refusal or missing
/// credentials, keeps `interval`.
///
/// State changes only are logged (see [`log_outcome`]); a watchdog that is
/// permanently down or an unset credential does not spam a log line every
/// tick.
pub fn spawn(manifest: ServiceManifest, interval: Duration) -> tokio::task::JoinHandle<()> {
    spawn_with(manifest, interval, RegisterConfig::from_env())
}

/// [`spawn`] with an explicit [`RegisterConfig`] instead of reading the
/// environment.
pub fn spawn_with(
    manifest: ServiceManifest,
    interval: Duration,
    config: RegisterConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(reassert_loop(manifest, interval, config))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one outcome that gets the fast retry, regardless of `interval`.
    #[test]
    fn unreachable_watchdog_gets_the_fast_retry() {
        let outcome = RegisterOutcome::WatchdogUnreachable { error: "connection refused".to_string() };
        assert_eq!(next_delay(&outcome, DEFAULT_REASSERT_INTERVAL), UNREACHABLE_RETRY_INTERVAL);
        assert_eq!(next_delay(&outcome, Duration::from_secs(1)), UNREACHABLE_RETRY_INTERVAL);
    }

    #[test]
    fn registered_keeps_the_configured_interval() {
        let interval = Duration::from_secs(42);
        assert_eq!(next_delay(&RegisterOutcome::Registered, interval), interval);
    }

    #[test]
    fn refused_keeps_the_configured_interval() {
        let interval = Duration::from_secs(42);
        let outcome = RegisterOutcome::Refused { status: 401, body: "bad bearer".to_string() };
        assert_eq!(next_delay(&outcome, interval), interval);
    }

    /// [`RegisterOutcome::NoCredentials`] stays as it is today: retrying
    /// sooner cannot help, since nothing will appear in the environment
    /// between now and the next tick.
    #[test]
    fn no_credentials_keeps_the_configured_interval() {
        let interval = Duration::from_secs(42);
        assert_eq!(next_delay(&RegisterOutcome::NoCredentials, interval), interval);
    }

    #[test]
    fn client_build_failed_keeps_the_configured_interval() {
        let interval = Duration::from_secs(42);
        let outcome = RegisterOutcome::ClientBuildFailed { error: "tls init failed".to_string() };
        assert_eq!(next_delay(&outcome, interval), interval);
    }
}
