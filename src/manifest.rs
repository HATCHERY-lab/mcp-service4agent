//! Wire types for d-watchdog's service registry (`PUT /registry/:name`).
//!
//! These mirror the manifest shape d-watchdog itself expects field-for-field
//! (`ServiceManifest`, `ServiceMeta`, `Links`, `Endpoint`, `ParamSpec`,
//! `ManageAction`, `Dependencies`, `HealthInfo`) — same field names, same
//! optionality, same `serde` defaults. d-watchdog is the source of truth:
//! if its registry shape adds, renames, or re-defaults a field, this module
//! must follow in the same commit, or `PUT /registry/:name` will reject the
//! body or silently drop a field.
//!
//! One field is deliberately not mirrored: d-watchdog's own manifest type
//! carries a local `manifest_path` marked `#[serde(skip)]` — it never
//! appears on the wire, so there is nothing to mirror. It exists on that
//! side to remember whether a manifest was loaded from disk or pushed; a
//! service asserting its own manifest has no such history to record.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The manifest a service asserts about itself via `PUT /registry/:name`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceManifest {
    pub service: ServiceMeta,
    #[serde(default)]
    pub links: Links,
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,
    #[serde(default)]
    pub manage_actions: Vec<ManageAction>,
    #[serde(default)]
    pub dependencies: Dependencies,
    #[serde(default)]
    pub health: Option<HealthInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceMeta {
    pub name: String,
    /// website | http_api | tunnel | daemon | service
    pub kind: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    /// Where an authorised caller finds the key. Either `<path>:<VAR>` — a
    /// `VAR=value` line inside that file — or `<path>` alone, naming a file
    /// whose entire contents ARE the key with no `VAR=` wrapper.
    /// Documentation only — the watchdog never reads it. Service-wide
    /// default, used when an [`Endpoint`] or [`ManageAction`] omits its own.
    #[serde(default)]
    pub key_ref: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Links {
    #[serde(default)]
    pub public_url: Option<String>,
    #[serde(default)]
    pub local_url: Option<String>,
    #[serde(default)]
    pub admin_url: Option<String>,
    #[serde(default)]
    pub docs: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    pub method: String,
    pub path: String,
    pub description: String,
    /// none | bearer | session | api_key
    #[serde(default = "default_auth")]
    pub auth: String,
    /// Same `<path>:<VAR>` / `<path>` convention as [`ServiceMeta::key_ref`].
    #[serde(default)]
    pub key_ref: Option<String>,
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub body_schema: Option<String>,
}

impl From<crate::router::Endpoint> for Endpoint {
    /// The conversion from [`crate::router::DocRouter`]'s own recorded
    /// route shape into this module's wire type — a straight field copy,
    /// since the two are deliberately kept structurally identical.
    fn from(route: crate::router::Endpoint) -> Self {
        Self {
            method: route.method,
            path: route.path,
            description: route.description,
            auth: route.auth,
            key_ref: route.key_ref,
            public: route.public,
            body_schema: route.body_schema,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParamSpec {
    pub name: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub example: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManageAction {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    pub method: String,
    pub path: String,
    #[serde(default = "default_auth")]
    pub auth: String,
    /// Same `<path>:<VAR>` / `<path>` convention as [`ServiceMeta::key_ref`].
    #[serde(default)]
    pub key_ref: Option<String>,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    pub icon: Option<String>,
    /// Optional request body to send (raw string, typically JSON).
    #[serde(default)]
    pub body: Option<String>,
    /// Content-Type for the body. Defaults to "application/json" when body is present.
    #[serde(default)]
    pub content_type: Option<String>,
    /// Per-action HTTP timeout override in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Execution mode: "sync" (default) or "async".
    #[serde(default)]
    pub mode: Option<String>,
    /// Parameter schema for the docs endpoint.
    #[serde(default)]
    pub params_schema: Vec<ParamSpec>,
}

impl ManageAction {
    /// Returns true when this action should be dispatched asynchronously.
    pub fn is_async(&self) -> bool {
        self.mode.as_deref() == Some("async")
    }

    /// Effective HTTP timeout: explicit override, else 10 s for sync / 600 s
    /// for async. Must mirror d-watchdog's own equivalent exactly.
    pub fn effective_timeout(&self) -> Duration {
        if let Some(secs) = self.timeout_secs {
            return Duration::from_secs(secs);
        }
        if self.is_async() {
            Duration::from_secs(600)
        } else {
            Duration::from_secs(10)
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Dependencies {
    #[serde(default)]
    pub upstream: Vec<String>,
    #[serde(default)]
    pub downstream: Vec<String>,
    #[serde(default)]
    pub external: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HealthInfo {
    pub endpoint: String,
    #[serde(default = "default_status")]
    pub expected_status: u16,
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
}

fn default_auth() -> String {
    "none".into()
}

fn default_status() -> u16 {
    200
}

fn default_interval() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_endpoint_converts_into_manifest_endpoint_by_field_copy() {
        let route = crate::router::Endpoint {
            method: "GET".to_string(),
            path: "/health".to_string(),
            description: "Liveness.".to_string(),
            auth: "none".to_string(),
            key_ref: None,
            public: true,
            body_schema: None,
        };
        let endpoint: Endpoint = route.into();
        assert_eq!(endpoint.method, "GET");
        assert_eq!(endpoint.path, "/health");
        assert_eq!(endpoint.description, "Liveness.");
        assert_eq!(endpoint.auth, "none");
        assert!(endpoint.public);
    }

    #[test]
    fn manage_action_effective_timeout_defaults_by_mode() {
        let sync = ManageAction {
            id: "a".into(),
            label: "A".into(),
            description: None,
            method: "POST".into(),
            path: "/a".into(),
            auth: "bearer".into(),
            key_ref: None,
            confirm: false,
            icon: None,
            body: None,
            content_type: None,
            timeout_secs: None,
            mode: None,
            params_schema: vec![],
        };
        assert_eq!(sync.effective_timeout(), Duration::from_secs(10));

        let async_action = ManageAction { mode: Some("async".into()), ..sync.clone() };
        assert_eq!(async_action.effective_timeout(), Duration::from_secs(600));

        let overridden = ManageAction { timeout_secs: Some(5), ..sync };
        assert_eq!(overridden.effective_timeout(), Duration::from_secs(5));
    }
}
