#![cfg(feature = "nemo")]
//! Golden-JSON proof that `manifest::ServiceManifest` serialises to the
//! exact wire shape a service registry expects on `PUT /registry/:name` —
//! every field name, nesting, and default spelled out literally, so a
//! rename or a dropped `#[serde(default)]` fails this test instead of
//! surfacing as a silent 400/404 in production.

use mcp_service4agent::manifest::{
    Dependencies, Endpoint, HealthInfo, Links, ManageAction, ParamSpec, ServiceManifest, ServiceMeta,
};
use serde_json::{Value, json};

fn sample_manifest() -> ServiceManifest {
    ServiceManifest {
        service: ServiceMeta {
            name: "example-service".to_string(),
            kind: "http_api".to_string(),
            display_name: Some("Example Service".to_string()),
            description: Some("A sample service for the golden JSON test.".to_string()),
            version: Some("1.2.3".to_string()),
            key_ref: Some("env/example.env:EXAMPLE_KEY".to_string()),
        },
        links: Links {
            public_url: Some("https://example.invalid".to_string()),
            local_url: Some("http://127.0.0.1:9000".to_string()),
            admin_url: None,
            docs: None,
            repo: Some("https://example.invalid/example-service".to_string()),
        },
        endpoints: vec![
            Endpoint {
                method: "GET".to_string(),
                path: "/health".to_string(),
                description: "Liveness.".to_string(),
                auth: "none".to_string(),
                key_ref: None,
                public: true,
                body_schema: None,
            },
            Endpoint {
                method: "POST".to_string(),
                path: "/items".to_string(),
                description: "Create an item.".to_string(),
                auth: "bearer".to_string(),
                key_ref: Some("env/example.env:EXAMPLE_KEY".to_string()),
                public: false,
                body_schema: Some("{\"type\":\"object\"}".to_string()),
            },
        ],
        manage_actions: vec![ManageAction {
            id: "restart".to_string(),
            label: "Restart".to_string(),
            description: Some("Restart the service.".to_string()),
            method: "POST".to_string(),
            path: "/manage/restart".to_string(),
            auth: "bearer".to_string(),
            key_ref: None,
            confirm: true,
            icon: Some("restart".to_string()),
            body: None,
            content_type: None,
            timeout_secs: Some(30),
            mode: Some("async".to_string()),
            params_schema: vec![ParamSpec {
                name: "force".to_string(),
                required: false,
                example: Some("true".to_string()),
                description: Some("Skip the graceful drain.".to_string()),
            }],
        }],
        dependencies: Dependencies {
            upstream: vec!["upstream-service".to_string()],
            downstream: vec![],
            external: vec!["postgres".to_string()],
        },
        health: Some(HealthInfo { endpoint: "/health".to_string(), expected_status: 200, interval_secs: 30 }),
    }
}

const GOLDEN: &str = r#"{
  "service": {
    "name": "example-service",
    "kind": "http_api",
    "display_name": "Example Service",
    "description": "A sample service for the golden JSON test.",
    "version": "1.2.3",
    "key_ref": "env/example.env:EXAMPLE_KEY"
  },
  "links": {
    "public_url": "https://example.invalid",
    "local_url": "http://127.0.0.1:9000",
    "admin_url": null,
    "docs": null,
    "repo": "https://example.invalid/example-service"
  },
  "endpoints": [
    {
      "method": "GET",
      "path": "/health",
      "description": "Liveness.",
      "auth": "none",
      "key_ref": null,
      "public": true,
      "body_schema": null
    },
    {
      "method": "POST",
      "path": "/items",
      "description": "Create an item.",
      "auth": "bearer",
      "key_ref": "env/example.env:EXAMPLE_KEY",
      "public": false,
      "body_schema": "{\"type\":\"object\"}"
    }
  ],
  "manage_actions": [
    {
      "id": "restart",
      "label": "Restart",
      "description": "Restart the service.",
      "method": "POST",
      "path": "/manage/restart",
      "auth": "bearer",
      "key_ref": null,
      "confirm": true,
      "icon": "restart",
      "body": null,
      "content_type": null,
      "timeout_secs": 30,
      "mode": "async",
      "params_schema": [
        {
          "name": "force",
          "required": false,
          "example": "true",
          "description": "Skip the graceful drain."
        }
      ]
    }
  ],
  "dependencies": {
    "upstream": ["upstream-service"],
    "downstream": [],
    "external": ["postgres"]
  },
  "health": {
    "endpoint": "/health",
    "expected_status": 200,
    "interval_secs": 30
  }
}"#;

#[test]
fn service_manifest_matches_the_golden_wire_shape() {
    let actual: Value = serde_json::to_value(sample_manifest()).expect("ServiceManifest serialises");
    let expected: Value = serde_json::from_str(GOLDEN).expect("golden fixture is valid JSON");
    assert_eq!(actual, expected);
}

#[test]
fn golden_json_round_trips_back_into_the_same_manifest() {
    let expected = sample_manifest();
    let recovered: ServiceManifest = serde_json::from_str(GOLDEN).expect("golden fixture parses");
    assert_eq!(expected, recovered);
}

#[test]
fn defaults_apply_when_optional_fields_are_omitted() {
    let minimal = json!({
        "service": { "name": "minimal-service", "kind": "daemon" }
    });
    let manifest: ServiceManifest = serde_json::from_value(minimal).expect("minimal manifest parses via defaults");
    assert_eq!(manifest.service.display_name, None);
    assert_eq!(manifest.links, Links::default());
    assert!(manifest.endpoints.is_empty());
    assert!(manifest.manage_actions.is_empty());
    assert_eq!(manifest.dependencies, Dependencies::default());
    assert_eq!(manifest.health, None);
}
