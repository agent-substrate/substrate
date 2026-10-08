// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use dashmap::DashMap;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use serde::Deserialize;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    true
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    envoy_filter_config: &mut EC,
    _filter_name: &str,
    filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    let raw = std::str::from_utf8(filter_config).unwrap_or("");
    let json_str = if raw.trim().is_empty() { "{}" } else { raw };
    let config: Config = serde_json::from_str(json_str)
        .map_err(|e| {
            envoy_log_error!("ingress_cache: invalid filter config: {}", e);
        })
        .ok()?;

    envoy_log_trace!(
        "ingress_cache: ttl={}s suffix={}",
        config.cache_ttl_seconds,
        config.actor_dns_suffix
    );

    Some(Box::new(FilterConfig {
        cache_hit: envoy_filter_config
            .define_counter("ate_ingress_cache.hit")
            .ok(),
        cache_miss: envoy_filter_config
            .define_counter("ate_ingress_cache.miss")
            .ok(),
        cache_eviction: envoy_filter_config
            .define_counter("ate_ingress_cache.eviction")
            .ok(),
        recreate_stream: envoy_filter_config
            .define_counter("ate_ingress_cache.recreate_stream")
            .ok(),
        settings: config,
    }))
}

/// Config is the filter's configuration, supplied as JSON in the Envoy listener config.
#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    /// How long an actor -> worker binding may be served from cache.
    #[serde(default = "default_ttl")]
    pub cache_ttl_seconds: u64,
    /// The DNS suffix every actor authority ends with.
    #[serde(default = "default_suffix")]
    pub actor_dns_suffix: String,
    /// The port atunnel listens on at the worker. The Go handler hardcodes 443.
    #[serde(default = "default_atunnel_port")]
    pub atunnel_port: u16,
    /// Header carrying the actor's target port to atunnel.
    #[serde(default = "default_port_header")]
    pub target_port_header: String,
    /// Set false to disable caching.
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cache_ttl_seconds: default_ttl(),
            actor_dns_suffix: default_suffix(),
            atunnel_port: default_atunnel_port(),
            target_port_header: default_port_header(),
            cache_enabled: default_cache_enabled(),
        }
    }
}

fn default_ttl() -> u64 {
    5
}

fn default_suffix() -> String {
    "actors.resources.substrate.ate.dev".to_string()
}

fn default_atunnel_port() -> u16 {
    443
}

fn default_port_header() -> String {
    "x-ate-target-port".to_string()
}

fn default_cache_enabled() -> bool {
    true
}

/// Filter-state key the dataplane publishes the real authority under.
pub const AUTHORITY_FILTER_STATE_KEY: &[u8] = b"dev.ate.authority";

/// Dynamic-metadata namespace and keys the ORIGINAL_DST cluster reads.
pub const ORIGINAL_DST_NAMESPACE: &str = "envoy.filters.listener.original_dst";
pub const ORIGINAL_DST_ADDRESS_KEY: &str = "local";
pub const ORIGINAL_DST_PORT_KEY: &str = "port";

/// Header the cache filter sets on a cache hit. The route config matches
/// it to select a route that has ext_proc disabled via typed_per_filter_config.
pub const ROUTE_RESOLVED_HEADER: &str = "ate-endpoint-cached";

/// Response header set by atunnel when a request arrives for an actor not
/// hosted on this worker.
pub const STALE_ASSIGNMENT_HEADER: &str = "x-ate-assignment-stale";

/// A resolved actor -> worker binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub worker_ip: String,
    pub expires_at: Instant,
}

/// Process-global cache shared across worker threads.
pub fn cache() -> &'static DashMap<String, Binding> {
    static CACHE: OnceLock<DashMap<String, Binding>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

/// Per-filter-chain configuration.
pub struct FilterConfig {
    pub settings: Config,
    pub cache_hit: Option<EnvoyCounterId>,
    pub cache_miss: Option<EnvoyCounterId>,
    pub cache_eviction: Option<EnvoyCounterId>,
    pub recreate_stream: Option<EnvoyCounterId>,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FilterConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(CacheFilter {
            config: self.settings.clone(),
            cache_hit: self.cache_hit,
            cache_miss: self.cache_miss,
            cache_eviction: self.cache_eviction,
            recreate_stream: self.recreate_stream,
            learn_key: None,
            actor_key: None,
            was_cache_hit: false,
        })
    }
}

/// An actor reference parsed out of a request authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorRef {
    pub atespace: String,
    pub name: String,
    pub target_port: u16,
}

impl ActorRef {
    pub fn cache_key(&self) -> String {
        format!("{}/{}", self.atespace, self.name)
    }
}

impl Config {
    pub fn route_to<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &mut EHF,
        actor: &ActorRef,
        worker_ip: &str,
    ) {
        let target = if worker_ip.contains(':') && !worker_ip.starts_with('[') {
            format!("[{}]:{}", worker_ip, self.atunnel_port)
        } else {
            format!("{}:{}", worker_ip, self.atunnel_port)
        };
        envoy_filter.set_dynamic_metadata_string(
            ORIGINAL_DST_NAMESPACE,
            ORIGINAL_DST_ADDRESS_KEY,
            &target,
        );
        envoy_filter.set_dynamic_metadata_string(
            ORIGINAL_DST_NAMESPACE,
            ORIGINAL_DST_PORT_KEY,
            &actor.target_port.to_string(),
        );
        envoy_filter.set_request_header(
            &self.target_port_header,
            actor.target_port.to_string().as_bytes(),
        );
    }

    pub fn authority<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> Option<String> {
        if let Some(buf) = envoy_filter.get_filter_state_bytes(AUTHORITY_FILTER_STATE_KEY) {
            if !buf.as_slice().is_empty() {
                if let Ok(s) = std::str::from_utf8(buf.as_slice()) {
                    return Some(s.to_string());
                }
            }
        }
        envoy_filter
            .get_request_header_value(":authority")
            .and_then(|b| std::str::from_utf8(b.as_slice()).ok().map(str::to_string))
    }

    pub fn parse_actor_ref(&self, authority: &str) -> Option<ActorRef> {
        let (host, target_port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().ok()?),
            None => (authority, 80),
        };
        let host = host.strip_suffix('.').unwrap_or(host);
        let suffix = self.actor_dns_suffix.trim_start_matches('.');
        let labels = host.strip_suffix(suffix)?.strip_suffix('.')?;
        let (name, atespace) = labels.split_once('.')?;
        if name.is_empty() || atespace.is_empty() || atespace.contains('.') {
            return None;
        }
        Some(ActorRef {
            atespace: atespace.to_string(),
            name: name.to_string(),
            target_port,
        })
    }
}

/// CacheFilter sits in front of the ext_proc filter in the HTTP filter chain.
///
/// * On a hit: publishes dynamic metadata for ORIGINAL_DST, sets the
///   ROUTE_RESOLVED_HEADER to skip ext_proc via per-route config, and continues.
/// * On a miss: ext_proc runs normally. In on_response_headers, the filter learns
///   the worker IP from the metadata ext_proc published and stores it in the cache.
/// * On upstream 421 Misdirected Request rejection (actor suspended/moved):
///   the filter intercepts the rejection, evicts the cache entry, clears the
///   cache-resolved header, and calls recreate_stream to re-run the slow path
///   (invoking ext_proc to resume and relocate the actor).
pub struct CacheFilter {
    pub config: Config,
    pub cache_hit: Option<EnvoyCounterId>,
    pub cache_miss: Option<EnvoyCounterId>,
    pub cache_eviction: Option<EnvoyCounterId>,
    pub recreate_stream: Option<EnvoyCounterId>,
    pub learn_key: Option<String>,
    pub actor_key: Option<String>,
    pub was_cache_hit: bool,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for CacheFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        let Some(authority) = self.config.authority(envoy_filter) else {
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
        };
        let Some(actor) = self.config.parse_actor_ref(&authority) else {
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
        };

        let key = actor.cache_key();
        self.actor_key = Some(key.clone());

        if self.config.cache_enabled {
            if let Some(entry) = cache().get(&key) {
                if entry.expires_at > Instant::now() {
                    let worker_ip = entry.worker_ip.clone();
                    drop(entry);
                    if let Some(id) = self.cache_hit {
                        let _ = envoy_filter.increment_counter(id, 1);
                    }
                    self.config.route_to(envoy_filter, &actor, &worker_ip);
                    envoy_filter.set_request_header(ROUTE_RESOLVED_HEADER, b"1");
                    envoy_filter.clear_route_cache();
                    self.was_cache_hit = true;
                    return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
                }
                drop(entry);
                cache().remove(&key);
                if let Some(id) = self.cache_eviction {
                    let _ = envoy_filter.increment_counter(id, 1);
                }
            }
        }

        if let Some(id) = self.cache_miss {
            let _ = envoy_filter.increment_counter(id, 1);
        }
        // Ensure cache-bypass header is not set on a miss or recreated stream.
        envoy_filter.remove_request_header(ROUTE_RESOLVED_HEADER);
        self.was_cache_hit = false;
        self.learn_key = Some(key);
        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    }

    fn on_response_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_response_headers_status {
        let status = envoy_filter
            .get_response_header_value(":status")
            .and_then(|b| std::str::from_utf8(b.as_slice()).ok().map(str::to_string));

        let is_stale_rejection = if status.as_deref() == Some("421") {
            let is_stale_header = envoy_filter
                .get_response_header_value(STALE_ASSIGNMENT_HEADER)
                .and_then(|b| std::str::from_utf8(b.as_slice()).ok().map(str::to_string));
            match is_stale_header {
                Some(val) => val.eq_ignore_ascii_case("true"),
                None => true,
            }
        } else {
            false
        };

        if is_stale_rejection && self.was_cache_hit {
            if let Some(key) = &self.actor_key {
                cache().remove(key);
                if let Some(id) = self.cache_eviction {
                    let _ = envoy_filter.increment_counter(id, 1);
                }
            }
            envoy_filter.remove_request_header(ROUTE_RESOLVED_HEADER);
            self.was_cache_hit = false;

            if let Some(id) = self.recreate_stream {
                let _ = envoy_filter.increment_counter(id, 1);
            }
            if envoy_filter.recreate_stream(None) {
                return abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration;
            }
        }

        // On a cache miss, learn from the metadata ext_proc published.
        if let (Some(key), true) = (self.learn_key.take(), self.config.cache_enabled) {
            if let Some(buf) = envoy_filter.get_metadata_string(
                abi::envoy_dynamic_module_type_metadata_source::Dynamic,
                ORIGINAL_DST_NAMESPACE,
                ORIGINAL_DST_ADDRESS_KEY,
            ) {
                if let Ok(addr) = std::str::from_utf8(buf.as_slice()) {
                    if let Some((raw_ip, _port)) = addr.rsplit_once(':') {
                        let ip = raw_ip.trim_matches(|c| c == '[' || c == ']');
                        if ip.parse::<std::net::IpAddr>().is_ok() {
                            cache().insert(
                                key,
                                Binding {
                                    worker_ip: ip.to_string(),
                                    expires_at: Instant::now()
                                        + Duration::from_secs(self.config.cache_ttl_seconds),
                                },
                            );
                        }
                    }
                }
            }
        }

        abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_actor_ref() {
        let config = Config::default();

        // Standard authority without port
        let actor = config
            .parse_actor_ref("actor-1.team-a.actors.resources.substrate.ate.dev")
            .expect("should parse valid actor ref");
        assert_eq!(
            actor,
            ActorRef {
                atespace: "team-a".to_string(),
                name: "actor-1".to_string(),
                target_port: 80,
            }
        );
        assert_eq!(actor.cache_key(), "team-a/actor-1");

        // With target port
        let actor_port = config
            .parse_actor_ref("actor-1.team-a.actors.resources.substrate.ate.dev:8080")
            .expect("should parse with port");
        assert_eq!(
            actor_port,
            ActorRef {
                atespace: "team-a".to_string(),
                name: "actor-1".to_string(),
                target_port: 8080,
            }
        );

        // FQDN with trailing dot
        let actor_dot = config
            .parse_actor_ref("actor-1.team-a.actors.resources.substrate.ate.dev.")
            .expect("should parse FQDN");
        assert_eq!(actor_dot.name, "actor-1");
        assert_eq!(actor_dot.atespace, "team-a");

        // Invalid suffixes or too many labels
        assert!(
            config
                .parse_actor_ref("actor-1.team-a.other.domain.com")
                .is_none()
        );
        assert!(
            config
                .parse_actor_ref("sub.actor-1.team-a.actors.resources.substrate.ate.dev")
                .is_none()
        );
        assert!(
            config
                .parse_actor_ref(".team-a.actors.resources.substrate.ate.dev")
                .is_none()
        );
        assert!(
            config
                .parse_actor_ref("actor-1..actors.resources.substrate.ate.dev")
                .is_none()
        );
    }

    #[test]
    fn test_cache_miss_and_learn_on_response() {
        let config = Config::default();
        let mut filter = CacheFilter {
            config: config.clone(),
            cache_hit: None,
            cache_miss: None,
            cache_eviction: None,
            recreate_stream: None,
            learn_key: None,
            actor_key: None,
            was_cache_hit: false,
        };

        let authority = b"actor-2.team-b.actors.resources.substrate.ate.dev";
        let mut mock = MockEnvoyHttpFilter::default();
        mock.expect_get_filter_state_bytes()
            .withf(|key| key == AUTHORITY_FILTER_STATE_KEY)
            .returning(|_| None);
        mock.expect_get_request_header_value()
            .withf(|key| key == ":authority")
            .returning(move |_| Some(EnvoyBuffer::new(authority)));
        mock.expect_remove_request_header()
            .withf(|key| key == ROUTE_RESOLVED_HEADER)
            .returning(|_| true);

        let status = filter.on_request_headers(&mut mock, false);
        assert_eq!(
            status,
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        );
        assert_eq!(filter.learn_key.as_deref(), Some("team-b/actor-2"));
        assert!(!filter.was_cache_hit);

        // Response headers arrive: learning worker address from dynamic metadata
        let metadata_local = b"10.244.1.50:443";
        mock.expect_get_response_header_value()
            .withf(|key| key == ":status")
            .returning(|_| Some(EnvoyBuffer::new(b"200")));
        mock.expect_get_metadata_string()
            .withf(|src, ns, key| {
                *src == abi::envoy_dynamic_module_type_metadata_source::Dynamic
                    && ns == ORIGINAL_DST_NAMESPACE
                    && key == ORIGINAL_DST_ADDRESS_KEY
            })
            .returning(move |_, _, _| Some(EnvoyBuffer::new(metadata_local)));

        let resp_status = filter.on_response_headers(&mut mock, false);
        assert_eq!(
            resp_status,
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
        );

        // Verify entry is now in cache
        let entry = cache().get("team-b/actor-2").expect("should be in cache");
        assert_eq!(entry.worker_ip, "10.244.1.50");
    }

    #[test]
    fn test_cache_hit_populates_metadata_and_skips_extproc() {
        let config = Config::default();
        cache().insert(
            "team-c/actor-3".to_string(),
            Binding {
                worker_ip: "10.244.2.100".to_string(),
                expires_at: Instant::now() + Duration::from_secs(5),
            },
        );

        let mut filter = CacheFilter {
            config,
            cache_hit: None,
            cache_miss: None,
            cache_eviction: None,
            recreate_stream: None,
            learn_key: None,
            actor_key: None,
            was_cache_hit: false,
        };

        let authority = b"actor-3.team-c.actors.resources.substrate.ate.dev";
        let mut mock = MockEnvoyHttpFilter::default();
        mock.expect_get_filter_state_bytes()
            .withf(|key| key == AUTHORITY_FILTER_STATE_KEY)
            .returning(|_| None);
        mock.expect_get_request_header_value()
            .withf(|key| key == ":authority")
            .returning(move |_| Some(EnvoyBuffer::new(authority)));

        mock.expect_set_dynamic_metadata_string()
            .withf(|ns, key, val| {
                ns == ORIGINAL_DST_NAMESPACE
                    && key == ORIGINAL_DST_ADDRESS_KEY
                    && val == "10.244.2.100:443"
            })
            .returning(|_, _, _| ());
        mock.expect_set_dynamic_metadata_string()
            .withf(|ns, key, val| {
                ns == ORIGINAL_DST_NAMESPACE && key == ORIGINAL_DST_PORT_KEY && val == "80"
            })
            .returning(|_, _, _| ());
        mock.expect_set_request_header()
            .withf(|key, val| key == "x-ate-target-port" && val == b"80")
            .returning(|_, _| true);
        mock.expect_set_request_header()
            .withf(|key, val| key == ROUTE_RESOLVED_HEADER && val == b"1")
            .returning(|_, _| true);
        mock.expect_clear_route_cache().returning(|| ());

        let status = filter.on_request_headers(&mut mock, false);
        assert_eq!(
            status,
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        );
        assert!(filter.was_cache_hit);
    }

    #[test]
    fn test_upstream_421_recreates_stream_on_cache_hit() {
        let config = Config::default();
        cache().insert(
            "team-d/actor-4".to_string(),
            Binding {
                worker_ip: "10.244.3.40".to_string(),
                expires_at: Instant::now() + Duration::from_secs(5),
            },
        );

        let mut filter = CacheFilter {
            config,
            cache_hit: None,
            cache_miss: None,
            cache_eviction: None,
            recreate_stream: None,
            learn_key: None,
            actor_key: Some("team-d/actor-4".to_string()),
            was_cache_hit: true,
        };

        let mut mock = MockEnvoyHttpFilter::default();
        mock.expect_get_response_header_value()
            .withf(|key| key == ":status")
            .returning(|_| Some(EnvoyBuffer::new(b"421")));
        mock.expect_get_response_header_value()
            .withf(|key| key == STALE_ASSIGNMENT_HEADER)
            .returning(|_| Some(EnvoyBuffer::new(b"true")));
        mock.expect_remove_request_header()
            .withf(|key| key == ROUTE_RESOLVED_HEADER)
            .returning(|_| true);
        mock.expect_recreate_stream().returning(|_| true);

        let resp_status = filter.on_response_headers(&mut mock, false);
        assert_eq!(
            resp_status,
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
        );

        // Cache entry must have been evicted
        assert!(cache().get("team-d/actor-4").is_none());
        assert!(!filter.was_cache_hit);
    }

    #[test]
    fn test_upstream_421_does_not_loop_on_cache_miss() {
        let config = Config::default();
        let mut filter = CacheFilter {
            config,
            cache_hit: None,
            cache_miss: None,
            cache_eviction: None,
            recreate_stream: None,
            learn_key: Some("team-e/actor-5".to_string()),
            actor_key: Some("team-e/actor-5".to_string()),
            was_cache_hit: false,
        };

        let mut mock = MockEnvoyHttpFilter::default();
        mock.expect_get_response_header_value()
            .withf(|key| key == ":status")
            .returning(|_| Some(EnvoyBuffer::new(b"421")));
        mock.expect_get_response_header_value()
            .withf(|key| key == STALE_ASSIGNMENT_HEADER)
            .returning(|_| Some(EnvoyBuffer::new(b"true")));
        mock.expect_get_metadata_string().returning(|_, _, _| None);

        // recreate_stream should NOT be called on a cache miss!
        let resp_status = filter.on_response_headers(&mut mock, false);
        assert_eq!(
            resp_status,
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
        );
    }

    #[test]
    fn test_ipv6_learning_and_routing() {
        let config = Config::default();
        let mut filter = CacheFilter {
            config: config.clone(),
            cache_hit: None,
            cache_miss: None,
            cache_eviction: None,
            recreate_stream: None,
            learn_key: Some("team-f/actor-6".to_string()),
            actor_key: None,
            was_cache_hit: false,
        };

        let mut mock = MockEnvoyHttpFilter::default();
        mock.expect_get_response_header_value()
            .returning(|_| Some(EnvoyBuffer::new(b"200")));
        mock.expect_get_metadata_string()
            .withf(|source, ns, key| {
                *source == abi::envoy_dynamic_module_type_metadata_source::Dynamic
                    && ns == ORIGINAL_DST_NAMESPACE
                    && key == ORIGINAL_DST_ADDRESS_KEY
            })
            .returning(|_, _, _| Some(EnvoyBuffer::new(b"[2001:db8::1]:443")));

        let resp_status = filter.on_response_headers(&mut mock, false);
        assert_eq!(
            resp_status,
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
        );

        let entry = cache()
            .get("team-f/actor-6")
            .expect("entry should be cached");
        assert_eq!(entry.worker_ip, "2001:db8::1");
        drop(entry);

        let actor = ActorRef {
            atespace: "team-f".to_string(),
            name: "actor-6".to_string(),
            target_port: 8080,
        };
        let mut route_mock = MockEnvoyHttpFilter::default();
        route_mock
            .expect_set_dynamic_metadata_string()
            .withf(|ns, key, val| {
                ns == ORIGINAL_DST_NAMESPACE
                    && key == ORIGINAL_DST_ADDRESS_KEY
                    && val == "[2001:db8::1]:443"
            })
            .returning(|_, _, _| ());
        route_mock
            .expect_set_dynamic_metadata_string()
            .withf(|ns, key, val| {
                ns == ORIGINAL_DST_NAMESPACE && key == ORIGINAL_DST_PORT_KEY && val == "8080"
            })
            .returning(|_, _, _| ());
        route_mock
            .expect_set_request_header()
            .withf(|key, val| key == "x-ate-target-port" && val == b"8080")
            .returning(|_, _| true);

        config.route_to(&mut route_mock, &actor, "2001:db8::1");
    }
}
