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

//! An Envoy dynamic module that does the atenet router's ingress routing
//! in-process, instead of over an ext_proc gRPC hop to the Go router.
//!
//! It is a like-for-like replacement for
//! `cmd/atenet/internal/router/ingress.Handler.HandleRequestHeaders`: it reads
//! the same authority from the same filter-state key, parses the same actor DNS
//! name, and publishes the same `envoy.filters.listener.original_dst` dynamic
//! metadata that the ORIGINAL_DST cluster in `xds.go` routes on. The difference
//! is where the work happens and how often the control plane is consulted:
//!
//! * The Go path calls `ResumeActor` on ate-apiserver for **every** request,
//!   including requests to an actor that is already running on a known worker.
//! * This module answers those from a TTL cache shared across Envoy's worker
//!   threads, and only calls the control plane on a miss.
//!
//! The cache is deliberately conservative: it holds only the actor -> worker
//! binding, for a short TTL, and a routing failure evicts the entry so the next
//! request re-resolves. See the README for why that is safe and where it is not.

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
    _envoy_filter_config: &mut EC,
    filter_name: &str,
    filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    match filter_name {
        // Two modes, sharing one cache:
        //
        //   "actor_router"       full replacement: resolves misses itself with an
        //                        HTTP callout, and ext_proc is not in the chain.
        //   "actor_router_cache" co-existence: a cache in FRONT of the existing
        //                        ext_proc filter. It answers hits and lets every
        //                        miss fall through to ext_proc untouched, then
        //                        learns the binding from what ext_proc decided.
        //                        Nothing about the Go path changes.
        "actor_router" | "actor_router_cache" => {
            let raw = std::str::from_utf8(filter_config).ok()?;
            let config: Config = serde_json::from_str(raw)
                .map_err(|e| {
                    envoy_log_error!("actor_router: invalid filter config: {}", e);
                })
                .ok()?;
            envoy_log_info!(
                "actor_router: cluster={} ttl={}s suffix={}",
                config.ateapi_cluster,
                config.cache_ttl_seconds,
                config.actor_dns_suffix
            );
            // Counters are Envoy-native stats, defined once per filter config and
            // incremented by id. They show up in Envoy's own /stats alongside every
            // other dataplane counter, with no per-request attribute allocation --
            // unlike the OTel histogram the Go path records per request.
            Some(Box::new(FilterConfig {
                cache_hit: _envoy_filter_config.define_counter("ate_router.cache_hit").ok(),
                cache_miss: _envoy_filter_config.define_counter("ate_router.cache_miss").ok(),
                coexist: filter_name == "actor_router_cache",
                settings: config,
            }))
        }
        other => {
            envoy_log_error!("actor_router: unknown filter name {}", other);
            None
        }
    }
}

/// Config is the filter's `filter_config`, supplied as JSON in the Envoy
/// listener config. FilterConfig below pairs it with the Envoy counter ids; the
/// filter itself keeps a clone of these settings.
#[derive(Deserialize, Clone)]
struct Config {
    /// Envoy cluster name to send the resolve callout to on a cache miss. Must
    /// be defined in the Envoy config; the module cannot invent clusters.
    ateapi_cluster: String,
    /// How long an actor -> worker binding may be served from cache. Must stay
    /// well below the control plane's idle-suspend timeout: see README.
    #[serde(default = "default_ttl")]
    cache_ttl_seconds: u64,
    /// Callout timeout on a cache miss.
    #[serde(default = "default_timeout")]
    callout_timeout_ms: u64,
    /// The DNS suffix every actor authority ends with.
    #[serde(default = "default_suffix")]
    actor_dns_suffix: String,
    /// The port atunnel listens on at the worker. The Go handler hardcodes 443.
    #[serde(default = "default_atunnel_port")]
    atunnel_port: u16,
    /// Header carrying the actor's target port to atunnel, which cannot read
    /// dynamic metadata. Mirrors atunnel.TargetPortHeader.
    #[serde(default = "default_port_header")]
    target_port_header: String,
    /// Set false to measure the module with caching off, which isolates the
    /// saving from removing the ext_proc hop alone.
    #[serde(default = "default_cache_enabled")]
    cache_enabled: bool,
}

fn default_ttl() -> u64 { 5 }
fn default_timeout() -> u64 { 5000 }
fn default_suffix() -> String { "actors.resources.substrate.ate.dev".to_string() }
fn default_atunnel_port() -> u16 { 443 }
fn default_port_header() -> String { "x-ate-target-port".to_string() }
fn default_cache_enabled() -> bool { true }

/// Filter-state key the dataplane publishes the real authority under. Ingress
/// reads this rather than :authority because a reinjected CONNECT tunnel's own
/// authority has nothing to do with the actor.
/// Mirrors ingress.AuthorityFilterStateKey.
const AUTHORITY_FILTER_STATE_KEY: &[u8] = b"dev.ate.authority";

/// Dynamic-metadata namespace and keys the ORIGINAL_DST cluster reads.
/// Mirrors ingress.OriginalDstMetadataKey / OriginalDstAddressKey / OriginalDstPortKey.
const ORIGINAL_DST_NAMESPACE: &str = "envoy.filters.listener.original_dst";
const ORIGINAL_DST_ADDRESS_KEY: &str = "local";
const ORIGINAL_DST_PORT_KEY: &str = "port";

/// A resolved actor -> worker binding.
#[derive(Clone)]
struct Binding {
    worker_ip: String,
    expires_at: Instant,
}

/// The cache is process-global and shared by every Envoy worker thread: the
/// module is dlopen'd once, so one map serves all of them. DashMap gives
/// sharded locking, so worker threads do not contend on a single mutex the way
/// they would behind a global RwLock.
fn cache() -> &'static DashMap<String, Binding> {
    static CACHE: OnceLock<DashMap<String, Binding>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

/// FilterConfig is the per-filter-chain configuration: the settings parsed from
/// JSON, plus the Envoy counter ids defined once at config load.
struct FilterConfig {
    settings: Config,
    cache_hit: Option<EnvoyCounterId>,
    cache_miss: Option<EnvoyCounterId>,
    coexist: bool,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FilterConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        if self.coexist {
            return Box::new(CacheFilter {
                config: self.settings.clone(),
                cache_hit: self.cache_hit,
                cache_miss: self.cache_miss,
                learn_key: None,
            });
        }
        Box::new(Filter {
            config: self.settings.clone(),
            cache_hit: self.cache_hit,
            cache_miss: self.cache_miss,
            pending: None,
        })
    }
}

/// An actor reference parsed out of a request authority.
struct ActorRef {
    atespace: String,
    name: String,
    /// The port named in the authority, or the actor's default port.
    target_port: u16,
}

impl ActorRef {
    fn cache_key(&self) -> String {
        format!("{}/{}", self.atespace, self.name)
    }
}

/// State carried from on_request_headers to on_http_callout_done for a request
/// that missed the cache.
struct Pending {
    actor: ActorRef,
    callout_id: u64,
}

struct Filter {
    config: Config,
    cache_hit: Option<EnvoyCounterId>,
    cache_miss: Option<EnvoyCounterId>,
    pending: Option<Pending>,
}

/// Helpers shared by both filter modes. They depend only on the parsed config,
/// so they live here rather than on either filter -- in particular so the
/// co-existence filter never has to name the callout filter's type.
impl Config {
    /// Publishes the routing decision the same way the Go ingress handler does:
    /// dynamic metadata for the ORIGINAL_DST cluster, plus the target-port
    /// header for atunnel. :authority is deliberately left untouched so atunnel
    /// still authorizes by the actor's own DNS name.
    fn route_to<EHF: EnvoyHttpFilter>(&self, envoy_filter: &mut EHF, actor: &ActorRef, worker_ip: &str) {
        let target = format!("{}:{}", worker_ip, self.atunnel_port);
        envoy_filter.set_dynamic_metadata_string(ORIGINAL_DST_NAMESPACE, ORIGINAL_DST_ADDRESS_KEY, &target);
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

    /// Reads the authority the dataplane resolved for this request, preferring
    /// the filter-state key the ingress listener publishes and falling back to
    /// :authority when the listener does not set it.
    fn authority<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> Option<String> {
        if let Some(buf) = envoy_filter.get_filter_state_bytes(AUTHORITY_FILTER_STATE_KEY) {
            if !buf.as_slice().is_empty() {
                return std::str::from_utf8(buf.as_slice()).ok().map(str::to_string);
            }
        }
        envoy_filter
            .get_request_header_value(":authority")
            .and_then(|b| std::str::from_utf8(b.as_slice()).ok().map(str::to_string))
    }

    /// Parses "<actor>.<atespace>.<suffix>" with an optional ":port".
    /// Mirrors resources.ParseActorDNSName + ingress.parseActorRef.
    fn parse_actor_ref(&self, authority: &str) -> Option<ActorRef> {
        let (host, target_port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().ok()?),
            None => (authority, 80),
        };
        let labels = host.strip_suffix(&self.actor_dns_suffix)?.strip_suffix('.')?;
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

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for Filter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        let Some(authority) = self.config.authority(envoy_filter) else {
            envoy_filter.send_response(404, &[], Some(b"no authority on request"), None);
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
        };

        let Some(actor) = self.config.parse_actor_ref(&authority) else {
            // Same disposition as ingress.invalidHostErr: an authority that is
            // not an actor DNS name is a 404, not a 500.
            envoy_filter.send_response(404, &[], Some(b"invalid actor host"), None);
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
        };

        // Fast path: a live binding answers without touching the control plane.
        // This is the request the Go path spends a full ResumeActor RPC on.
        if self.config.cache_enabled {
            let key = actor.cache_key();
            if let Some(entry) = cache().get(&key) {
                if entry.expires_at > Instant::now() {
                    let worker_ip = entry.worker_ip.clone();
                    drop(entry);
                    if let Some(id) = self.cache_hit {
                        let _ = envoy_filter.increment_counter(id, 1);
                    }
                    self.config.route_to(envoy_filter, &actor, &worker_ip);
                    return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
                }
                // Expired: drop it so a concurrent request cannot serve it either.
                drop(entry);
                cache().remove(&key);
            }
        }

        // Slow path: resolve through the control plane, holding the request.
        if let Some(id) = self.cache_miss {
            let _ = envoy_filter.increment_counter(id, 1);
        }
        let path = format!(
            "/v1/resume?atespace={}&actor={}",
            actor.atespace, actor.name
        );
        let (result, callout_id) = envoy_filter.send_http_callout(
            &self.config.ateapi_cluster,
            &[
                (":method", b"GET"),
                (":path", path.as_bytes()),
                (":authority", self.config.ateapi_cluster.as_bytes()),
            ],
            None,
            self.config.callout_timeout_ms,
        );
        if result != abi::envoy_dynamic_module_type_http_callout_init_result::Success {
            envoy_log_error!("actor_router: callout init failed: {:?}", result);
            envoy_filter.send_response(503, &[], Some(b"control plane unreachable"), None);
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
        }

        self.pending = Some(Pending { actor, callout_id });
        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    }

    fn on_http_callout_done(
        &mut self,
        envoy_filter: &mut EHF,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        _response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
        response_body: Option<&[EnvoyBuffer]>,
    ) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        if pending.callout_id != callout_id {
            return;
        }

        if result != abi::envoy_dynamic_module_type_http_callout_result::Success {
            envoy_log_error!("actor_router: resume callout failed: {:?}", result);
            envoy_filter.send_response(503, &[], Some(b"actor resume failed"), None);
            return;
        }

        let mut body = Vec::new();
        if let Some(chunks) = response_body {
            for chunk in chunks {
                body.extend_from_slice(chunk.as_slice());
            }
        }

        #[derive(Deserialize)]
        struct ResumeResponse {
            worker_ip: String,
        }

        let Ok(resp) = serde_json::from_slice::<ResumeResponse>(&body) else {
            envoy_log_error!("actor_router: unparsable resume response");
            envoy_filter.send_response(503, &[], Some(b"actor resume failed"), None);
            return;
        };
        if resp.worker_ip.parse::<std::net::IpAddr>().is_err() {
            // Mirrors the net.ParseIP guard in ingress.go: a non-IP answer is a
            // control-plane bug, and routing to it would fail confusingly later.
            envoy_log_error!("actor_router: resume returned non-IP {}", resp.worker_ip);
            envoy_filter.send_response(500, &[], Some(b"actor routing failed"), None);
            return;
        }

        if self.config.cache_enabled {
            cache().insert(
                pending.actor.cache_key(),
                Binding {
                    worker_ip: resp.worker_ip.clone(),
                    expires_at: Instant::now() + Duration::from_secs(self.config.cache_ttl_seconds),
                },
            );
        }

        self.config.route_to(envoy_filter, &pending.actor, &resp.worker_ip);
        envoy_filter.continue_decoding();
    }
}

/// Header the co-existence filter sets on a cache hit. The route config matches
/// it to select a route that has ext_proc disabled via typed_per_filter_config,
/// which is how a hit skips the gRPC hop. A request that does not carry it
/// takes the normal route and the normal ext_proc path.
const ROUTE_RESOLVED_HEADER: &str = "x-ate-route-resolved";

/// CacheFilter sits in front of the existing ext_proc filter and changes
/// nothing about it.
///
/// * On a hit it publishes the same dynamic metadata ext_proc would have
///   published, marks the request so route selection skips ext_proc, and
///   continues.
/// * On a miss it does nothing at all: ext_proc runs exactly as it does today,
///   with its resumer, singleflight, parking and metrics intact. Once the
///   response comes back the filter reads the decision ext_proc published and
///   caches it for next time.
///
/// So the module never talks to ate-apiserver, needs no client certificate, and
/// cannot invent a route ext_proc would not have chosen. Removing the filter
/// from the chain restores today's behaviour exactly.
struct CacheFilter {
    config: Config,
    cache_hit: Option<EnvoyCounterId>,
    cache_miss: Option<EnvoyCounterId>,
    /// Set when this request missed, naming the entry to fill in on the way back.
    learn_key: Option<String>,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for CacheFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        // A helper filter must never turn a request away: anything it cannot
        // understand is simply handed to ext_proc, which owns the error
        // responses and their exact wording.
        let Some(authority) = self.config.authority(envoy_filter) else {
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
        };
        let Some(actor) = self.config.parse_actor_ref(&authority) else {
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
        };

        let key = actor.cache_key();
        if self.config.cache_enabled {
            if let Some(entry) = cache().get(&key) {
                if entry.expires_at > Instant::now() {
                    let worker_ip = entry.worker_ip.clone();
                    drop(entry);
                    if let Some(id) = self.cache_hit {
                        let _ = envoy_filter.increment_counter(id, 1);
                    }
                    self.config.route_to(envoy_filter, &actor, &worker_ip);
                    // Mark the request and force route re-selection, so ext_proc
                    // picks up the per-route "disabled" override.
                    envoy_filter.set_request_header(ROUTE_RESOLVED_HEADER, b"1");
                    envoy_filter.clear_route_cache();
                    return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
                }
                drop(entry);
                cache().remove(&key);
            }
        }

        if let Some(id) = self.cache_miss {
            let _ = envoy_filter.increment_counter(id, 1);
        }
        // Fall through to ext_proc and learn from whatever it decides.
        self.learn_key = Some(key);
        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    }

    fn on_response_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_response_headers_status {
        // Only a request that missed has anything to learn. Reaching response
        // headers at all means the request was routed to a worker, so the
        // metadata ext_proc published is a decision that actually worked.
        if let (Some(key), true) = (self.learn_key.take(), self.config.cache_enabled) {
            if let Some(buf) = envoy_filter.get_metadata_string(
                abi::envoy_dynamic_module_type_metadata_source::Dynamic,
                ORIGINAL_DST_NAMESPACE,
                ORIGINAL_DST_ADDRESS_KEY,
            ) {
                if let Ok(addr) = std::str::from_utf8(buf.as_slice()) {
                    // ext_proc writes "<worker ip>:443"; keep only the IP so the
                    // port stays this filter's configuration, not a parsed value.
                    if let Some((ip, _port)) = addr.rsplit_once(':') {
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
