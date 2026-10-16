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

use hickory_resolver::config::{LookupIpStrategy, NameServerConfig, Protocol, ResolverConfig, ResolverOpts};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::AsyncResolver;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ProbeRequest {
    #[serde(default)]
    runtime: String,
    #[serde(default)]
    op: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    attempts: usize,
    #[serde(default)]
    interval_ms: u64,
    #[serde(default)]
    enable_cache: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeAttempt {
    success: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    addresses: Vec<String>,
    #[serde(skip_serializing_if = "is_zero")]
    status_code: u16,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    duration_ms: u64,
}

fn is_zero(v: &u16) -> bool {
    *v == 0
}

#[derive(Serialize)]
struct ProbeResponse {
    attempts: Vec<ProbeAttempt>,
}

fn parse_http_url(url: &str) -> (String, u16, String) {
    let rest = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    let (host, port) = match host_port.find(':') {
        Some(idx) => (
            host_port[..idx].to_string(),
            host_port[idx + 1..].parse::<u16>().unwrap_or(80),
        ),
        None => (host_port.to_string(), 80),
    };
    (host, port, path.to_string())
}

fn build_hickory_resolver(enable_cache: bool) -> AsyncResolver<TokioConnectionProvider> {
    let mut config = ResolverConfig::new();
    let mut ndots: usize = 1;
    if let Ok(content) = fs::read_to_string("/etc/resolv.conf") {
        for line in content.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("nameserver ") {
                if let Ok(ip) = rest.trim().parse::<IpAddr>() {
                    let ns = NameServerConfig {
                        socket_addr: SocketAddr::new(ip, 53),
                        protocol: Protocol::Udp,
                        tls_dns_name: None,
                        trust_negative_responses: false,
                        bind_addr: None,
                    };
                    config.add_name_server(ns);
                }
            } else if let Some(rest) = trimmed.strip_prefix("search ") {
                for domain in rest.split_whitespace() {
                    if let Ok(name) = hickory_resolver::Name::from_ascii(domain) {
                        config.add_search(name);
                    }
                }
            } else if let Some(rest) = trimmed.strip_prefix("options ") {
                for opt in rest.split_whitespace() {
                    if let Some(val) = opt.strip_prefix("ndots:") {
                        if let Ok(parsed) = val.parse::<usize>() {
                            ndots = parsed;
                        }
                    }
                }
            }
        }
    }
    let mut opts = ResolverOpts::default();
    opts.ip_strategy = LookupIpStrategy::Ipv4Only;
    opts.timeout = Duration::from_millis(1500);
    opts.attempts = 2;
    opts.ndots = ndots;
    opts.cache_size = if enable_cache { 128 } else { 0 };
    AsyncResolver::new(config, opts, TokioConnectionProvider::default())
}

fn do_http_get(addr: SocketAddr, host_header: &str, path: &str, dur_start: Instant) -> ProbeAttempt {
    match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
        Ok(mut stream) => {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
            let req = format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                path, host_header
            );
            if let Err(e) = stream.write_all(req.as_bytes()) {
                return ProbeAttempt {
                    success: false,
                    addresses: vec![],
                    status_code: 0,
                    error_code: "WriteError".to_string(),
                    error: e.to_string(),
                    duration_ms: dur_start.elapsed().as_millis() as u64,
                };
            }
            let mut buf = [0u8; 256];
            let n = stream.read(&mut buf).unwrap_or(0);
            let line = String::from_utf8_lossy(&buf[..n]);
            let status_code = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            ProbeAttempt {
                success: (200..400).contains(&status_code),
                addresses: vec![],
                status_code,
                error_code: String::new(),
                error: String::new(),
                duration_ms: dur_start.elapsed().as_millis() as u64,
            }
        }
        Err(e) => ProbeAttempt {
            success: false,
            addresses: vec![],
            status_code: 0,
            error_code: "ConnectError".to_string(),
            error: e.to_string(),
            duration_ms: dur_start.elapsed().as_millis() as u64,
        },
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let raw = env::args().nth(1).unwrap_or_else(|| "{}".to_string());
    let req: ProbeRequest = serde_json::from_str(&raw).unwrap_or_default();
    let attempts = if req.attempts == 0 { 1 } else { req.attempts };
    let hickory = if req.runtime == "rust-hickory" {
        Some(build_hickory_resolver(req.enable_cache))
    } else {
        None
    };

    let mut out = Vec::with_capacity(attempts);
    for i in 0..attempts {
        if i > 0 && req.interval_ms > 0 {
            tokio::time::sleep(Duration::from_millis(req.interval_ms)).await;
        }
        let start = Instant::now();
        if req.op == "http_get" {
            let (uhost, uport, upath) = parse_http_url(&req.url);
            if let Some(ref resolver) = hickory {
                match resolver.ipv4_lookup(&uhost).await {
                    Ok(lookup) => {
                        if let Some(a) = lookup.iter().next() {
                            let sock = SocketAddr::new(IpAddr::V4(a.0), uport);
                            out.push(do_http_get(sock, &uhost, &upath, start));
                        } else {
                            out.push(ProbeAttempt {
                                success: false,
                                addresses: vec![],
                                status_code: 0,
                                error_code: "NoRecordsFound".to_string(),
                                error: "empty lookup".to_string(),
                                duration_ms: start.elapsed().as_millis() as u64,
                            });
                        }
                    }
                    Err(e) => {
                        out.push(ProbeAttempt {
                            success: false,
                            addresses: vec![],
                            status_code: 0,
                            error_code: "ResolveError".to_string(),
                            error: format!("ResolveError: {} ({:?})", e, e.kind()),
                            duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                }
            } else {
                match (uhost.as_str(), uport).to_socket_addrs() {
                    Ok(mut iter) => {
                        if let Some(sock) = iter.find(|s| s.is_ipv4()) {
                            out.push(do_http_get(sock, &uhost, &upath, start));
                        } else {
                            out.push(ProbeAttempt {
                                success: false,
                                addresses: vec![],
                                status_code: 0,
                                error_code: "NoIPv4".to_string(),
                                error: "no ipv4 address".to_string(),
                                duration_ms: start.elapsed().as_millis() as u64,
                            });
                        }
                    }
                    Err(e) => {
                        out.push(ProbeAttempt {
                            success: false,
                            addresses: vec![],
                            status_code: 0,
                            error_code: format!("{:?}", e.kind()),
                            error: format!("std::io::Error ({:?}): {}", e.kind(), e),
                            duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                }
            }
        } else if let Some(ref resolver) = hickory {
            match resolver.ipv4_lookup(&req.host).await {
                Ok(lookup) => {
                    let addrs: Vec<String> = lookup.iter().map(|a| a.0.to_string()).collect();
                    out.push(ProbeAttempt {
                        success: true,
                        addresses: addrs,
                        status_code: 0,
                        error_code: String::new(),
                        error: String::new(),
                        duration_ms: start.elapsed().as_millis() as u64,
                    });
                }
                Err(e) => {
                    out.push(ProbeAttempt {
                        success: false,
                        addresses: vec![],
                        status_code: 0,
                        error_code: "ResolveError".to_string(),
                        error: format!("ResolveError: {} ({:?})", e, e.kind()),
                        duration_ms: start.elapsed().as_millis() as u64,
                    });
                }
            }
        } else {
            match (req.host.as_str(), 80u16).to_socket_addrs() {
                Ok(iter) => {
                    let mut addrs = Vec::new();
                    for s in iter {
                        if let SocketAddr::V4(v4) = s {
                            let ip = v4.ip().to_string();
                            if !addrs.contains(&ip) {
                                addrs.push(ip);
                            }
                        }
                    }
                    out.push(ProbeAttempt {
                        success: true,
                        addresses: addrs,
                        status_code: 0,
                        error_code: String::new(),
                        error: String::new(),
                        duration_ms: start.elapsed().as_millis() as u64,
                    });
                }
                Err(e) => {
                    out.push(ProbeAttempt {
                        success: false,
                        addresses: vec![],
                        status_code: 0,
                        error_code: format!("{:?}", e.kind()),
                        error: format!("std::io::Error ({:?}): {}", e.kind(), e),
                        duration_ms: start.elapsed().as_millis() as u64,
                    });
                }
            }
        }
    }

    let resp = ProbeResponse { attempts: out };
    println!("{}", serde_json::to_string(&resp).unwrap());
}
