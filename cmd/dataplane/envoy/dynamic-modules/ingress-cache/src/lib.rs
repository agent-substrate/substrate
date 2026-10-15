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

//! The ingress-cache dynamic module provides an HTTP filter that caches
//! actor -> worker bindings to bypass `ext_proc` on warm ingress paths.

pub mod cache;
pub mod config;
pub mod filter;
pub mod routing;

pub use cache::*;
pub use config::*;
use envoy_proxy_dynamic_modules_rust_sdk::declare_init_functions;
pub use filter::*;
pub use routing::*;

declare_init_functions!(init, filter::new_http_filter_config_fn);

/// Called when the dynamic module is loaded into Envoy.
fn init() -> bool {
  true
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_init() {
    assert!(init());
  }
}
