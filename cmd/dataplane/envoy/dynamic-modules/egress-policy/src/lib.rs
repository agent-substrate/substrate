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

//! The egress-policy dynamic module provides listener and HTTP filters for
//! egress policy evaluation and storage. See the README in this directory.

pub mod egress_policy;
pub mod egress_policy_store;
pub mod listener_filter;
pub mod policy_store_filter;

use envoy_proxy_dynamic_modules_rust_sdk::declare_all_init_functions;
pub use egress_policy::*;
pub use egress_policy_store::*;
pub use listener_filter::*;
pub use policy_store_filter::*;

declare_all_init_functions!(
  init,
  http: policy_store_filter::new_http_filter_config_fn,
  listener: listener_filter::new_listener_filter_config_fn,
);

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
