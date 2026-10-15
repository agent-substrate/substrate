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

//! Process-global actor -> worker binding cache shared across Envoy worker threads.

use std::sync::OnceLock;
use std::time::Instant;

use dashmap::DashMap;

/// A resolved actor -> worker binding stored in the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
  pub worker_ip: String,
  pub worker_spiffe_id: Option<String>,
  pub expires_at: Instant,
}

/// Returns the process-global actor -> worker binding cache shared across all
/// Envoy worker threads.
pub fn cache() -> &'static DashMap<String, Binding> {
  static CACHE: OnceLock<DashMap<String, Binding>> = OnceLock::new();
  CACHE.get_or_init(DashMap::new)
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Duration;

  #[test]
  fn test_cache_insert_lookup_and_remove() {
    let key = "test-ns/cache-unit-actor".to_string();
    let binding = Binding {
      worker_ip: "10.244.0.42".to_string(),
      worker_spiffe_id: Some("spiffe://cluster.local/ns/default/sa/worker-pool-1".to_string()),
      expires_at: Instant::now() + Duration::from_secs(5),
    };

    cache().insert(key.clone(), binding.clone());
    let entry = cache().get(&key).expect("entry should exist");
    assert_eq!(*entry, binding);
    drop(entry);

    let removed = cache().remove(&key);
    assert_eq!(removed.map(|(_, v)| v), Some(binding));
    assert!(cache().get(&key).is_none());
  }
}
