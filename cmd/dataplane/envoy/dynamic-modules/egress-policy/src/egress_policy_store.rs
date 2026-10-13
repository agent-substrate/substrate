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

//! Global store for egress policies shared across filters in the dynamic module.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::thread::LocalKey;

use crate::egress_policy::EgressPolicy;

thread_local! {
  static LOCAL_EGRESS_POLICY_STORE: RefCell<EgressPolicyStore> =
    RefCell::new(EgressPolicyStore::new());
}

/// Per-thread store for egress policies.
#[derive(Debug, Default)]
pub struct EgressPolicyStore {
  pub policies: HashMap<u64, Rc<EgressPolicy>>,
}

impl EgressPolicyStore {
  /// Creates a new [`EgressPolicyStore`].
  pub fn new() -> Self {
    Self {
      policies: HashMap::new(),
    }
  }
}

/// Global store for egress policies holding a thread-local [`EgressPolicyStore`].
#[derive(Debug)]
pub struct GlobalEgressPolicyStore {
  pub store: &'static LocalKey<RefCell<EgressPolicyStore>>,
}

impl GlobalEgressPolicyStore {
  /// Creates a new [`GlobalEgressPolicyStore`].
  pub const fn new() -> Self {
    Self {
      store: &LOCAL_EGRESS_POLICY_STORE,
    }
  }
}

impl Default for GlobalEgressPolicyStore {
  fn default() -> Self {
    Self::new()
  }
}

/// Process-wide [`GlobalEgressPolicyStore`] instance.
pub static GLOBAL_EGRESS_POLICY_STORE: GlobalEgressPolicyStore = GlobalEgressPolicyStore::new();

#[cfg(test)]
mod tests {
  use super::*;
  use crate::egress_policy::SniRule;

  #[test]
  fn test_global_egress_policy_store() {
    let policy = Rc::new(EgressPolicy {
      rules: vec![SniRule {
        pattern: "api.example.com".to_string(),
        mode: "mitm".to_string(),
      }],
    });
    GLOBAL_EGRESS_POLICY_STORE.store.with(|store| {
      store.borrow_mut().policies.insert(1, Rc::clone(&policy));
      assert_eq!(store.borrow().policies.get(&1), Some(&policy));
    });
  }
}
