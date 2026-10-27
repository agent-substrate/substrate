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

use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

use thread_local::ThreadLocal;

use crate::egress_policy::EgressPolicy;

/// Per-thread store for egress policies.
#[derive(Debug, Default)]
pub struct EgressPolicyStore {
  // The 64 bit key is computed by Rapidhash and is shared via filter state
  // between the HTTP filter that populates policy store and inner listener
  // filter that looks up the policy by this key and enforces it.
  pub policies: HashMap<u64, Rc<EgressPolicy>>,
}

// This is needed to mollify Rust's borrows checker, as otherwise it will not
// allow retruning a mutable reference of EgressPolicyStore from the ThreadLocal store. 
// It is safe to mollify borrows checker, because EgressPolicyStore object are creted and accessed
// on the same worker thread, and never cross thread boundaries. They are also never dropped.
unsafe impl Send for EgressPolicyStore {}

impl EgressPolicyStore {
  /// Creates a new [`EgressPolicyStore`].
  pub fn new() -> Self {
    Self {
      policies: HashMap::new(),
    }
  }

  /// Looks up a policy by key and returns a cloned [`Rc<EgressPolicy>`] if present.
  pub fn lookup(&self, key: u64) -> Option<Rc<EgressPolicy>> {
    self.policies.get(&key).cloned()
  }

  /// Stores a cloned [`Rc<EgressPolicy>`] under `key`.
  pub fn store(&mut self, key: u64, policy: &Rc<EgressPolicy>) {
    self.policies.insert(key, Rc::clone(policy));
  }

  /// Removes the policy under `key` and returns it, or `None` if not present.
  pub fn remove(&mut self, key: u64) -> Option<Rc<EgressPolicy>> {
    self.policies.remove(&key)
  }
}

/// Global store for egress policies holding a thread-local [`EgressPolicyStore`].
#[derive(Debug, Default)]
pub struct GlobalEgressPolicyStore {
  pub store: ThreadLocal<RefCell<EgressPolicyStore>>,
}

impl GlobalEgressPolicyStore {
  /// Creates a new [`GlobalEgressPolicyStore`].
  pub const fn new() -> Self {
    Self {
      store: ThreadLocal::new(),
    }
  }

  /// Returns a mutable reference to the current thread's [`EgressPolicyStore`],
  /// initializing it on first access.
  pub fn get_local_store() -> RefMut<'static, EgressPolicyStore> {
    GLOBAL_EGRESS_POLICY_STORE
      .store
      .get_or(|| RefCell::new(EgressPolicyStore::new()))
      .borrow_mut()
  }
}

/// Process-wide [`GlobalEgressPolicyStore`] instance.
pub static GLOBAL_EGRESS_POLICY_STORE: GlobalEgressPolicyStore = GlobalEgressPolicyStore::new();

// ==========================================
// Tests
// ==========================================

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
      hash: 1,
    });
    GlobalEgressPolicyStore::get_local_store().store(policy.hash, &policy);
    let looked_up = GlobalEgressPolicyStore::get_local_store().lookup(policy.hash);
    assert_eq!(looked_up, Some(Rc::clone(&policy)));
    assert!(Rc::ptr_eq(&looked_up.unwrap(), &policy));
    assert_eq!(GlobalEgressPolicyStore::get_local_store().lookup(2), None);

    let removed = GlobalEgressPolicyStore::get_local_store().remove(policy.hash);
    assert_eq!(removed, Some(Rc::clone(&policy)));
    assert!(Rc::ptr_eq(&removed.unwrap(), &policy));
    assert_eq!(GlobalEgressPolicyStore::get_local_store().lookup(policy.hash), None);
    assert_eq!(GlobalEgressPolicyStore::get_local_store().remove(policy.hash), None);
  }
}
