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

//! Per-thread actor -> worker binding cache.
//!
//! Each Envoy worker thread has its own cache. The HTTP filter and the
//! `tunnel_watcher` network filter for a request run on the same thread
//! (the internal listener connection is accepted on the thread that opened it),
//! and so do Envoy's connection pools, so the network filter's marks always land
//! in the cache the request was routed from.
//!
//! A request routed from the cache holds an [`Rc`] to its [`Entry`] until the
//! request ends. The cache keeps two maps:
//!
//! * current bindings, `actor key -> Rc<Entry>`, which new requests look up;
//! * live entries, `(actor key, worker IP) -> [Weak<Entry>]`, which the network
//!   filter marks. An entry stays reachable here while any request holds it,
//!   even after it leaves the current bindings.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

/// A resolved actor -> worker binding.
#[derive(Debug)]
pub struct Entry {
  pub worker_ip: String,
  pub worker_spiffe_id: Option<String>,
  pub expires_at: Instant,
  /// Set when a tunnel for this binding was refused before it was established.
  stale: Cell<bool>,
  /// Set when an established tunnel for this binding closed while a request was
  /// waiting for its response, so that request may have reached the actor.
  spoiled: Cell<bool>,
}

impl Entry {
  pub fn is_stale(&self) -> bool {
    self.stale.get()
  }

  pub fn is_spoiled(&self) -> bool {
    self.spoiled.get()
  }

  /// Reports whether a request routed from this entry whose connection ended
  /// before a response can be replayed: a tunnel for the binding was refused,
  /// and no request on it may have reached the actor.
  pub fn refused_and_unspoiled(&self) -> bool {
    self.is_stale() && !self.is_spoiled()
  }
}

/// The result of looking up an actor in the cache.
pub enum Lookup {
  /// An unexpired binding.
  Hit(Rc<Entry>),
  /// A binding that had expired and has been removed.
  Expired,
  /// No binding.
  Miss,
}

/// How often a full cache may scan for expired bindings, so a cache full of
/// unexpired bindings doesn't rescan on every miss.
const FULL_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Cache {
  bindings: HashMap<String, Rc<Entry>>,
  live: HashMap<(String, String), Vec<Weak<Entry>>>,
  last_full_sweep: Option<Instant>,
}

impl Cache {
  /// Reports whether `key` can be bound under `max_actors`: it already is, or
  /// there's room, possibly after dropping expired bindings.
  fn has_room_for(&mut self, key: &str, now: Instant, max_actors: usize) -> bool {
    if self.bindings.len() < max_actors || self.bindings.contains_key(key) {
      return true;
    }
    if self
      .last_full_sweep
      .is_some_and(|last| now < last + FULL_SWEEP_INTERVAL)
    {
      return false;
    }
    self.last_full_sweep = Some(now);
    self.bindings.retain(|_, entry| entry.expires_at > now);
    self.bindings.len() < max_actors
  }

  /// Drops live-entry slots whose entries have been freed. Runs a full sweep
  /// only when dead slots may outnumber live bindings, so the cost is amortized.
  fn maybe_sweep(&mut self) {
    if self.live.len() <= 2 * self.bindings.len() + 64 {
      return;
    }
    self.live.retain(|_, entries| {
      entries.retain(|e| e.strong_count() > 0);
      !entries.is_empty()
    });
  }

  /// Applies `mark` to every live entry for `(actor, worker_ip)` and returns how
  /// many entries it marked.
  fn mark(&mut self, actor: &str, worker_ip: &str, mark: impl Fn(&Entry)) -> usize {
    let Some(entries) = self.live.get_mut(&(actor.to_string(), worker_ip.to_string())) else {
      return 0;
    };
    let mut marked = 0;
    entries.retain(|weak| match weak.upgrade() {
      Some(entry) => {
        mark(&entry);
        marked += 1;
        true
      }
      None => false,
    });
    marked
  }
}

thread_local! {
  static CACHE: RefCell<Cache> = RefCell::default();
}

/// Looks up the current binding for `key`, removing it if it expired by `now`.
pub fn lookup(key: &str, now: Instant) -> Lookup {
  CACHE.with_borrow_mut(|cache| match cache.bindings.get(key) {
    Some(entry) if entry.expires_at > now => Lookup::Hit(Rc::clone(entry)),
    Some(_) => {
      cache.bindings.remove(key);
      Lookup::Expired
    }
    None => Lookup::Miss,
  })
}

/// Returns the current binding for `key`, expired or not.
pub fn current(key: &str) -> Option<Rc<Entry>> {
  CACHE.with_borrow(|cache| cache.bindings.get(key).cloned())
}

/// Makes a new binding for `key` current, replacing any previous one, and returns it.
/// If the cache already binds `max_actors` unexpired actors, the entry isn't cached.
pub fn insert(
  key: String,
  worker_ip: String,
  worker_spiffe_id: Option<String>,
  ttl: Duration,
  max_actors: usize,
) -> Rc<Entry> {
  let now = Instant::now();
  let entry = Rc::new(Entry {
    worker_ip,
    worker_spiffe_id,
    expires_at: now + ttl,
    stale: Cell::new(false),
    spoiled: Cell::new(false),
  });
  CACHE.with_borrow_mut(|cache| {
    if !cache.has_room_for(&key, now, max_actors) {
      return;
    }
    let slot = cache
      .live
      .entry((key.clone(), entry.worker_ip.clone()))
      .or_default();
    slot.retain(|e| e.strong_count() > 0);
    slot.push(Rc::downgrade(&entry));
    cache.bindings.insert(key, Rc::clone(&entry));
    cache.maybe_sweep();
  });
  entry
}

/// Removes the current binding for `key` if it is `entry`, so a newer binding
/// learned by another request is left alone. Reports whether it removed it.
pub fn remove_if_current(key: &str, entry: &Rc<Entry>) -> bool {
  CACHE.with_borrow_mut(|cache| {
    if cache
      .bindings
      .get(key)
      .is_some_and(|current| Rc::ptr_eq(current, entry))
    {
      cache.bindings.remove(key);
      true
    } else {
      false
    }
  })
}

/// Marks every live entry for `(actor, worker_ip)` stale and removes the current
/// binding if it points at that worker, so new requests resolve through
/// `ext_proc`. Returns how many entries it marked.
pub fn mark_stale(actor: &str, worker_ip: &str) -> usize {
  CACHE.with_borrow_mut(|cache| {
    let marked = cache.mark(actor, worker_ip, |e| e.stale.set(true));
    if cache
      .bindings
      .get(actor)
      .is_some_and(|current| current.worker_ip == worker_ip)
    {
      cache.bindings.remove(actor);
    }
    marked
  })
}

/// Marks every live entry for `(actor, worker_ip)` spoiled. Returns how many
/// entries it marked.
pub fn mark_spoiled(actor: &str, worker_ip: &str) -> usize {
  CACHE.with_borrow_mut(|cache| cache.mark(actor, worker_ip, |e| e.spoiled.set(true)))
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;

  // Each Rust test runs on its own thread, so each test starts with an empty cache.

  const MAX: usize = 100;

  #[test]
  fn test_insert_lookup_and_remove() {
    let entry = insert(
      "ns/a".to_string(),
      "10.244.0.42".to_string(),
      Some("spiffe://cluster.local/ns/default/sa/worker-pool-1".to_string()),
      Duration::from_secs(5),
      MAX,
    );
    match lookup("ns/a", Instant::now()) {
      Lookup::Hit(hit) => assert!(Rc::ptr_eq(&hit, &entry)),
      _ => panic!("expected a hit"),
    }
    assert!(remove_if_current("ns/a", &entry));
    assert!(matches!(lookup("ns/a", Instant::now()), Lookup::Miss));
  }

  #[test]
  fn test_lookup_removes_expired_binding() {
    insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::ZERO, MAX);
    assert!(matches!(
      lookup("ns/a", Instant::now() + Duration::from_millis(1)),
      Lookup::Expired
    ));
    assert!(current("ns/a").is_none());
  }

  #[test]
  fn test_remove_if_current_keeps_newer_binding() {
    let old = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    let new = insert("ns/a".to_string(), "10.0.0.2".to_string(), None, Duration::from_secs(5), MAX);
    assert!(!remove_if_current("ns/a", &old));
    assert!(Rc::ptr_eq(&current("ns/a").unwrap(), &new));
  }

  #[test]
  fn test_mark_stale_marks_held_entry_and_removes_binding() {
    let held = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    assert_eq!(mark_stale("ns/a", "10.0.0.1"), 1);
    assert!(held.is_stale());
    assert!(!held.is_spoiled());
    assert!(held.refused_and_unspoiled());
    assert!(current("ns/a").is_none());
  }

  #[test]
  fn test_mark_stale_ignores_other_worker() {
    let on_a = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    assert_eq!(mark_stale("ns/a", "10.0.0.2"), 0);
    assert!(!on_a.is_stale());
    assert!(current("ns/a").is_some());
  }

  #[test]
  fn test_marks_reach_entry_held_after_newer_binding_replaces_it() {
    // A request still holds the worker-A entry after ext_proc learned worker B.
    let old = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    let new = insert("ns/a".to_string(), "10.0.0.2".to_string(), None, Duration::from_secs(5), MAX);
    assert_eq!(mark_spoiled("ns/a", "10.0.0.1"), 1);
    assert!(old.is_spoiled());
    assert!(!new.is_spoiled());
    // The current binding points at worker B, so a stale mark for A keeps it.
    assert_eq!(mark_stale("ns/a", "10.0.0.1"), 1);
    assert!(Rc::ptr_eq(&current("ns/a").unwrap(), &new));
  }

  #[test]
  fn test_marks_reach_every_live_entry_for_the_same_worker() {
    // The actor moved away from worker A and back while a request held the old entry.
    let first = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    let second = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    assert_eq!(mark_spoiled("ns/a", "10.0.0.1"), 2);
    assert!(first.is_spoiled());
    assert!(second.is_spoiled());
  }

  #[test]
  fn test_freed_entries_are_not_marked() {
    let entry = insert("ns/a".to_string(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
    assert!(remove_if_current("ns/a", &entry));
    drop(entry);
    assert_eq!(mark_stale("ns/a", "10.0.0.1"), 0);
  }

  #[test]
  fn test_sweep_drops_dead_live_slots() {
    for i in 0..200 {
      let key = format!("ns/actor-{i}");
      let entry = insert(key.clone(), "10.0.0.1".to_string(), None, Duration::from_secs(5), MAX);
      remove_if_current(&key, &entry);
    }
    let live = CACHE.with_borrow(|cache| cache.live.len());
    assert!(live <= 65, "dead live slots were not swept: {live}");
  }

  fn fill(ttl: Duration) {
    for i in 0..MAX {
      insert(format!("ns/actor-{i}"), "10.0.0.1".to_string(), None, ttl, MAX);
    }
  }

  #[test]
  fn test_full_cache_skips_new_actor_but_rebinds_known_ones() {
    fill(Duration::from_secs(5));
    insert("ns/new".to_string(), "10.0.0.2".to_string(), None, Duration::from_secs(5), MAX);
    assert!(current("ns/new").is_none());
    let rebound = insert("ns/actor-0".to_string(), "10.0.0.2".to_string(), None, Duration::from_secs(5), MAX);
    assert!(Rc::ptr_eq(&current("ns/actor-0").unwrap(), &rebound));
    assert_eq!(CACHE.with_borrow(|cache| cache.bindings.len()), MAX);
  }

  #[test]
  fn test_full_cache_drops_expired_bindings_to_make_room() {
    fill(Duration::ZERO);
    let entry = insert("ns/new".to_string(), "10.0.0.2".to_string(), None, Duration::from_secs(5), MAX);
    assert!(Rc::ptr_eq(&current("ns/new").unwrap(), &entry));
    assert_eq!(CACHE.with_borrow(|cache| cache.bindings.len()), 1);
  }
}
