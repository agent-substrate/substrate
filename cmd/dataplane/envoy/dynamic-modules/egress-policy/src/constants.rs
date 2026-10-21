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

//! Various constants used by multiple dynamic modules.

/// Filter state key holding the initial timestamp of the policy retrieval.
pub const ATE_EGRESS_POLICY_CREATED_AT: &[u8] = b"dev.ate.egress.policy.created_at";

/// Filter state holding the egress policy hash.
pub const ATE_POLICY_EGRESS_HASH: &[u8] = b"dev.ate.policy.egress.hash";

/// Filter state key where the unshared with upstream egress policy hash is written.
pub const ATE_POLICY_EGRESS_HASH_NOT_SHARED: &[u8] = b"dev.ate.policy.egress.hash.not_shared";

/// Mode of a rule whose match terminates the connection.
pub const SNI_MODE_MITM: &str = "mitm";
