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

module actor_egress_policy

// Bounded review model for the Actor Egress Policy Proposal.
// This is an analysis artifact, not an implementation specification.

sig Atespace {
  defaultPolicy: lone Policy
}

sig Actor {
  space: one Atespace,
  actorPolicy: lone Policy
}

sig Host {}
sig Address {}
sig ProtectedAddress in Address {}
sig Header {}

sig Destination {
  authority: lone Host,
  address: one Address
}

sig Credential {
  owner: one Atespace
}

sig Injection {
  header: one Header,
  credential: one Credential
}

sig Extension {}

sig Policy {
  rules: set Rule,
  extensions: set Extension
}

abstract sig Rule {}
one sig AllowAll extends Rule {}

abstract sig HostRule extends Rule {
  matchedHosts: set Host
}

sig ExactHostRule extends HostRule {
  injections: set Injection
}

sig WildcardHostRule extends HostRule {}

sig CIDRRule extends Rule {
  addresses: set Address
}

// The effective-policy selection is replacement, not merging.
fun effectivePolicy[a: Actor]: lone Policy {
  { p: Policy |
    p = a.actorPolicy or
    (no a.actorPolicy and p = a.space.defaultPolicy)
  }
}

pred ruleMatches[r: Rule, d: Destination] {
  r = AllowAll or
  (r in HostRule and some d.authority and d.authority in r.matchedHosts) or
  (r in CIDRRule and d.address in r.addresses)
}

// The proposal explicitly protects hostname resolution from loopback,
// link-local, and unspecified results, but does not apply that prohibition to
// a directly matching CIDR rule.
pred proposalPortableAllows[a: Actor, d: Destination] {
  some p: effectivePolicy[a] |
    some r: p.rules |
      ruleMatches[r, d] and
      (r not in HostRule or d.address not in ProtectedAddress)
}

fun policyInjections[p: set Policy]: set Injection {
  (p.rules & ExactHostRule).injections
}

fun policyCredentials[p: set Policy]: set Credential {
  policyInjections[p].credential
}

// CredentialReference.name is resolved within the Actor's Atespace.
fact ScopedCredentialResolution {
  all a: Actor |
    all i: policyInjections[effectivePolicy[a]] |
      i.credential.owner = a.space
}

// A Decision models an implementation that selects one matching OR rule as
// the authorizing rule. The proposal does not state whether effects are taken
// from one rule or the union of every matching rule.
sig Decision {
  actor: one Actor,
  destination: one Destination,
  selectedRule: one Rule
}

fact ValidDecision {
  all d: Decision |
    some p: effectivePolicy[d.actor] |
      d.selectedRule in p.rules and
      ruleMatches[d.selectedRule, d.destination]
}

fun decisionInjections[d: Decision]: set Injection {
  (d.selectedRule & ExactHostRule).injections
}

sig PEP {
  installed: Actor -> lone Policy,
  healthyStream: set Actor,
  validLease: set Actor,
  installedCredentials: set Credential,
  supportedExtensions: set Extension
}

// This reflects the proposal's local admission conditions. It intentionally
// has no omniscient test that the installed version is still authoritative.
pred proposalPEPMayAllow[pep: PEP, a: Actor, d: Destination] {
  a in pep.healthyStream
  a in pep.validLease
  some pep.installed[a]
  some r: pep.installed[a].rules | ruleMatches[r, d]
  policyCredentials[pep.installed[a]] in pep.installedCredentials
  pep.installed[a].extensions in pep.supportedExtensions
}

assert EffectiveSelectionIsUnique {
  all a: Actor | lone effectivePolicy[a]
}

assert CredentialResolutionStaysInAtespace {
  all a: Actor |
    policyCredentials[effectivePolicy[a]].owner in a.space
}

// Expected counterexample: Actor replacement may be more permissive than the
// Atespace default, which is expressly allowed by the proposal.
assert AtespaceDefaultIsPermissionCeiling {
  all a: Actor, d: Destination |
    proposalPortableAllows[a, d] implies
      (some a.space.defaultPolicy implies
        some r: a.space.defaultPolicy.rules | ruleMatches[r, d])
}

// Expected counterexample: a directly allowed CIDR can contain a protected
// address even though hostname rules reject the same resolved address.
assert ProtectedDestinationsAlwaysDenied {
  all a: Actor, d: Destination |
    d.address in ProtectedAddress implies
      not proposalPortableAllows[a, d]
}

// Expected counterexample: two matching OR rules can select different
// injection effects for the same Actor and destination.
assert MatchingEffectsAreDeterministic {
  all disj x, y: Decision |
    x.actor = y.actor and x.destination = y.destination implies
      decisionInjections[x] = decisionInjections[y]
}

// Expected counterexample: stream and lease health alone cannot reveal a
// committed policy version that has not yet reached the PEP.
assert HealthyLeaseNeverUsesStalePolicy {
  all pep: PEP, a: Actor, d: Destination |
    proposalPEPMayAllow[pep, a, d] implies
      pep.installed[a] = effectivePolicy[a]
}

// This should hold because unsupported extensions are excluded by the local
// admission predicate.
assert UnsupportedExtensionsFailClosed {
  all pep: PEP, a: Actor, d: Destination |
    proposalPEPMayAllow[pep, a, d] implies
      pep.installed[a].extensions in pep.supportedExtensions
}

check EffectiveSelectionIsUnique for 5
check CredentialResolutionStaysInAtespace for 5
check AtespaceDefaultIsPermissionCeiling for 5
check ProtectedDestinationsAlwaysDenied for 5
check MatchingEffectsAreDeterministic for 5
check HealthyLeaseNeverUsesStalePolicy for 5
check UnsupportedExtensionsFailClosed for 5
