# Actor Egress Policy Alloy Model

This document explains how to use
[`actor-egress-policy-model.als`](actor-egress-policy-model.als) to review the
Actor Egress Policy proposal.

The model is a bounded design-analysis artifact. It is not an implementation
of the policy engine or a complete specification of its behavior.

## Prerequisites

Install the Alloy Analyzer. The model has been tested with Alloy Analyzer
6.2.0.

Open `actor-egress-policy-model.als` in the Alloy Analyzer desktop application.
The commands at the bottom of the file will appear in the command selector.

## Running the model

Select a `check` command and click **Execute**. Run each command individually:

```alloy
check EffectiveSelectionIsUnique for 5
check CredentialResolutionStaysInAtespace for 5
check AtespaceDefaultIsPermissionCeiling for 5
check ProtectedDestinationsAlwaysDenied for 5
check MatchingEffectsAreDeterministic for 5
check HealthyLeaseNeverUsesStalePolicy for 5
check UnsupportedExtensionsFailClosed for 5
```

The `for 5` clause asks Alloy to search structures within a bounded scope of
five atoms per top-level signature, subject to Alloy's normal scope rules.

## Interpreting results

Alloy reports one of two relevant outcomes for these commands:

- **No counterexample found** means Alloy found no violation within the
  selected scope.
- A generated instance is a counterexample. Open it in the visualizer to see
  the Actors, Atespaces, policies, rules, destinations, credentials, and PEP
  state that violate the assertion.

A counterexample is not necessarily a defect in the Alloy model. Several
assertions intentionally test properties that the proposal does not guarantee.
Those counterexamples make the resulting design consequence concrete.

## Expected results

| Assertion | Expected result | Meaning |
|---|---|---|
| `EffectiveSelectionIsUnique` | No counterexample | Replacement-based policy selection produces at most one effective policy. |
| `CredentialResolutionStaysInAtespace` | No counterexample | Referenced credentials remain scoped to the Actor's Atespace. |
| `AtespaceDefaultIsPermissionCeiling` | Counterexample | An Actor policy can be more permissive than the Atespace default, as the proposal permits. |
| `ProtectedDestinationsAlwaysDenied` | Counterexample | The proposal's protected-address check applies to hostname rules but does not clearly cover every authorization path. |
| `MatchingEffectsAreDeterministic` | Counterexample | Different matching rules can produce different credential-injection effects. |
| `HealthyLeaseNeverUsesStalePolicy` | Counterexample | Stream and lease health alone do not prove that the installed policy is the latest committed policy. |
| `UnsupportedExtensionsFailClosed` | No counterexample | The modeled PEP admission condition prevents use of unsupported extensions. |

If a result differs from this table after changing the model, inspect the
counterexample or confirm that the design change intentionally altered the
invariant.

## Inspecting a counterexample

In the visualizer:

1. Identify the `Actor` and its `Atespace`.
2. Compare `actorPolicy` with `defaultPolicy` and determine which one
   `effectivePolicy` selects.
3. Inspect the selected policy's `rules` and `extensions`.
4. Follow each `Decision` to its `destination` and `selectedRule`.
5. For credential cases, follow `injections` to the `Credential` and its
   owning Atespace.
6. For delivery cases, compare `PEP.installed` with `effectivePolicy` and
   inspect stream, lease, credential, and extension state.

Use **New Instance** to ask Alloy for another counterexample. Different
instances can reveal distinct ways that the same assertion fails.

## Using the model during design review

The normal workflow is:

1. Translate a proposed requirement into a predicate, fact, or assertion.
2. Run a `check` command to search for a counterexample.
3. Decide whether the counterexample exposes a specification problem, an
   accepted design consequence, or an inaccurate model assumption.
4. Update the proposal or the model accordingly.
5. Rerun every assertion to detect semantic regressions.

For example, if the proposal defines how effects from overlapping rules are
combined, update the rule-effect logic and rerun
`MatchingEffectsAreDeterministic`. The desired outcome would then be no
counterexample within the chosen scope.

## Extending the model

Use the following Alloy constructs according to intent:

- A `sig` introduces a design entity or state object.
- A `fact` encodes an unconditional assumption or required invariant. Use
  facts sparingly because they can accidentally exclude the behavior being
  investigated.
- A `pred` defines reusable behavior or a condition that can be explored.
- A `fun` computes a relation or set.
- An `assert` states a property that should hold.
- A `check` searches for a counterexample to an assertion.
- A `run` searches for an instance satisfying a predicate and is useful for
  confirming that the model still has valid instances.

When adding an invariant, first consider whether it is already guaranteed by
the proposal. If it is merely a desired property, encode it as an assertion
before making it a fact. This allows Alloy to show whether the current design
actually guarantees it.

## Scope and limitations

Alloy performs bounded analysis. "No counterexample found" means no violation
was found within the selected scope. It is not a proof for every possible
system size.

The current model intentionally abstracts away:

- DNS resolution and caching;
- HTTP and TLS parsing;
- cryptographic identity validation;
- protobuf and xDS wire encoding;
- datastore transactions and event delivery;
- concurrency, timing, and performance; and
- implementation-specific authorization.

These areas require protocol review, threat analysis, testing, or additional
state-machine models. Increase the scope or extend the model when a design
question depends on more entities or behavior than the current abstraction
represents.
