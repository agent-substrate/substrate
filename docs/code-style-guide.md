# Code Style Guide

Go conventions for this repository — written for humans and coding agents
alike. The [API style guide](api-style-guide.md) governs the proto surface;
this document governs the Go code behind it. Repository layout is covered by
[code layout](dev/code-layout.md).

The baseline is [Effective Go](https://go.dev/doc/effective_go), the
[Google Go style guide](https://google.github.io/styleguide/go/), and `gofmt`.
This document records only decisions that are specific to this project.

## Proto field access: check presence, don't default it

Generated getters (e.g. `req.GetActor().GetName()`) return zero values when any
message in the chain is nil. That is convenient and dangerous: a required
message the caller forgot to set silently becomes `""`, and the bug surfaces
far from its cause.

- A getter chain must never stand in for a presence check. Whenever a field
  is required at this point in the code, check it explicitly and fail loudly:
  `INVALID_ARGUMENT` at the API boundary, a real error (not a guessed zero
  value) elsewhere.
- Once the boundary has validated presence, getters downstream are fine, as is
  the guarded form for optional messages:

  ```go
  if wass := worker.Assignment; wass != nil {
      // Fields of wass were validated on write; use them directly.
  }
  ```

- Absence of a group of fields that are set and cleared together is
  represented by a nil message (`worker.Assignment == nil`), never by probing
  a scalar field inside it for its zero value.

## Testing

- Standard library `testing` only — no assertion or mocking frameworks.
- Table-driven tests with `t.Run` subtests are the default shape.
- Prefer a real fake when one exists: `miniredis` for the store, `envtest` for
  the Kubernetes API. Release resources with `t.Cleanup`.

## TODOs

Deferred decisions are recorded in code as `TODO(<issue-number>): ...`, placed where
the decision will eventually have to be made.
