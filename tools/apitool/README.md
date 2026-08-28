# Substrate API Tool (`apitool`)

The `apitool` is a CLI that automates tasks related to the Substrate API.

## Usage

```bash
cd tools/apitool
go run . validate
```

`validate` runs the rules in `internal/lint` against `ateapi.proto` and fails if any
rule reports a finding that isn't exempted (see below).

### Exemptions

Findings can be exempted via `exemptions.json`. To regenerate `exemptions.json`
so it matches every current finding exactly (e.g. after fixing some findings, or
before adding new ones you plan to exempt), run:

```bash
go run . validate --update
```

Review the diff to `exemptions.json` like any other change: a shrinking diff means
findings got fixed, a growing one means new violations are being exempted rather
than fixed.

The matching is strict meaning that both an extra exemption and a missing one will
cause a failure. This helps during review to guarantee that the changes in the
exemptions file really reflects the state of the system.

A unit test enforces this same strict matching as a presubmit gate.

```bash
go test ./internal/validate/... -run TestExemptions
```
