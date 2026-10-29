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

## `generate-docs`

Generates the website's API reference. `hack/site.sh content` runs it before
each site build, writing to `site/assets/api-reference/`:

- `latest/`, from this checkout's `ateapi.proto`, and with `--releases-since`
  a directory per release tagged since, read from git. Each holds
  `reference.html`, the page body, and `nav.json`, the resource tree the
  site's sidebar lists.
- `versions.json`, the version names, newest first.
- `style.css` and `search.js`, which every version's page loads. The site's
  theme sets the colors (see `site/README.md`).

```bash
go run . generate-docs --out-dir ../../site/assets/api-reference --releases-since v1.0.0
```
