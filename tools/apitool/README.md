# Substrate API Tool (`apitool`)

A CLI for tasks related to the Substrate API. Run it from `tools/apitool`.

## `validate`

Checks `ateapi.proto` against the rules in `internal/lint`, failing on any
finding not listed in `exemptions.json`.

```bash
go run . validate
```

To rewrite `exemptions.json` to match the current findings:

```bash
go run . validate --update
```

## `generate-docs`

Generates the API reference page, `docs/api-reference.html`, from
`ateapi.proto`. `hack/update/codegen.sh` runs it.

```bash
go run . generate-docs --out ../../docs/api-reference.html
```
