# Declaring Go environment variables

Declare production settings beside their consumers with `internal/env`:

```go
var storageBackend = env.Var[string]{
    Name:        "ATE_STORAGE_BACKEND",
    Default:     "",
    Description: "Snapshot backend: exact s3 selects S3; every other value uses GCS.",
}
```

`Get()` reads the current value; `Lookup()` also reports whether it is set.
Strings are returned unchanged. Booleans use `strconv.ParseBool`, falling back
to `Default` on invalid input. Unset variables return `Default`; explicitly
empty values are parsed. Keep special parsing at the call site: S3 path-style
addressing uses a string declaration and `Get() == "true"` to preserve its
existing behavior.

Use `Description` for relevant defaults, accepted values, and flag precedence.
The generator links each declaration to its source, so shared names can have
different descriptions in different consumers. System-injected inputs such as
`NODE_NAME` are listed in `systemVariables` in `tools/envdoc/main.go`, keeping
separate system/operator sections without additional fields on `Var`.

`Name`, `Default`, and `Description` must use literals or explicit constants in
the same file. The generator reads Go source without evaluating initializers or
reading live environment values. Never put secrets in defaults or descriptions.

Regenerate the [reference](../environment-variables.md) after changing a declaration:

```sh
hack/update/environment-variables.sh
hack/verify/environment-variables.sh
```

The initial registry covers direct reads in production components and shared
runtime packages. Setup tools, demos, tests, and dependency-owned settings are
outside this migration. `make verify` checks the generator and documentation drift.
