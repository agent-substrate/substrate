# Declaring Go environment variables

Production Go settings are declared beside their consumers with `internal/env`.
The [generated reference](../environment-variables.md) separates operator
configuration from system-provided values, such as Downward API node identity.
The first migration covers direct reads in production components and shared
runtime packages; it excludes setup tools, demos, tests, and dependency-owned
settings.

```go
var storageBackend = env.Var[string]{
    Name:               "ATE_STORAGE_BACKEND",
    Default:            "",
    Component:          "ateapi",
    Description:        "Object storage backend for external snapshots.",
    AcceptedValues:     "Exact s3 selects S3; every other value selects GCS.",
    Precedence:         "No CLI flag.",
    DefaultDescription: "GCS",
}
```

Use `storageBackend.Get()` at the existing read site. `Lookup()` returns a typed
value and a presence boolean when the consumer must distinguish unset from empty.
Both read on each call. Declaring a variable does not read the environment.
Preserve the original read timing, parsing, validation, and flag precedence.

`Var[T]` supports the string and boolean types used by this migration. Defaults
are typed: `Var[bool]` takes a boolean default and returns a boolean. Its default
parser is `strconv.ParseBool`, falling back to the declared default on invalid
input. An optional `Parse func(string) T` keeps a consumer's existing semantics;
for example, S3 path-style addressing uses `raw == "true"`, because accepting
`TRUE` or `1` would change existing behavior. Unset variables use `Default`
without invoking `Parse`; explicitly empty values are parsed.

Record an effective default in `DefaultDescription` when the consumer resolves
it later, including component-specific differences. `AcceptedValues` documents
validation and invalid-input behavior; `Precedence` documents relevant flags and
other environment variables. These fields describe behavior; they do not change
runtime validation. Set `SystemProvided: true` for values meant to be injected by
the system, and describe their provider and missing-value behavior. Operator
settings remain operator settings when delivered through a ConfigMap.

Declare settings as `env.Var[string]{...}` or `env.Var[bool]{...}` literals.
`Name`, `Default`, `Component`, `Description`, `AcceptedValues`, and `Precedence`
are required. Metadata must use literals or explicit constants in the same file;
functions, computed values, and cross-file constants are rejected by the generator.
Keep secrets out of defaults and descriptions. Custom parser bodies are ignored.
A shared name may have multiple declarations for different consumers; duplicate
name/component pairs fail generation.

Regenerate the reference after changing a declaration:

```sh
hack/update/environment-variables.sh
hack/verify/environment-variables.sh
```

The generator uses Go's source parser, so declarations in `main` packages and
binary-private packages need no import aggregation or initialization. It scans
`cmd`, `internal`, and `pkg`, excluding test files, and reads only declaration
metadata. It never evaluates initializers, invokes custom parsers, or reads live
setting values. `make verify` checks its tests and the generated file for drift.
