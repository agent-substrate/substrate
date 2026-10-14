# gVisor sources for the CPU feature tables

Verbatim copies of two files from [gVisor](https://github.com/google/gvisor),
`pkg/cpuid/features_amd64.go` and `pkg/cpuid/features_arm64.go`, plus its
`LICENSE`. They are the input to `tools/cpufeatures`, which generates
`internal/hardware/zz_generated.cpufeatures.go`: the names `runsc cpu-features`
prints, in the order that fixes each name's bit in the `cpu_features`
compatibility attribute. Nothing compiles them; `go.mod` exists only so root
`./...` package walks skip the partial package.

- `VERSION` names the `gvisor.dev/gvisor` module version the files came from
  and is the only pin.
- `SHA256SUMS` lets the generator check the copies are the bytes a fetch wrote.
  Do not edit the copies by hand; the generator refuses them if you do.

To refresh: `GVISOR_VERSION=<module version> hack/update/cpufeatures.sh`. See
that script's header for what to expect in the diff.
