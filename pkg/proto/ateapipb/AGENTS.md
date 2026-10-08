# ateapipb

`ateapi.proto` defines Substrate's public API.

- See `docs/api-style-guide.md` for the API style guide.
- See `docs/resource-model.md` for the resources and subresources the API exposes. Update it when you add, remove, or re-parent a resource.
- See `docs/api-validation.md` for how to use declarative validation (DV) tags.
- `*.pb.go` files are generated. Never edit them by hand. After changing `ateapi.proto`, run `hack/update/codegen.sh`, which also regenerates the DV code in `cmd/ateapi/internal/apivalidation`.
