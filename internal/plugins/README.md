# Plugins

Each directory here is a self-contained plugin: a Go module of its own that
plugs into substrate through its public APIs.

This is a temporary location. Each plugin will move to a repository of its
own, and its path here goes away when it does. Until then:

- **Do not depend on these paths.** Nothing else in the repository should
  build on, script against or link into a plugin's directory. The only
  references from outside a plugin are its own CI steps and pointers from the
  docs.
- **Do not import these packages.** Code outside a plugin must not import it.
  Go already refuses most such imports, since a plugin's packages sit under its
  own `internal/` or are `main` packages.
- **A plugin imports only substrate's public `pkg/` packages**, so it can move
  out as it is. Its `go.mod` replaces the substrate module with this checkout,
  so it builds against the current tree.

