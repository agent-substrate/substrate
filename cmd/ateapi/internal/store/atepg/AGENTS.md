# atepg

This is the database layer. It stores and retrieves data from the database. It
should not have logic that is not related to this.

- Never put API validation logic here. That belongs in the API layers.
- Always include tests, in particular for edge cases, consistency and
  concurrency, error handling and performance.
- All type-specific code should go in the type's own file(s). For example, all
  Actor related code goes in `actor.go` and all Actor related tests go in
  `actor_test.go`.
