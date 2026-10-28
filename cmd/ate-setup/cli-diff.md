# `hack/install-ate.sh` after the move to `ate-setup`

`hack/install-ate.sh` no longer installs anything. It merely directly invokes
`go run ./cmd/ate-setup`.  New logic should not be added to it.