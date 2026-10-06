# Mac guest agent

`macguestagent` is installed in the macOS base image and started by `launchd`.
It mounts the read-only `ate-config` VirtioFS share, reads `volumes.json`, and
mounts every externally durable share at the path declared by the
`ActorTemplate`. `GET /ready` returns 200 only while all configured shares are
mounted; `GET /healthz` reports whether the agent process is serving.

Build an Apple Silicon binary and install it in the powered-off image:

```sh
GOOS=darwin GOARCH=arm64 go build -o macguestagent ./cmd/macguestagent
sudo install -o root -g wheel -m 0755 macguestagent /usr/local/libexec/macguestagent
sudo install -o root -g wheel -m 0644 \
  cmd/macguestagent/com.agent-substrate.macguestagent.plist \
  /Library/LaunchDaemons/com.agent-substrate.macguestagent.plist
```

The image must also grant this LaunchDaemon Full Disk Access, or an equivalent
managed PPPC policy that permits its VirtioFS access. macOS System Policy blocks
an unapproved background daemon from reading these shares even as root; the
standard network- and removable-volume code-signing entitlements do not bypass
that policy.

The image's Mac Actor wakeup probe must use port `8123` and path `/ready`.
The agent refuses malformed configuration, nested mounts, symlink mount points,
and non-empty directories that a mount would hide. It continuously reconciles
mounts, so readiness drops if a share disappears.
