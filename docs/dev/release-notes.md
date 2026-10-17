# Release notes

GitHub generates the release notes for each Substrate release from the labels and titles of the merged pull requests. [`.github/release.yml`](../../.github/release.yml) defines the sections. Pushing a release tag creates a draft release with the generated notes. A maintainer edits the draft and publishes it; see [Releasing Substrate](releasing.md).

The notes are only as good as the labels. You can fetch the current labels and their descriptions using `gh`.

```sh
gh label list --repo agent-substrate/substrate --limit 200 --json name,description,color
```

## Sections

Each pull request is listed in the first section it matches, in this order:

| Section | Labels |
|---|---|
| ⚠️ Breaking Changes | `breaking-change` |
| Bug Fixes | `kind/bug` |
| Features: Networking and Egress | `area/network` |
| Features: Security and Identity | `area/security`, `area/identity` |
| Features: Observability | `area/observability` |
| Features: Workers and Actors | `area/node`, `area/gvisor`, `area/microVM`, `area/scheduling`, `area/storage`, `area/api`, `area/api-machinery` |
| Features: Install and Operations | `area/dev-infra`, `area/cli`, `area/reliability`, `area/demos`, `area/benchmarking` |
| Documentation | `kind/docs` |
| Dependencies | `dependencies` |
| Other Changes | everything else |

The core feature sections (everything except Install and Operations) run from the most specific area to the broadest. Workers and Actors comes last because `area/node` and `area/api` appear on many pull requests as secondary areas. Install and Operations come after all other feature areas. `kind/cleanup` pull requests are never listed as features; they fall through to Other Changes. Pull requests labeled `release-note/none` or `DO NOT MERGE` are left out completely.

## Write the title as a release note

The generated notes list each pull request as `<title> by @author in <link>`. Write the title for someone upgrading Substrate: say what changed for them, not how the code changed.

| Instead of | Write |
|---|---|
| `Egresspolicy impl` | `Enforce EgressPolicy in the egress gateway` |
| `multi actor worker support` | `Run more than one actor on a worker` |
| `Fix #1234` | `Fix the kind install on arm64 hosts` |

For a breaking change, describe the upgrade step in the pull request's "Breaking change" section. The release manager uses it to write the note.
