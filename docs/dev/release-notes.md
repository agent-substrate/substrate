# Release notes

GitHub generates the release notes for each Substrate release from the labels and titles of the merged pull requests. [`.github/release.yml`](../../.github/release.yml) defines the sections. Pushing a release tag creates a draft release with the generated notes. A maintainer edits the draft and publishes it.

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

## Cut a release

1. Tag the release commit `vX.Y.Z`, or `vX.Y.Z-rc.N` for a release candidate, and push the tag. The [`release`](../../.github/workflows/release.yaml) workflow runs [`hack/release/draft-release.sh`](../../hack/release/draft-release.sh), which creates a draft release. The notes cover every change since the previous `vX.Y.Z` release and end with a list of committers. A release candidate is marked as a prerelease.
2. Edit the draft:
   - Write a summary at the top.
   - Rewrite each breaking change with its upgrade step.
   - Optionally, group the feature sections under one `## Features` heading.
   - Remove Other Changes entries users do not need, and move entries that landed in the wrong section.
3. Publish the release.

If the workflow did not run, start it from the Actions tab with the tag as input, or run the script with an authenticated `gh` that has write access to the repository:

```sh
hack/release/draft-release.sh --dry-run v0.3.0   # print the notes only
hack/release/draft-release.sh v0.3.0             # create the draft
```

The script never modifies a release that already exists.

To preview the notes before tagging, run the dry run with the tag you plan to push. If the tag does not exist yet, the notes run up to your local `HEAD`, which must already be on a branch in the repository.
