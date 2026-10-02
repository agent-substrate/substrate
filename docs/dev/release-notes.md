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

1. Tag the release commit `vX.Y.Z`, or `vX.Y.Z-rc.N` for a release candidate, and push the tag. The [`release`](../../.github/workflows/release.yaml) workflow does two things, in order:
   1. The `images` job runs [`hack/release/publish-images.sh`](../../hack/release/publish-images.sh), which builds every release image for `linux/amd64` and `linux/arm64` and pushes it to `ghcr.io/agent-substrate/substrate/<image>:<tag>`. [`hack/release/verify-images.sh`](../../hack/release/verify-images.sh) then checks that every image exists for both platforms. Most of the job's time goes to building `envoy-dataplane` for arm64 under emulation.
   2. Once the images are verified, the `draft` job runs [`hack/release/draft-release.sh`](../../hack/release/draft-release.sh), which creates a draft release. The notes cover every change since the previous `vX.Y.Z` release, give the `ate-setup` command that installs the release images, and end with a list of committers. A release candidate is marked as a prerelease.

   If the `images` job fails, no draft is created. Fix the cause and run the workflow again from the Actions tab with the tag as input.
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

## Release images

Only `agent-substrate/substrate` publishes images. In a fork the `images` job is skipped, since GitHub bills package storage to the fork's owner, and the draft is created without them.

A rerun leaves images that already exist for the tag in place. To rebuild and replace them, run the workflow manually with `force` checked. Replacing images changes their digests, so avoid it once a release is published.

To test the image build without publishing anything, for example in a fork, run the workflow manually with `dry_run_images` checked. The images are pushed to a registry that exists only inside the job and are checked there. No draft is created.

GitHub creates a new package as private. After the first release that adds an image, an owner of the `agent-substrate` organization must make the package public in its package settings, otherwise `ate-setup --image-repo` cannot read it without credentials.

To publish images by hand, check out the tag in a clean clone and run:

```sh
REPO=<registry path> hack/release/publish-images.sh v0.3.0
REPO=<registry path> hack/release/verify-images.sh v0.3.0
```

The build needs `docker buildx` with a builder that can build `linux/arm64`; [`hack/release/setup-ci-builder.sh`](../../hack/release/setup-ci-builder.sh) sets one up on an Ubuntu host.

To preview the notes before tagging, run the dry run with the tag you plan to push. If the tag does not exist yet, the notes run up to your local `HEAD`, which must already be on a branch in the repository.
