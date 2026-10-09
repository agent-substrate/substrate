# Releasing Substrate

A release is a `vX.Y.Z` tag, or `vX.Y.Z-rc.N` for a release candidate, with container images in `ghcr.io/agent-substrate/substrate` and a GitHub release whose notes are generated from the merged pull requests. How the notes are generated, and how to label and title a pull request for them, is in [Release notes](release-notes.md).

## Cut a release

1. Tag the release commit and push the tag. The [`release`](../../.github/workflows/release.yaml) workflow does two things, in order:
   1. The `images` job runs `ate-setup publish release-images`, which builds every release image for `linux/amd64` and `linux/arm64`, pushes it to `ghcr.io/agent-substrate/substrate/<image>:<tag>`, and then checks that every image exists for both platforms. Most of the job's time goes to building `envoy-dataplane` for arm64 under emulation.
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

To preview the notes before tagging, run the dry run with the tag you plan to push. If the tag does not exist yet, the notes run up to your local `HEAD`, which must already be on a branch in the repository.

## Release images

Only `agent-substrate/substrate` publishes images. In a fork the `images` job is skipped, since GitHub bills package storage to the fork's owner, and the draft is created without them.

A rerun builds nothing if every image already exists for the tag on both platforms. An incomplete set, left by a failed run, is rebuilt in full. To rebuild and replace a complete set, run the workflow manually with `force` checked. Replacing images changes their digests, so avoid it once a release is published.

To test the image build without publishing anything, for example in a fork, run the workflow manually with `dry_run_images` checked. The images are pushed to a registry that exists only inside the job and are checked there. No draft is created.

GitHub creates a new package as private. After the first release that adds an image, an owner of the `agent-substrate` organization must make the package public in its package settings, otherwise `ate-setup --image-repo` cannot read it without credentials.

To publish images by hand, check out the tag in a clean clone, log in to the registry with `docker login`, and run:

```sh
VERSION=v0.3.0 KO_DEFAULTPLATFORMS=linux/amd64,linux/arm64 \
  go run ./cmd/ate-setup publish release-images --ko-docker-repo <registry path>
```

The images are built from the work tree, so it must hold the tag and nothing else. The build needs `docker buildx` with a builder that can push multi-platform images and run `linux/arm64` binaries under QEMU; the `images` job in the workflow shows the setup on an Ubuntu host.
