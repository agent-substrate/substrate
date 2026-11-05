---
title: "Contributing Guidelines"
linkTitle: "Contributing"
weight: 3
docsite: false
description: >
  How to set up your development environment, submit pull requests, and contribute to Agent Substrate.
---

# How to Contribute

We would love to accept your patches and contributions to this project. Before
you spend a lot of time on a contribution, please review the following
guidelines.

## Before you begin

### Sign off your commits

Every commit must be signed off under the
[Developer Certificate of Origin](https://developercertificate.org/) (DCO).
The `Signed-off-by` line certifies that you wrote the change or otherwise have
the right to submit it under the project's license.

Sign off with your real name, not an anonymous ID or a false name. It doesn't
need to be your legal name, just the name the community knows you by. See the
[CNCF DCO guidelines](https://github.com/cncf/foundation/blob/main/policies-guidance/dco-guidelines.md).

Git adds the line with `-s`, using `user.name` and `user.email` from your git
config. The name and email must match the commit author.

```sh
git commit -s -m "fix typo in README"
```

The DCO check on your PR fails if any commit is missing a sign-off. To sign off
the commits already on your branch, then force-push:

```sh
git rebase --signoff upstream/main
```

### Code of conduct

Please read and internalize our [code of conduct](../../CODE_OF_CONDUCT.md).  We want
this to be a welcoming and inclusive community, and we expect everyone to
adhere to the code of conduct at all times.  If you see someone violating the
code of conduct, please report it to the maintainers immediately.  We take all
reports seriously and will take appropriate action to address any issues.

Open, direct, honest communication is encouraged, but always be respectful and
professional.

### Review our Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google/conduct/).

### Set up a local development environment

The [Quickstart (Development)](../../README.md#quickstart-development) in the README
covers bringing up a local cluster with the default (gVisor) runtime. To run
the microVM runtime locally — which needs `/dev/kvm`, or Lima nested
virtualization on Apple Silicon — see
[docs/dev/microvm-local.md](../dev/microvm-local.md). To measure where a
suspend or resume spends its time on such a cluster, follow
[docs/dev/suspend-resume-phase-breakdown.md](../dev/suspend-resume-phase-breakdown.md).

## Collaborating

This project is intended to be "very" open.  Especially in the early stages, we
do not have a lot of infrastructure in place to manage the repository, so we are
relying on each other to be good collaborators.  To that end, here are some
guidelines for collaborating on this project.

### World-wide community

This project is not limited to one timezone, and we hope to have collaborators
from all over the world.  Please be mindful of this at all times.

This project uses English as the primary language for communication.  If
English is not your first language, please do not hesitate to ask for
clarification if you do not understand something.

### Written communication

Prefer written communication (PRs, issues, mailing-list, slack) over verbal
communication (e.g. video calls).  This allows for people who might not have
been in the conversation to understand why a decision was made the way it was.

When writing, please strive for clarity and conciseness.  Not everyone in the
community will have the same proficiency in English, so please be mindful of
that when writing.

### Using AI

For the time being, the Substrate project uses the same AI contribution policy
as [the Kubernetes
project](https://www.kubernetes.dev/docs/guide/pull-requests/#ai-guidance):

> Using AI tools to help write your PR is acceptable, but as the author, you are
> responsible for understanding every change. If you used AI tools in preparing
> your PR, you must disclose this in the description of your PR. Listing AI
> tooling as a co-author, co-signing commits using an AI tool, or using the
> assisted-by, co-developed or similar commit trailer is not allowed.

> All contributions must follow the contributions policies and use commit
> messages that align with the policy.

> Large AI generated PRs and AI generated commit messages are not allowed.

> Do not leave the first review of AI generated changes to the reviewers. Verify
> the changes (code review, testing, etc.) before submitting your PR. Reviewers
> may ask questions about your AI-assisted code, and if you cannot explain why a
> change was made, the PR will be closed.

> When responding to review comments, you must do so without relying on AI
> tools. Reviewers want to engage directly with you, not with generated
> responses. If you do not engage directly with reviewers, the PR will be
> closed.

### Working hours

We want to move quickly, but we also want to be respectful of each other's
time.  For the sake of consistency, this project's "working hours" are Monday
to Friday, 8am to 8pm, US Pacific Time (PT). Please avoid merging PRs late
at night or on weekends, unless it is an urgent fix.  This allows for people to
have a better work-life balance and prevents burnout.  If you do need to merge
something outside of normal working hours, please try to give a heads up to the
rest of the community so that they are aware.

### Ownership

As this project grows, we hope to see more and more collaborators.  We want to
encourage a culture of ownership, where people demonstrate ownership of various
parts of the codebase - a file, a package, a subsystem.  We should all try to
respect each other's ownership, but we must not become territorial.  Everyone
should be ready to track an issue or feature across the code as needed.

## Contribution process

This is a very new project, so we are still working out exactly how it is going
to be developed. For now, we are focused on iterating quickly to find the right
design and architecture. This has implications for contributors:

1) Things are moving quickly, so PRs may need to be rebased or updated
frequently. Small PRs that are focused on a single issue or feature are easier
to review and update than large PRs that touch many different parts of the
codebase.

2) While we welcome new contributors, we are really focused on the minimal
capabilities needed to make this project useful. Small fixes and cleanups can
go straight to a PR. Before you start anything larger, please discuss it with
us first (if there is an issue open, comment there and if not, open one), and
search open PRs so we are not duplicating work that is already in flight. We
want to make sure that your work is aligned with our near-term goals for the
project.

3) PRs which are not aligned with our near-term goals may be closed without
extensive review. We are not trying to be discouraging, but we need to make
sure that we are focused on the most important work.

If you are building something that runs *on* Substrate rather than changing
Substrate itself, see [Integration
Repositories](integration-repos.md) for where that code should live.

### Sizing PRs for review

We optimize PRs for easy review — large PRs get broken
down, small PRs get merged.

* **Large PRs**: split huge changes into a series of smaller PRs, each a
  logically distinct feature. When the intermediate steps are not useful
  on their own, keep the change as one PR split into commits at logical
  break points for review. PRs are squash-merged, so only the branch
  `HEAD` needs to build and pass tests.
* **Small and bulk PRs**: if you find a typo, review the whole file and
  fix everything in one pass rather than sending the single edit. Group
  related typo, doc, and single-line cleanup fixes into one PR rather
  than opening several small ones for the same area. Maintainers may ask
  you to consolidate fragmented PRs into one, or close them in favor of a
  combined submission.

As a rough scale: S is under 30 changed lines, M under 100, L under 500, XL under 1000. Most PRs should be L or smaller; XL and above are candidates for breaking down.

### Code Reviews

All submissions, including submissions by project members, require review. We
use [GitHub pull requests](https://docs.github.com/articles/about-pull-requests)
for this purpose.

We want to move quickly, but we need to balance that with quality and
maintainability.  Code reviews are an important part of that process.

Agent Substrate requires at least one approving GitHub review from a maintainer
in order for the PR to be eligible for the merge queue.  It is allowable to use
the "merge when ready" functionality to mark the PR as eligible for the merge
queue as soon as all preconditions are met.

Note that most changes to a PR will require a new approving review.  Use the
"re-request review" button to automatically place the PR back into your
reviewer's GitHub inbox.

As this community grows, we expect to see collaborators from different
companies.  Whenever possible, we want to encourage code reviews across
companies.  This helps foster collaboration and knowledge sharing across the
community, and it helps prevent silos from forming within the community.

Changes that add or alter behavior should come with tests, and existing tests
should be updated when behavior they cover changes. We will not merge code that
causes tests to fail. Every test runs in CI on every PR, so please don't add
tests whose only purpose is to raise coverage numbers. Tests should be
inherently valuable for future changes.

Follow the [PostgreSQL schema evolution rules](../dev/postgresql-schema-evolution.md) for each application schema change.

Each pull request title becomes its line in the [release notes](../dev/release-notes.md).

### Root-gated tests

Tests that need root (overlay mounts, mknod, `trusted.*` xattrs, ...) call
`roottest.Require(t, ...)` from [internal/roottest](../../internal/roottest) as
their first statement. They skip in a plain `go test ./...`; CI reruns every
package whose tests import that package under `sudo`. To run them locally:

```sh
hack/run-root-tests.sh
```

New privileged tests only need the `roottest.Require` call — no CI changes.

### Copyright Headers

Every file containing source code must include copyright and license
information. This includes any JS/CSS files that you might be serving out to
browsers. (This is to help well-intentioned people avoid accidental copying
that doesn't comply with the license.)

Our standard headers for various filetypes can be found in
[./hack/boilerplate](../../hack/boilerplate/).
