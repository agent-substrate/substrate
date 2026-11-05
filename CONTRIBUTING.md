# How to Contribute

We would love to accept your patches and contributions to this project. Before
you spend a lot of time on a contribution, please review this document, so you
can understand the project's norms and culture.

## Before you begin

### Code of conduct

Please read and internalize our [code of conduct](CODE_OF_CONDUCT.md). We want
this to be a welcoming and inclusive community, and we expect everyone to
adhere to the code of conduct at all times. If you see someone violating the
code of conduct, please report it to the maintainers immediately. We take all
reports seriously and will take appropriate action to address any issues.

Open, direct, honest communication is encouraged, but always be respectful and
professional.

## Culture and Collaboration

This project is intended to be "very" open. Especially in the early stages, we
do not have a lot of infrastructure in place to manage the repository, so we are
relying on each other to be good collaborators. To that end, here are some
guidelines for collaborating on this project.

### World-wide community

This project is not limited to one timezone, and we hope to have collaborators
from all over the world. Please be mindful of this at all times.

This project uses English as the primary language for communication. If
English is not your first language, please do not hesitate to ask for
clarification if you do not understand something.

### Written communication

Prefer written communication (PRs, issues, mailing-list, slack) over verbal
communication (e.g. video calls). This allows for people who might not have
been in the conversation to understand why a decision was made the way it was.

When writing, please strive for clarity and conciseness. Not everyone in the
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
time. For the sake of consistency, this project's "working hours" are Monday
to Friday, 8am to 8pm, US Pacific Time (PT). We know that this is not ideal
for everyone in our global community, but it at least gives us some
consistency. If you are working outside of these hours, please be mindful of
the fact that other people may not be available to respond to your questions or
comments in real time. We will try to schedule real time events, such as
community meetings at times that are less disruptive to the larger community,
but we cannot always accommodate everyone's schedule.

### Ownership

As this project grows, we hope to see more and more collaborators. We want to
encourage a culture of ownership, where people demonstrate ownership of various
parts of the codebase - a file, a package, a subsystem. We should all try to
respect each other's ownership, but we must not become territorial. Everyone
should be ready to track an issue or feature across the code as needed.

## Development

Every project eventually grows its own development workflow, and this project
is no exception. Here are some hard rules and some softer guidelines for how we
develop this project.

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
the commits already on your branch, do the following, and then force-push your
branch:

```sh
MERGEBASE=$(git merge-base HEAD upstream/main)
git rebase --signoff $MERGEBASE
```

### Set up a local development environment

The [Quickstart (Development)](README.md#quickstart-development) in the README
covers bringing up a local cluster with the default (gVisor) runtime. To run
the microVM runtime locally — which needs `/dev/kvm`, or Lima nested
virtualization on Apple Silicon — see
[docs/dev/microvm-local.md](docs/dev/microvm-local.md). To measure where a
suspend or resume spends its time on such a cluster, follow
[docs/dev/suspend-resume-phase-breakdown.md](docs/dev/suspend-resume-phase-breakdown.md).

## Contribution process

This project is focused on iterating quickly but responsibly. This has
implications for contributors:

1) Things move quickly, so PRs may need to be rebased or updated frequently.
Small PRs that are focused on a single issue or feature are easier to review
and update than large PRs that touch many different parts of the codebase.

2) While we welcome new contributors, we do have priorities and many efforts in
flight. Small fixes and cleanups can go straight to a PR, but before you start anything
larger, please discuss it with us first. If there is an issue open, comment
there (and if not, open one). Before jumping into a task, please search open
PRs, so we are not duplicating work and wasting time. We want to be respectful
of everyone's time, and to make sure that your work is aligned with our goals
for the project.

3) PRs which are not aligned with our priorities and goals may be closed
without extensive review. We are not trying to be discouraging, but we need to
make sure that we are focused on the most important work.

4) When in doubt, communicate. We have GitHub, a mailing list, and Slack
channels, and we encourage you to use them. We also have regular community
meetings (via video calls), and you are welcome to attend those, too. If you
are unsure about something, please ask. It can save us all a lot of time and
effort.

If you are building something that runs *on* Substrate rather than changing
Substrate itself, see [Integration
Repositories](docs/project/integration-repos.md) for where that code should live.

### Optimizing PRs for review

We optimize PRs for easy review: too-large PRs get broken down, too-small PRs
get merged. Please keep that in mind as you work, especially with AI agents.

* **Large PRs**: Try to split huge changes into a series of smaller PRs, each a
  logically distinct step or feature. When the intermediate steps are not
  useful on their own, you can keep the change as one PR, but split into many
  commits at logical break points for review. PRs are squash-merged, so only
  the branch `HEAD` needs to build and pass tests (though it is usually best to
  update code and tests together).
* **Small and bulk PRs**: if you find a typo, review the whole file and
  fix everything in one pass rather than sending a single edit. Group
  related typo, doc, and single-line cleanup fixes into one PR rather
  than opening several small ones for the same topic. Maintainers may ask
  you to consolidate fragmented PRs into one, or close them in favor of a
  combined submission.

#### T-shirt sizes

As a rough scale: "S" is under 30 changed lines, "M" under 100, "L" under 500,
and "XL" under 1000. Most PRs should be "L" or smaller; "XL" and above are
candidates for breaking down, and generally signal the need for special
attention and discussion before review.

### Code Reviews

All submissions, including submissions by project maintainers, require review.
We use [GitHub pull
requests](https://docs.github.com/articles/about-pull-requests) for this
purpose.

We want to move quickly, but we need to balance that with quality and
maintainability. Code reviews are an important part of that process.

Agent Substrate requires at least one approving GitHub review from a maintainer
in order for the PR to be eligible for the merge queue. It is allowable to use
the "merge when ready" functionality to mark the PR as eligible for the merge
queue as soon as all preconditions are met.

Note that most changes to a PR will require a new review and approval. Use the
"re-request review" button to automatically place the PR back into your
reviewer's GitHub inbox.

As this community grows, we expect to see collaborators from different
companies. Whenever possible, we want to encourage code reviews across
companies. This helps foster collaboration and knowledge sharing across the
community, and it helps prevent silos from forming within the community.

Changes that add or alter behavior MUST come with tests, and existing tests
must be updated when behavior they cover changes. We will not merge code that
causes tests to fail. Every test runs in CI on every PR, so please don't add
tests whose only purpose is to raise coverage numbers. Tests should be
inherently valuable and protect against future mistakes.

### Release numbers

We generally follow [semantic versioning](https://semver.org/) for our
releases.

"Patch" releases (`1.0.x` -> `1.0.y`) are for bug fixes and security fixes. We do
not introduce new features in patch releases. It should always be safe to
upgrade a patch release without any risk of breaking your existing code.

"Minor" releases (`1.x.*` -> `1.y.*`) are for new features that are backwards
compatible. We do not remove features or introduce breaking changes in minor
releases. It should always be safe to upgrade to the next minor release without
any risk of breaking your existing code, though it may require some changes to
take advantage of new features.

"Major" releases (`x.*.*` -> `y.*.*`) are the only opportunity to make breaking
changes, but we still try to avoid them. It may or may not be safe to upgrade a
major release without some risk of breaking your existing code. We will try to
make major releases as infrequent as possible, and we will try to provide as
much notice and support as possible when we do make a major release.

#### Release branches

Every minor release has a release branch (named `release-x.y`), which is used
to prepare the release and to backport bug fixes and security fixes. A PR
destined for a release branch USUALLY starts as a PR against main, which is
then cherry-picked into the release branch. If a PR is only relevant to a
release branch, it may be opened directly against the release branch.

Example: Suppose we have a release branch `release-1.2` and a PR that fixes a
bug in that release. The PR author would first open a PR against `main`, which
would be reviewed and merged. Then, the PR author would cherry-pick the commit
into the `release-1.2` branch and open a PR against that branch. The PR would
be reviewed and merged into the release branch. This process ensures that the
fix is included in both the main branch and the release branch, and that the
fix is properly tested and reviewed. It also means that the final diff applied
to the release branch may be different from the diff applied to the main
branch, due to other changes that have been made in the meantime. The PR author
should be aware of this and be prepared to resolve any conflicts that may arise
when cherry-picking the commit into the release branch.

### Release notes

Part of moving fast is documenting what changed. Every PR should have a clear,
concise description of what changed and why. The PR description should be
written in a way that is understandable to someone who is not familiar with the
code. Each pull request title becomes its line in the [release
notes](docs/dev/release-notes.md).

### Compatibility and schema evolution

This project is committed to maintaining compatibility with previous releases.
This means that we WILL NOT accept changes that break compatibility without an
INCREDIBLY good reason, and in such a case project maintainers MUST be involved
in the discussion well before the PR is created.

Being "compatible" has many meanings. Below are some facets of "compatibility"
that we care about, but this list is not exhaustive. If you are unsure about
whether a change is compatible, please discuss it before spending too much time
on it.

* API compatibility: API operations that used to work still work and have the
  same effects. The APIs may be extended, but existing APIs MUST NOT be
  removed or changed in a way that breaks existing clients. This includes
  removing RPCs or fields, changing the type, name, or proto tag number of a
  field, changing an optional field to required, or changing the semantics of
  an existing API in a way that breaks existing clients.
  - NOTE: APIs covered by [preview gates](./docs/dev/preview-gates.md) are NOT
    subject to the same compatibility requirements as stable APIs, and may be
    changed or even removed.
* CLI compatibility: Command-lines are effectively an API, and all of the
  above rules apply to them as well. This includes the names of commands and
  sub-commands and the flags they accept.
  - NOTE: Some, but not all, CLI output is also important for compatibility.
    Some CLI commands are "machine-oriented" (e.g. `kubectl-ate get -o json`),
    and some are "human-oriented" (e.g. `kubectl-ate get -o table`).
    Machine-oriented output must remain compatible, but human-oriented output
    may change in ways that break attempts to parse it.
* Data compatibility: Data that was written by a previous version of the
  system can still be read by a new version. This includes on-disk data,
  as well as data stored in databases. Data formats may be extended, but
  existing data formats MUST NOT be changed in a way that breaks existing
  clients.
* Rollback compatibility: It must be possible to roll back to the previous
  version of the system. This means that we must design data formats and APIs
  in a way that allows for being extended, and that we must think about and
  test rollbacks.

More details are covered in the [version skew
policy](docs/reference/version-skew-policy.md) and [schema
evolution](docs/dev/postgresql-schema-evolution.md) documents.

Always follow the [PostgreSQL schema evolution
rules](docs/dev/postgresql-schema-evolution.md) for each application schema
change.

Below are some examples of things that are NOT considered part of our
compatibility promise:

* Log-lines: We do not guarantee that log-lines will remain the same across
  releases. We may change log-lines to improve clarity, add or remove
  information, or change the format of the log-line. We will try to avoid
  changing log-lines that are used by external tools, but we cannot guarantee
  that we will not change them.

### Root-gated tests

Tests that need root (overlay mounts, mknod, `trusted.*` xattrs, ...) call
`roottest.Require(t, ...)` from [internal/roottest](internal/roottest) as
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
[./hack/boilerplate](hack/boilerplate/).
