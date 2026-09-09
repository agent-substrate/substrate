# Governance

Feedback is welcome via PR review or on the `ate-dev@googlegroups.com` mailing
list.

Agent Substrate is an Apache-2.0 open-source project. This document describes
how decisions get made and how contributors can take on more responsibility
over time.

For day-to-day collaboration norms (PR workflow, communication, AI-tool
disclosure), see [COLLABORATING.md](COLLABORATING.md). For build and
contribution instructions, see [CONTRIBUTING.md](CONTRIBUTING.md). For
community conduct, see [code-of-conduct.md](code-of-conduct.md).

## Roles

We distinguish four tiers. Promotion to each tier requires sustained,
high-quality contributions; "enough" is intentionally judgment-based and
decided among current Maintainers.

**Default** — No formal designation. Anyone may open issues and PRs on the
project, or review other people's PRs and issues. Must follow the Code of
Conduct.  Opening a PR requires that the author sign the [Google Contributor
License Agreement](https://cla.developers.google.com/about) (see
[CONTRIBUTING.md](CONTRIBUTING.md)).

**Contributor** — Someone who has made at least one non-trivial contribution
and is known to or vouched for by a Maintainer, or has accumulated enough
contributions over time. Granted `Read` access on the repository. Must follow
the Code of Conduct and sign the [Google Contributor License
Agreement](https://cla.developers.google.com/about) before their first
contribution is merged (see [CONTRIBUTING.md](CONTRIBUTING.md)).

**Reviewer** — A contributor who has made enough contributions and is vouched
for by at least 2 Maintainers. Granted `Triage` access: can review, label, and
comment on PRs and issues, but cannot merge PRs without a Maintainer.

**Maintainer** — A reviewer who has made enough sustained contributions across
the project and is vouched for by at least 2 existing Maintainers. Granted
`Write` access: can approve and merge PRs, manage releases, and represent the
project externally.

A formal list of Maintainers and per-area Reviewers (e.g., via `CODEOWNERS` or
`OWNERS` files) is a separate discussion and will land as roles are formalized.

### Becoming a Contributor

Contributors are community members who have made some contributions to the
project and are known to other community members.  Being a Contributor means
your PRs can automatically run CI, and do not need to have a maintainer kick it
off.

To become a Contributor, you can either nominate yourself (via email to the
ate-dev mailing list) or be nominated by another Maintainer or Reviewer.

### Becoming a Reviewer

Reviewers are Contributors who have made more significant contributions to the
project and whose opinions are sought when reviewing other contributions.
Being a Reviewer means you can review and label PRs and issues, but cannot
merge PRs without a Maintainer.

To become a Reviewer, you can either nominate yourself (via email to the
ate-dev mailing list) or be nominated by another Maintainer or Reviewer.

### Becoming a Maintainer

Maintainers are Reviewers or Contributors who have made significant
contributions to the project, and are trusted to approve and merge other
people's PRs.

To become a Reviewer, you can either nominate yourself (via email to the
ate-dev mailing list) or be nominated by another Maintainer.

### Emeritus Maintainers

Depending on the reason for removal or resignation, a Maintainer may be
converted to Emeritus status. Emeritus Maintainers are recognized for their
past contributions and may still be consulted on project matters, but do not
have voting rights or merge access. Emeritus Maintainers are listed in
MAINTAINERS.md under a separate Emeritus section.

An Emeritus Maintainer may be reinstated to active Maintainer status by a
simple majority vote of existing Maintainers, provided they meet the current
Maintainer requirements and can commit to ongoing participation.

## Decision Making

- **Code changes.** Every PR needs at least one Maintainer approval and green
  CI before merge. Authors should never approve or merge their own PRs, unless
  something critical needs to be fixed urgently (e.g. a build break) and no
  other Maintainer is available.
- **Design changes.** File a GitHub issue or discussion describing the proposal
  and tag relevant Maintainers. Allow at least one week for feedback.
  Significant changes require Maintainer support; as the project grows, more
  authority will be delegated to per-subsystem owners.
- **Disputes.** Try to resolve on the PR or issue first. If that fails, any
  participant can ask the Maintainers to decide.
- **Code-of-Conduct issues.** Reported and handled per
  [code-of-conduct.md](code-of-conduct.md).

## Activity

Roles require ongoing participation. Reviewers and Maintainers inactive for six
months may have their status reviewed, with allowances for known absences
(e.g., sabbatical, parental leave).  Individuals who are inactive may be
designated "emeritus", which carries no formal authority but recognizes their
past contributions and allows them to return at a future date if they wish.

### Removing a Maintainer, Reviewer, or Contributor

Maintainers may resign at any time if they feel that they will not be able to
continue fulfilling their project duties.

Maintainers may also be removed after being inactive, failure to fulfill their
Maintainer responsibilities, violating the Code of Conduct, or other reasons.
Inactivity is defined as a period of very low or no activity in the project for
6 months or more, with no definite schedule to return to full Maintainer
activity.

A Maintainer may be removed at any time by a 2/3 vote of the remaining
maintainers.

## Changing this document

Open a PR. Allow at least one week for discussion. Requires Maintainer approval
to merge.
