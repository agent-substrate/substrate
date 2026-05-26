# Governance

> **Status: Draft — under review and discussion.** This document is a proposal and has not yet been ratified by the Substrate maintainers. Feedback welcome via PR review or on the `ate-dev@googlegroups.com` mailing list.

Substrate is an Apache-2.0 open-source project. This document describes how decisions get made and how contributors can take on more responsibility over time.

For day-to-day collaboration norms (PR workflow, communication, AI-tool disclosure), see [COLLABORATING.md](COLLABORATING.md). For build and contribution instructions, see [CONTRIBUTING.md](CONTRIBUTING.md). For community conduct, see [code-of-conduct.md](code-of-conduct.md).

## Roles

We distinguish four tiers. Promotion to each tier requires sustained, high-quality contributions; "enough" is intentionally judgment-based and decided among current Maintainers.

**Default** — not yet a collaborator. Anyone may open issues and PRs.

**Contributor** — has made at least one non-trivial contribution and is known to or vouched for by a Maintainer, or has accumulated enough contributions over time. Granted `Read` access on the repository. Must follow the Code of Conduct and sign the [Google Contributor License Agreement](https://cla.developers.google.com/about) before their first contribution is merged (see [CONTRIBUTING.md](CONTRIBUTING.md)).

**Reviewer** — Contributor who has made enough contributions in a specific area and is vouched for by at least 2 Maintainers. Granted `Triage` access: can review, label, and comment on PRs and issues, but cannot merge.

**Maintainer** — Reviewer who has made enough sustained contributions across the project and is vouched for by at least 2 existing Maintainers. Granted `Write` access: can approve and merge PRs, manage releases, and represent the project externally.

A formal list of Maintainers and per-area Reviewers (e.g., via `CODEOWNERS` or `OWNERS` files) is a separate discussion and will land as roles are formalized.

## Decisions

- **Code changes.** Every PR needs at least one Maintainer approval and green CI before merge. Authors do not merge their own PRs.
- **Design changes.** File a GitHub issue or discussion describing the proposal and tag relevant Maintainers. Allow at least one week for feedback. Significant changes require Maintainer support; as the project grows, more authority will be delegated to per-subsystem owners.
- **Disputes.** Try to resolve on the PR or issue first. If that fails, any participant can ask the Maintainers to decide.
- **Code-of-Conduct issues.** Reported and handled per [code-of-conduct.md](code-of-conduct.md).

## Activity

Roles require ongoing participation. Reviewers and Maintainers inactive for six months may have their status reviewed, with allowances for known absences (e.g., sabbatical, parental leave).

## Changing this document

Open a PR. Allow at least one week for discussion. Requires Maintainer approval to merge.
