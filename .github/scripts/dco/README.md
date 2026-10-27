# DCO labels

`dco-check.yaml` updates `needs-dco` on open PRs, including drafts and forks,
hourly and when opened, reopened, pushed, or retargeted. Manual runs only report
changes unless `apply` is selected. The workflow creates the label if needed
and posts no comments.

The [NVKE workflow](https://github.com/dsx-ai-factory/nvke/blob/4613e924abc1f7a9c3cf7b9beef4e2e897094fac/.github/workflows/dco-check.yml)
provided the starting point. Substrate differs in two ways:

- Our `DCO` check comes from [CNCF DCO2](https://github.com/cncf/dco2), app
  `dco-2` (ID 974774). The scan reads checks from each PR's head because fork
  check runs can have no PR association.
- Older open PRs predate the app and have no verdict. For these, the scan reads
  all PR commits and checks for a sign-off matching the author or committer,
  ignoring case. Like DCO2, it exempts merge commits and GitHub bot authors.
  REST preserves GitHub's bot classification.
  This fallback assumes the current DCO2 defaults: members must sign off and
  remediation commits are disabled. Revisit it if `.github/dco.yml` is added.

A completed `action_required` adds the label; `success` removes it, including
an override. Other app results leave it unchanged. Only DCO2's runs count,
and an existing run always takes precedence over the fallback.

The fallback checks matching trailers, not DCO2's email validation. Without
an app verdict, incomplete lists and PRs over the REST API's 250-commit limit
are left unchanged. GraphQL batches keep check lookups within the API budget.

The scan rechecks the PR state, head, base, labels, and verdict before writing.
Concurrent changes after that read are corrected on the next run. Runs are
serialized; the hourly sweep catches replaced queued events. The write job
runs only upstream and executes code only from the default branch.

See [CONTRIBUTING.md](../../../CONTRIBUTING.md#sign-off-your-commits) for sign-off
instructions. Run the regression tests with:

```sh
node --test .github/scripts/dco/label.test.cjs
```
