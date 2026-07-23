# How to Contribute

We would love to accept your patches and contributions to this project. Before
you spend a lot of time on a contribution, please review the following
guidelines.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google/conduct/).

## Contribution process

This is a very new project, so we are still working out exactly how it is going
to be developed. For now, we are focused on iterating quickly to find the right
design and architecture. This has implications for contributors:

1) Things are moving quickly, so PRs may need to be rebased or updated
frequently. Small PRs that are focused on a single issue or feature are easier
to review and update than large PRs that touch many different parts of the
codebase.

2) While we welcome new contributors, we are really focused on the minimal
capabilities needed to make this project useful. Before you start a new
contribution, please discuss it with us first (if there is an issue open,
comment there and if not, open one). We want to make sure that your work is
aligned with our near-term goals for the project and that we are not
duplicating work that is already in flight.

3) PRs which are not aligned with our near-term goals may be closed without
extensive review. We are not trying to be discouraging, but we need to make
sure that we are focused on the most important work.

### Sizing PRs for review

We optimize PRs for easy review — large PRs get broken
down, small PRs get merged.

* **Large PRs**: split huge changes into a series of smaller PRs, each a
  logically distinct feature. When the intermediate steps are not useful
  on their own, keep the change as one PR split into commits at logical
  break points, and preserve those commits on merge.
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

All code changes should be accompanied by tests. We will not merge code that
does not have tests, and we will not merge code that causes tests to fail.

### Copyright Headers

Every file containing source code must include copyright and license
information. This includes any JS/CSS files that you might be serving out to
browsers. (This is to help well-intentioned people avoid accidental copying
that doesn't comply with the license.)

Our standard headers for various filetypes can be found in
[./hack/boilerplate](./hack/boilerplate/).
