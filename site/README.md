# Agent Substrate website

The project website, built with [Hugo](https://gohugo.io/) and the
[Docsy](https://www.docsy.dev/) theme. It is published to GitHub Pages by
`.github/workflows/site.yaml`.

## Build locally

You need:

- [Hugo extended](https://gohugo.io/installation/), at least the version set in
  `module.hugoVersion.min` in `hugo.yaml`
- Go, which Hugo uses to fetch Docsy as a Hugo module
- Node.js and npm, which only production builds need (for PostCSS)

From the repository root:

```shell
hack/site.sh serve   # live-reloading dev server at http://localhost:1313/substrate/
hack/site.sh build   # production build into site/public/
```

## How docs get onto the site

The site does not check in any Markdown content under `site/content/`. Instead:

- User-facing documentation lives in section subdirectories under `docs/` (`docs/get-started/`, `docs/concepts/`, `docs/guides/`, `docs/reference/`, `docs/security/`, `docs/project/`), and example tutorials live alongside their code in `demos/*/README.md`.
- Contributor-only documentation lives in `docs/dev/` (which `hugo.yaml` and `tools/site-content` exclude) or at the root of `docs/` with `docsite: false` in its YAML front matter.
- Before Hugo runs, `hack/site.sh` executes `go run ./tools/site-content --out site/content`, which generates the Hugo section `_index.md` files and a `{{% include-file %}}` stub page for each included Markdown file.

The `include-file` shortcode (`layouts/_shortcodes/include-file.html`) drops
the file's leading `# Heading` (the page title replaces it) and rewrites
relative links:

- a link to a file that some page names in `repo_source` goes to that page;
- a link under a directory in `params.repo_static_dirs` goes to the copy
  mounted into `static/`;
- any other relative link goes to the file on GitHub.

`docs/reference/api.md` is not included this way. Its front matter sets
`layout: api-reference`, so `layouts/docs/api-reference.html` renders the page
from `assets/api-reference/`, and the sidebar lists its resources from
`nav.json` there. `hack/site.sh` fills that directory with
`apitool generate-docs` (see `tools/apitool/README.md`), which generates the
reference from `ateapi.proto` for main and for each release since v1.0.0.
`api.md` is the page for main. The content adapter
`adapters/docs/reference/api/_content.gotmpl` adds a hidden page for each
release, which the page's version menu links to. The `.apiref` variables in
`assets/scss/_styles_project.scss` set its colors.

To publish another doc, place it in the appropriate `docs/<section>/`
directory (or `demos/<name>/README.md`). Optional YAML front matter (`title`,
`linkTitle`, `weight`, `description`, `aliases`, `card_icon`, `estimated_time`,
`status`, `status_note`) is read directly from the Markdown file; if omitted,
`tools/site-content` infers `title` from the leading `# Heading` and
`description` from the first paragraph. To exclude a file in `docs/` from the
website, add `docsite: false` to its YAML front matter.

## Doc status and new docs

A doc can declare its status in its own front matter:

```yaml
---
status: deprecated          # or: experimental, planned
status_note: >              # optional; replaces the default banner text
  Replaced by the [Egress guide](egress.md).
---
```

The page shows a banner under the title, and the sidebar and section cards
show a pill. Relative links in `status_note` are rewritten like links in the
doc. Any other `status` value fails the build. Front matter on the stub page
overrides the doc's.

The sidebar marks a page "New" when its `repo_source` was added since the
latest full release tag (`vX.Y.Z`). `hack/site.sh release-data` (run by
`hack/site.sh serve` and `hack/site.sh build`) works this out from git and
writes `data/release.json`.

`hack/site.sh test` builds the site against `testdata/` and checks the rendered
templates.

## Machine-readable documentation (`llms.txt`)

Hugo builds three plain-text outputs alongside the HTML pages:

- `/llms.txt` (`layouts/index.llms.txt`), a concise project overview and index
  linking to the Markdown build of each doc page and the repository's proto,
  CRD, and metric schemas;
- `/llms-full.txt` (`layouts/index.llmsfull.txt`), a single-file compilation of
  every published doc page;
- `/docs/<section>/<page>/index.md` (`layouts/docs/single.md`), the standalone
  Markdown version of each page, with relative links rewritten to absolute URLs.

All three share `layouts/_partials/raw-markdown.html` with the per-page
"Copy as Markdown" button.

## Glossary hover cards

On doc pages, the first mention of each glossary term gets a hover card with
its definition and a link to the term on the glossary page. Terms and
definitions come only from `docs/glossary.md`: each term is a top-level
bullet like

```markdown
- **Term** (optional note): definition, continued on lines
  indented two spaces.
```

The build fails if the doc has no terms in that shape, repeats a term, or a
term's bold label can't be found on the rendered glossary page. A mention
matches the term's exact casing first, then any casing, plus an optional
plural "s". `assets/js/glossary.js` holds the matching and has no terms of
its own.

## Theme

`assets/scss/_variables_project.scss` sets the palette, taken from the logo.
`assets/scss/_styles_project.scss` holds the dark theme, which follows the
[Agent Sandbox](https://agent-sandbox.sigs.k8s.io/) site. Templates under
`layouts/` override Docsy's.

The logo is `assets/icons/logo.png`, a copy of `logo/ate-logo.png`. The navbar,
the home page cover, and the favicons are all resized from it at build time.
