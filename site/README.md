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
make site-serve   # live-reloading dev server at http://localhost:1313/substrate/
make site-build   # production build into site/public/
```

## How docs get onto the site

The site does not keep copies of the docs. `hugo.yaml` mounts selected files
from the repository into the site's assets under `repo/`, and each page under
`content/docs/` is a short stub that renders one of them:

```markdown
---
title: "Architecture"
weight: 1
repo_source: "docs/architecture.md"
description: >
  One sentence shown on the section index.
---

{{% include-file %}}
```

The `include-file` shortcode (`layouts/_shortcodes/include-file.html`) drops
the file's leading `# Heading` (the page title replaces it) and rewrites
relative links:

- a link to a file that some page names in `repo_source` goes to that page;
- a link under a directory in `params.repo_static_dirs` goes to the copy
  mounted into `static/`;
- any other relative link goes to the file on GitHub.

To publish another doc:

1. Add the file to the `module.mounts` entry for its directory in `hugo.yaml`.
2. Add a stub page under `content/docs/` with `repo_source` set to its path
   from the repository root.

## Theme

`assets/scss/_variables_project.scss` sets the palette, taken from the logo.
`assets/scss/_styles_project.scss` holds the dark theme, which follows the
[Agent Sandbox](https://agent-sandbox.sigs.k8s.io/) site. Templates under
`layouts/` override Docsy's.

The logo is `assets/icons/logo.png`, a copy of `logo/ate-logo.png`. The navbar,
the home page cover, and the favicons are all resized from it at build time.
