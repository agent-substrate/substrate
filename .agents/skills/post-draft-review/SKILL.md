---
name: post-draft-review
description: Posts pull request review findings as GitHub draft (pending) inline comments for a human to edit and submit, instead of publishing them straight to the PR author. Use whenever asked to review a pull request or leave review comments on one, including phrasings like "review PR 764" or "take a look at this PR".
---

# Post Draft Review Comments

A "draft review" on GitHub is a review left in the **`PENDING`** state. It is visible
only to its author, accumulating comments, until the author explicitly **submits**
(publishes) or **discards** it.

Findings go in as pending inline comments rather than individually published comments, so
a human reviews them before anything reaches the PR author.

`gh pr review` can **not** create one — it always submits immediately
(`--approve` / `--comment` / `--request-changes`). To stay in draft, hit the REST API:
`POST .../reviews` **with no `event` field** creates the review `PENDING` rather than
submitting it.

## Inline comments only — the human authors the summary

Contribute **inline comments only**, created with an **empty body** (`body: ""`). The
top-level summary is written by the human in the UI's "Finish your review" box when they
submit. Rationale:

- The high-level overview (praise, verdict, framing) is the human's voice, not the
  agent's.
- The UI's summary box does **not** prefill an API-set body, so an agent-authored body
  is silently dropped on UI submit anyway, and can't be restored afterwards: `PUT` on a
  submitted review with an empty body is rejected with 422. An empty body sidesteps the
  whole failure mode.
- Don't smuggle summary-level content into an inline comment. Top-level content anchored
  to a code line is poor UX for the reader.

Anything with no inline anchor — what was checked and found clean, issues in files
outside the diff — goes in the chat hand-off, for the human to draw on when they write
the summary.

## Disclose that an agent found it

Each inline finding leads with `:robot:` followed by the bolded severity name and color
(e.g. `🤖 **should-fix** 🟡 – …`) to mark it as an AI finding. Keep it generic: don't
name the agent, vendor, or model. Disclose only that *an* agent was involved. The human
who finishes the review is accountable for what gets submitted.

## Keep it tight

Bias toward brevity. A human reads this, then decides. Concretely:

- **Inline comments**: lead with the severity tag and state the problem in a sentence or
  two, then the fix. Drop preamble, restated context, and pasted command/test output —
  point at the line and trust the reader.
- **Short sentences.** One clause each where possible; no run-ons or long em-dash chains.
  State the claim and its consequence; leave the derivation (timelines, worked examples,
  step-by-step traces) out. The author can re-derive it from the claim, and can ask if
  they can't.
- **Severity tags**: blocking 🔴 · should-fix 🟡 · nit 🟢 · question 🟢. Every inline
  finding opens with the robot marker, then the **bolded** severity name, then the color:
  `🤖 **blocking** 🔴 – …`, `🤖 **should-fix** 🟡 – …`, `🤖 **nit** 🟢 – …`,
  `🤖 **question** 🟢 – …`. Leading with 🤖 makes agent comments uniformly recognizable.
  The severity *name* is required because the colors alone aren't a standard PR authors
  know, and a bare 🟢 beside a critique reads like approval.
- When in doubt, cut. A finding the reader can act on in five seconds beats a paragraph.
- **Brevity comes from cutting content, not compressing prose.** Humans read these: keep
  complete sentences. No telegraphic fragments, abbreviations ("incl.", "w/"), or bare
  "label: clause" constructions. Drop whole points that don't change what the reader does
  next; write the surviving ones plainly.

## Create the draft (PENDING) review

**First check whether a pending review already exists.** You post under the human's
account, and each author may have only one pending review per PR — so any existing draft
is theirs, and it may hold comments they wrote by hand:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews \
  --jq '.[] | select(.state == "PENDING") | {id, node_id}'
```

No output means there's no draft yet; create one as below. If it returns a review, skip
to [Adding to a draft that already exists](#adding-to-a-draft-that-already-exists) and
**do not delete it**.

Write each comment body to a file and assemble the payload with `jq --rawfile`, which
escapes markdown, backticks, and newlines correctly. The body is always empty:

```bash
commit_id=$(gh pr view <num> --repo <owner>/<repo> --json headRefOid --jq '.headRefOid')

jq -n --arg commit "$commit_id" --rawfile c1 c1.md --rawfile c2 c2.md '{
  body: "",
  commit_id: $commit,
  comments: [
    {path:"cmd/ateapi/internal/store/ateredis/ateredis.go", line:849, side:"RIGHT", body:$c1},
    {path:"cmd/ateapi/main.go", line:57, side:"RIGHT", body:$c2}
  ]
}' > /tmp/review.json

gh api repos/<owner>/<repo>/pulls/<num>/reviews \
  --method POST --input /tmp/review.json --jq '{id, state}'
# => {"id": ..., "state": "PENDING"}
```

`state: PENDING` confirms it's a draft: not visible to anyone else, no notifications,
until submitted. Keep the returned `id` to inspect or discard. Omitting `event` is what
keeps it in draft; including `event` submits immediately.

Inline-comment notes:

- `line` is the line number **in the file at the PR head**. `side: "RIGHT"` is the new
  (post-change) version, `"LEFT"` the base side.
- Multi-line: use `start_line` and `start_side` together with `line`/`side`.
- The line **must fall within the PR's diff hunk**, or the API rejects the whole review.
  A file that isn't in the diff at all (for example a caller that wasn't touched) can't
  take an inline comment. Anchor the point on a *changed* line nearby, such as the struct
  field that the untouched caller fails to populate, and reference the real location in
  prose. Otherwise leave it to the chat hand-off.
- One bad anchor rejects the **whole batch**, and the error doesn't say which one. If the
  POST fails, create the review with a single comment and append the rest one at a time
  (see below) — the offender is then the one call that fails.
- Pin exact head line numbers from a checkout of the PR head (`gh pr checkout <num>`, or
  fetch `pull/<num>/head`). Don't eyeball them from the diff.
- `commit_id` pins the review to the head you actually read. Without it GitHub anchors
  against whatever is current when the POST lands, so a push between your fetch and your
  post silently moves every comment to lines you never looked at. The GraphQL append below
  has no equivalent field, so re-check the head SHA before appending to an older draft.

### Adding to a draft that already exists

A pending review you didn't create is the human's, and it may already hold comments they
wrote themselves. **Never delete it.** Deleting takes their comments with it, they aren't
recoverable, and drafts are exactly where someone parks a half-finished thought.

Append instead, one mutation per finding, using the review's `node_id` (the `PRR_…`
value, not the numeric id):

```bash
gh api graphql -f query='
mutation($review: ID!, $path: String!, $line: Int!, $body: String!) {
  addPullRequestReviewThread(input: {
    pullRequestReviewId: $review,
    path: $path, line: $line, side: RIGHT, body: $body
  }) { thread { id } }
}' -f review="PRR_kwDO..." \
   -f path="cmd/ateom-microvm/restore.go" -F line=214 \
   -F body=@c1.md
```

`-F body=@c1.md` reads the body straight from the file, keeping the file-per-comment
discipline that `--rawfile` gives the batch path. Prefer it over `-f body="$(cat c1.md)"`:
command substitution strips trailing newlines, so the last line of the comment loses its
break. For a multi-line anchor, add `startLine` and `startSide`.

Leave the review body alone whether it's empty or not. If the human started the draft,
that text is theirs.

Appending this way is verified: the review keeps its id, stays pending, and the human's
own comments come through untouched. The single-comment delete below hasn't been
exercised — read the review back the first time you use it.

### Revising your own findings

Delete your own comments one at a time. Don't delete the review — recreating it is only
safe when you know the draft is entirely yours, and in a shared draft you don't:

```bash
# your findings are the 🤖-tagged ones
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<id>/comments \
  --jq '.[] | select(.body | startswith("🤖")) | .id'

gh api repos/<owner>/<repo>/pulls/comments/<comment_id> --method DELETE
```

This is the second job the 🤖 marker does. In a shared draft it's the only thing that
tells your comments apart from the human's.

A finding the human has already edited may no longer start with 🤖, so the filter skips
it. That's the safe direction — they've taken it over — but don't read a missing marker
as proof a comment was never yours.

### Verify anchors landed correctly

`line` and `original_line` come back `null` while a review is PENDING (they resolve on
submit), and `position` is a legacy cumulative diff offset. Neither tells you the file
line at a glance. Instead, check the **last line of each comment's `diff_hunk`**, which
is the line the comment is attached to:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<id>/comments \
  --jq '.[] | "\(.path)\n   ↳ \(.diff_hunk | split("\n") | last)\n"'
```

## Hand-off, then the human finishes

The hand-off in chat should give the human what they need to write the summary and
decide: a short findings table (severity, one-line claim, `file:line` anchor) and the
checked-and-found-clean notes, meaning the verification work with no inline anchor. No
paste-ready summary text — the summary is the human's to write.

Close the hand-off with the review id, the comment count, and these three points. A human
who doesn't know them can publish the whole thing unread:

- Nothing is published yet. The review is `PENDING` and visible only to them.
- They should read each finding against the code before submitting, and delete anything
  they don't stand behind. Deleting a pending comment costs nothing.
- Submitting publishes every remaining comment at once, under their name. There is no
  partial submit.

Point them at [README.md](README.md), which is the full version of the above and the
authoritative instructions for the human — keep it in sync if you change this section.
Don't imply the review is finished. It isn't, and it can't be until someone has read the
findings.

To discard the whole draft instead:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews/<review_id> --method DELETE
```

This removes every comment in the review, including any the human wrote. Only reach for
it on a draft you created and know is entirely yours.

## Gotchas

- Each author may have only **one** pending review per PR at a time, and you post as the
  human, so a second `POST .../reviews` errors out. Append to the existing draft rather
  than clearing it — see [Adding to a draft that already
  exists](#adding-to-a-draft-that-already-exists).
- Relative markdown links in comment bodies (for example `[x](cmd/.../foo.go#L1)`) render
  oddly on GitHub. Prefer plain `` `path:line` `` in backticks for code references.
