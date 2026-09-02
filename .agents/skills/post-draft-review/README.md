# post-draft-review

How agents post pull request review findings in this repo — and what you have to do
before any of those findings become public.

If an agent just told you it "posted a review" on your PR, read this first. Nothing has
been published yet, and finishing the review is your job.

## What the agent did

It reviewed the PR and created a **draft (pending)** GitHub review: inline comments
anchored to specific lines, with an empty summary body.

A pending review is visible **only to you**. No one else can see it, the PR author is not
notified, and nothing is published until you submit it. That is the whole point — it
gives you a checkpoint between an agent's findings and the author's inbox.

**If you had already started your own draft on that PR, the agent added its findings to
it.** GitHub allows you only one pending review per PR, and the agent posts as you, so
there is no separate agent review to look at — your comments and its findings are now in
the same draft, and yours are untouched. The 🤖 prefix is what tells them apart.

## What you have to do

The pending review is yours, not the agent's. **Every comment publishes under your name.**
Each finding leads with 🤖 to disclose that an agent found it, but that marker does not
tell readers you skipped checking it — they will reasonably assume you did.

1. Open the PR's **Files changed** tab. Pending comments carry a **Pending** badge, and a
   **Finish your review** button sits at the top.
2. **Read each finding against the code it points at.** Agents produce confident,
   well-worded findings that are wrong often enough that this step is the entire reason
   the review is a draft.
3. **Edit or delete** anything wrong, redundant, or not worth the author's time. Deleting
   a pending comment costs nothing — it was never published.
4. **Write your own summary** in the "Finish your review" box.
5. Submit.

## Two things that catch people out

- **Submitting publishes every remaining pending comment at once.** There is no partial
  submit. Anything you did not delete goes out, so deleting is the only way to drop a
  finding you don't stand behind.
- **The "checked and found clean" notes in the chat hand-off are agent claims too.** They
  read as reassurance and are the easiest thing to paste into a summary unverified. An
  agent asserting it verified something is not the same as it having verified it.

## Why the agent doesn't write the summary

The overall verdict and framing are yours. Beyond that, GitHub's "Finish your review" box
does not prefill a body set through the API, so an agent-written summary would be silently
dropped when you submit — and it cannot be restored afterwards. Leaving the body empty
avoids the failure mode entirely.

## To throw the whole thing away

Find the draft's id, then delete it:

```bash
gh api repos/<owner>/<repo>/pulls/<num>/reviews \
  --jq '.[] | select(.state == "PENDING") | .id'

gh api repos/<owner>/<repo>/pulls/<num>/reviews/<review_id> --method DELETE
```

This deletes **every** comment in the draft, including any you wrote yourself. To drop
only the agent's findings and keep your own, delete them individually in the UI instead.

## For agents

See [SKILL.md](SKILL.md) for the posting mechanics, severity tags, and hand-off format.
