> The PR title becomes the release note. Write it for users: what changed for them, not how the code changed.

#### Breaking change

<!--

If users must act when they upgrade, describe what they must do, and ask for the `breaking-change` label.
Otherwise delete this section.

What is a breaking change? Something that requires customer code/configuration changes to make their agents work.
But more broadly it might include any behavior change that customers were relying on (Hyrum’s law). Examples of
breaking changes:

- Moving from 1:1 pods to actors to multiple actors in one pod.
- Changing actor primary keys or identity formats.
- Adding authorization or authentication where none was required before.

-->

Fixes #<issue_number_goes_here>

> It's a good idea to open an issue first for discussion.

- [ ] Tests pass
- [ ] Appropriate changes to documentation are included in the PR
