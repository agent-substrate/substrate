---
name: review-crds
description: Reviews CRDs for compliance with Kubernetes API conventions.
---

# Kubernetes API Conventions Skill

## Task

Review any CRDs in Agent Substrate to ensure they follow the conventions in `references/api-conventions.md`.

## Output Format

Your output should be a list of violations conforming to the following JSON format:

```json
[
  {
    "location": "",
    "rule": "",
    "fix": ""
  }
]
```

- location: file path and line range that violated the rule, formatted like this: `path/to/file/under/review:1-10`. The path is relative to the repo root.
- rule: Exact quote of violated rule from `references/api-conventions.md`. If there is no rule, this should be set to the string `OPINION`.
- fix: Your suggestion of how to fix the issue.

## Hints

- All CRDs are defined in `pkg/api/v1alpha1`.

## References
- [Kubernetes API Conventions](references/api-conventions.md)
