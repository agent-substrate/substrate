---
name: agents-md
description: Generates or updates an AGENTS.md file
---

Generate or update AGENTS.md files in key folders of the project.
Ensure the target AGENTS.md file accurately reflects the current status of the code.

## Key Requirements

There must always be an AGENTS.md file in the project root.
The AGENTS.md file at the root of the project may include the following sections:

- Project overview
- Build and test commands
- Code style guidelines
- Testing instructions
- Security considerations

AGENTS.md files in subfolders may be even more concise and specific to those subfolders.

## Maximize AGENTS.md Performance

To maximize agent performance and token efficiency, adhere to the following:

### Progressive Disclosure

Each AGENTS.md should be short. Approx. 100-150 lines total.
Link to a handful of relevant, focused reference documents in the same repo, as needed.

### Modularity

While only one AGENTS.md is allowed per folder, you can create an AGENTS.md file in any folder.
Rather than putting everything in the root AGENTS.md, move highly specific advice to AGENTS.md in the most relevant folders of the project.

### References

When documentation that would have helped you is missing, add references in the docs/ folder found at the project root. When these get out of date, update them.

### Workflows

When references describe workflows, the tasks should be described as numbered steps. For example:

1. Get the beep from the boop.
2. Then bop it.

### Decisions

When there are multiple ways of doing something in the codebase, use a decision table like the following:

| Question | Boop | Bop |
| :---- | :---- | :---- |
| Is foo a bar baz? | ✅ |  |
| Is foo a quux? |  | ✅ |

### Real Code Examples

If you think they are particularly good examples, include snippets from actual code in the repo, up to approx. 10 lines in length.
These should only be the most relevant examples and representative of patterns that would be good to repeat in future code.
Their purpose is to improve code reuse by agents, rather than each agent reinventing the wheel.

### Domain Specific Rules

If you think it would help, you can include a few simple domain specific rules.
Not too many, or agents will get stuck trying to handle too many rules.
A general example of a domain specific rule is "Use a finance-specific numeric type for any financial calculations."

### Don't vs. Do

When adding warnings against doing something, also make a concrete suggestion for the correct approach.
"Don't do X, do Y instead."
