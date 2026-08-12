---
name: security-status-report
description: Generates a security status report based on docs/threats.json by spinning up sub-agents for each threat to compute a quality score.
---

# Task
Generate a security status report by evaluating threats listed in `docs/threats.json`.

# Workflow

1. Copy `.agents/skills/security-status-report/scripts/template_dispatch.py` to `.agents/scratch/security-status-report/template_dispatch.py`.
2. Complete the TODO in `.agents/scratch/security-status-report/template_dispatch.py`, meeting the following requirements:
  a. Iterate over each threat from `docs/threats.json` to produce a list of invocations that matches your tool for invoking sub-agents.
  b. Each invocation must address a single threat, use the below prompt, and instruct the sub-agent to output the correct schema.
3. Execute the script from the repository root, specifying `.agents/scratch/security-status-report/subagents.json` as the output file argument (`python3 .agents/scratch/security-status-report/template_dispatch.py .agents/scratch/security-status-report/subagents.json`).
4. Read `.agents/scratch/security-status-report/subagents.json` and copy its exact JSON array into your tool for invoking subagents. DO NOT manually craft or bypass the invocations. Run ALL generated sub-agents concurrently in a single tool call by default unless bound by model rate limits or harness concurrency limits (use batching intelligently if needed). You already updated the script in step 2 to output exactly what you need, so no modifications to the output should be necessary unless you made a mistake or hit limits.
5. Wait for all sub-agents to complete.
6. Run `python3 .agents/skills/security-status-report/scripts/compile_report.py docs/threats.json .agents/scratch/security-status-report .agents/scratch/security-status-report/final.json` to produce the final report.
7. Run `python3 .agents/skills/security-status-report/scripts/render_chart.py .agents/scratch/security-status-report/final.json .agents/scratch/security-status-report/chart.png` to render a bar chart of the scores.

