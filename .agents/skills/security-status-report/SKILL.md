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
3. Execute the script using `run_command` from the repository root, specifying `.agents/scratch/security-status-report/subagents.json` as the output file argument (`python3 .agents/scratch/security-status-report/template_dispatch.py .agents/scratch/security-status-report/subagents.json`).
4. Read `.agents/scratch/security-status-report/subagents.json` and copy its exact JSON array into your tool for invoking subagents. DO NOT manually craft or bypass the invocations. Run ALL generated sub-agents concurrently in a single tool call by default unless bound by model rate limts or harness consurrency limits (use batching intelligently if needed). You already updated the script in step 2 to output exactly what you need, so no modifications to the output should be necessary unless you made a mistake or hit limits.
5. Wait for all sub-agents to complete.
6. Run `python3 .agents/skills/security-status-report/scripts/compile_report.py docs/threats.json .agents/scratch/security-status-report .agents/scratch/security-status-report/final.json` to produce the final report.
7. Run `python3 .agents/skills/security-status-report/scripts/render_chart.py .agents/scratch/security-status-report/final.json .agents/scratch/security-status-report/chart.png` to render a bar chart of the scores.

## Prompt template for each sub-agent that should be used in the script

You are a security reviewer evaluating the following specific threat:

{INSERT EACH THREAT JSON VERBATIM HERE}

- Focus on this threat only.
- Review the entire repo.
- Produce a gut-feel "quality score" based on the current security posture of the repo with respect to that threat.
- Output your results using the following schema, by writing them to `.agents/scratch/security-status-report/{threat_id}.json`,
  where `{threat_id}` matches the id in the threat json you were initially provided.

```json
{
  "threat_id": "<threat_id from input>",
  "threat": "<threat text from input>",
  "quality": <Decimal between 0 (no effective mitigation) and 1 (perfectly mitigated).>,
  "strengths": "<Specific positive code/design mechanisms responsible for the score.>",
  "weaknesses": "<Specific negative code/design mechanisms responsible for the score.>",
  "citations": ["<repo-relative/path/to/file1.go>"]
}
```
