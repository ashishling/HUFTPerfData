# Agent Workflow Rules

These instructions apply to all future repository changes.

## 1) Keep STATUS.MD Updated
- After any meaningful code, data-pipeline, MCP, dashboard, deployment, or infra change:
  - Review `STATUS.MD`
  - Update sections that are impacted (date, current state, architecture, deployment notes, open issues, or next actions)
  - Keep entries concrete and dated

## 2) Push Changes To Git Remote
- After implementing and validating changes:
  - Commit changes with a clear message
  - Push to the configured remote repository
- Default remote target:
  - `origin` -> `https://github.com/ashishling/HUFTPerfData.git`
- If push fails, resolve and retry until successful (or clearly report blocker).

## 3) Scope Control
- Do not include unrelated local metadata churn (for example `.DS_Store`) unless explicitly requested.
- Prefer committing only files that are part of the requested change.
