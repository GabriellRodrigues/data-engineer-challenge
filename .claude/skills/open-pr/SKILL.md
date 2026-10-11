---
name: open-pr
description: Open or update the challenge pull request from the current branch of the candidate's fork to the original repository (branch main), describing only what the branch actually did, after a review-changes pass. Use when the user asks to open/create a PR, push and PR, or ship a branch in the data-engineer-challenge repo.
metadata:
  source: https://github.com/pascalorg/editor/blob/main/.agents/skills/open-pr/SKILL.md
  license: MIT (see LICENSE in this folder); adapted for this repository
allowed-tools: Bash(git *) Bash(gh *) Bash(make *) Bash(uv *) Read Glob Grep Skill
---

Open (or refresh) the pull request from the candidate's fork to the original challenge repository,
branch `main`, as the challenge README ("Entrega") requires.

## 0. Remotes

```bash
git remote -v
```

- `upstream` must point to the original challenge repository and `origin` to the candidate's fork.
- If `origin` is the original repository (no fork configured), stop and ask the user for the fork
  URL. Never push to the original repository.
- `gh` must be installed and authenticated (`gh auth status`). If it is not, do not install it:
  push the branch and give the user the compare URL
  (`https://github.com/<upstream-owner>/<repo>/compare/main...<fork-owner>:<branch>`) with the
  title and body to paste.

## 1. What is actually on the branch

```bash
git branch --show-current                      # not main
git status --short                             # nothing uncommitted you weren't asked to commit
git fetch upstream main
base=$(git merge-base HEAD upstream/main)
git log --oneline --first-parent --no-merges "$base"..HEAD
git diff --stat "$base"..HEAD
```

The PR describes **only** these commits and this diff. Work that arrived by merging `main`,
reverted commits and patch-equivalent cherry-picks are not this PR's claims. If a file in the diff
has no commit on the branch explaining it, stop and ask.

Never commit generated data or local tooling: `data/`, `.venv/`, `.idea/`,
`.claude/settings.local.json`. If any of them appears in the diff, stop and ask.

Checks, when the change touches code (run them, never claim a check you did not run):

```bash
uv run --extra dev ruff check src etl_jobs tests run_duckdb_ui.py
make test                                      # or: uv run --extra dev pytest
```

For pipeline changes, also run `make bronze` (and `make report`) and note the outcome.

### 1.1 Review before opening

Always run the `review-changes` skill on the branch before pushing, and show its output to the
user.

- **Blocking items** under "Needs change": stop. Open the PR only after they are fixed, or after
  the user explicitly accepts them; accepted items go under **Pending** in the body.
- **Nothing blocking:** continue. Reuse its objective in "What does this PR do?" and its checks in
  "Checks run" instead of running them twice.

## 2. Body

There is no `.github/pull_request_template.md`; use these headings, filled from §1, not from memory:

- **What does this PR do?** The result, one paragraph or a short list, grouped by layer/component
  (Bronze, data contracts, observability, maintenance, docs).
- **How to test.** Numbered, concrete reviewer steps with the expected outcome (e.g.
  `make up`, `make generate`, `make bronze`, `make report`, `make test`). Automated commands only
  if you ran them.
- **Checks run.** The exact commands from §1 and their result (e.g. `make test: 114 passed`).
- **Documentation.** Links to the files under `docs/pipeline_docs/` changed by the branch.

The final PR must also have the README sections the challenge asks for ("Como rodar", "Premissas e
decisões", "Visão geral da arquitetura", "Extensibilidade", "Limites do desenho", "O que faria
diferente em produção", "Ferramentas de IA utilizadas"). If they are missing or out of date with the
branch, list that under a **Pending** heading instead of claiming them.

Title: under ~70 characters, a concrete result (not "update"/"improve"), scope-prefixed when one
area owns it (`bronze:`, `silver:`, `gold:`, `contracts:`, `observability:`, `maintenance:`,
`docs:`).

## 3. Push and open, or update

```bash
git push -u origin HEAD
gh pr view --repo <upstream-owner>/<repo> --json number,url,body 2>/dev/null     # existing PR?
```

- None → `gh pr create --repo <upstream-owner>/<repo> --base main --head <fork-owner>:<branch>
  --title … --body "$(cat <<'EOF' … EOF)"`.
- Exists → `gh pr edit <n> --repo <upstream-owner>/<repo> --body …`: rebuild "What" and "How to
  test" from the current branch, keep the reviewer's checklist ticks and any extra sections
  verbatim. Change the title only if the scope changed.

End the body with the attribution lines the session asks for, if any.

## 4. Report

PR URL (or compare URL when `gh` is unavailable), title, which checks you ran, and anything left
unchecked or pending for the reviewer.
