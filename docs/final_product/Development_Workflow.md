# Development Workflow

This document is the working agreement for how new engineering tasks should be handled from this point onward.

The goal is consistency:

- branch the same way every time
- write commit messages in a predictable format
- push changes through the same CI gate
- handle failures and correction cycles without improvising

Local Codex note:

- a matching local Codex skill named `moto-dev-workflow` is installed on this machine
- the skill is the agent-facing version of this document
- this markdown file remains the human-facing source of truth inside the repo

## Purpose

Now that `main` is the default branch and GitHub Actions is active, we should treat `main` as the stable integration branch.

That means:

- do not start new work directly on `main`
- do not keep long-running experimental work only in your local checkout
- let CI validate changes before they are merged back

## Branching Rule

Every new task starts from the latest `main`.

Recommended sequence:

```powershell
git switch main
git pull origin main
git switch -c feature/<short-task-name>
```

Examples:

```powershell
git switch -c feature/access-control
git switch -c feature/admin-mode
git switch -c feature/dev-prod-launchers
git switch -c fix/output-browser
git switch -c fix/pdf-layout
```

## Branch Naming Convention

Use one of these prefixes:

- `feature/`
- `fix/`
- `docs/`
- `chore/`

Examples:

- `feature/access-control`
- `feature/admin-mode`
- `fix/duplicate-snapshot-warning`
- `docs/operator-sop`
- `chore/packaging-cleanup`

## Commit Message Convention

Use short imperative commit messages in this format:

```text
<type>: <what changed>
```

Recommended commit types:

- `feature`
- `fix`
- `docs`
- `test`
- `chore`
- `refactor`
- `ci`

Examples:

- `feature: add read-only lock mode for secondary users`
- `fix: prevent rerun from using the wrong staged snapshot`
- `docs: add operator workflow recap`
- `test: add snapshot fixtures for ci`
- `ci: enforce ruff and pytest on push`
- `refactor: split ui staging and run selection`

Rules:

- keep the first word lowercase
- describe the change, not the ticket number
- use present tense / imperative wording
- avoid vague messages like `updates`, `stuff`, or `wip`

## Local Development Cycle

For each task, follow this loop:

1. create a branch from updated `main`
2. make the code or documentation changes
3. run the local quality checks
4. review `git diff`
5. commit with a clear message
6. push the branch
7. let GitHub Actions run
8. fix any CI failures on the same branch
9. open or update the pull request

## Local Quality Checks

Before pushing, run:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest
```

If a change intentionally alters a stored snapshot baseline, update snapshots locally first:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_snapshot_pipeline.py --update-snapshots
```

Then run the full suite again:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

## Staging and Committing

Review the work before committing:

```powershell
git status
git diff
```

Then stage and commit:

```powershell
git add <files>
git commit -m "feature: add access-control lock service"
```

If the task is broad, it is fine to use:

```powershell
git add .
```

but only after reviewing `git status` and making sure no unwanted runtime artifacts are included.

## Pushing a New Branch

For a new branch:

```powershell
git push -u origin feature/<short-task-name>
```

After that, later pushes can use:

```powershell
git push
```

## Pull Request Rule

Once CI passes on the branch:

1. open a pull request into `main`
2. review the changed files and CI result
3. merge only when required checks are green

As a working rule, new feature work should not be committed directly to `main`.

## Failure and Correction Cycle

If CI fails:

1. open the failing GitHub Actions run
2. read the first failing step carefully
3. identify whether it is:
   - lint failure
   - test failure
   - snapshot drift
   - environment/dependency issue
4. fix the problem locally
5. rerun local checks
6. commit the correction on the same branch
7. push again

Recommended correction pattern:

```powershell
git add <files>
git commit -m "fix: align snapshot test with committed reference fixtures"
git push
```

Do not create a new branch just to fix the CI for an already-open task branch.

## How To Handle Snapshot Failures

If snapshot tests fail, first decide whether the change is:

- unintentional regression
- intentional output change

If it is unintentional:

- fix the code
- rerun tests

If it is intentional:

1. update the snapshots locally
2. review the changed snapshot files carefully
3. commit both the code and the snapshot updates

Command:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_snapshot_pipeline.py --update-snapshots
```

Then:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

## How To Start The Next Task

When a new task begins:

```powershell
git switch main
git pull origin main
git switch -c feature/<new-task-name>
```

This should be the default opening move from now on.

## Rules For `main`

`main` should be treated as:

- stable
- protected
- merge-only

That means:

- no casual feature development on `main`
- no bypassing CI intentionally
- no merging when required checks are red

## Summary

The operating rhythm from now on is:

1. branch from `main`
2. build on the branch
3. run `ruff` and `pytest` locally
4. update snapshots only when intentional
5. commit with a clear typed message
6. push the branch
7. fix CI failures on that branch until green
8. merge back into `main`
