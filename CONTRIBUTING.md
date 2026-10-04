# Contributing

How the team works on this repository. Questions or changes to this page go through a PR like any other change.

## Workflow

1. Pick up an issue, or create one. Every change starts from an issue.
2. Create a branch from `main`.
3. Make the change, commit, and push the branch.
4. Open a pull request into `main`.
5. Get one approval, then merge.

## Branches

All work goes through a branch and a PR into `main`. Do not commit to `main` directly.

Name branches `type/ISSUE_NUMBER-short-description`, in lowercase with hyphens, for example `docs/27-agree-on-naming-standards`.

| Prefix | Use for |
| --- | --- |
| `feature/` | New pipeline work or functionality |
| `bugfix/` | Fixing something that is broken |
| `hotfix/` | Urgent fix |
| `docs/` | Documentation only |
| `chore/` | Maintenance, such as config or cleanup |

The prefix is required and CI rejects other names. The issue number is recommended, not required.

## Commit messages

Use a type and a short summary, for example `docs: add naming standard`. The types match the branch prefixes. This is recommended, not required. Keep the summary short and say what changed.

## Pull requests

- Title: type and summary, in the same style as commit messages.
- Description: fill in the PR template and link the issue with `Closes #NUMBER`. Use `Part of #NUMBER` if the issue needs more than one PR. CI fails without an issue link.
- Keep each PR to one issue.
- Every PR changes at least one `.md` file (CI checks this), so update the docs that your change affects.
- Do not include secrets, credentials or personal data.

## Review and merging

- A reviewer is assigned automatically when the PR is opened. You can add more reviewers by hand as a backup.
- One approval is enough to merge.
- The author merges by default. If the author cannot get to a computer soon, they can ask the reviewer to merge.
- Merge commit and squash merge are both fine.

## Decisions

If your change involves a choice that someone might question later, record it in `docs/decisions/`. See the README there for when to write one and how.

## Ownership

Who owns which area is listed in [docs/governance/ownership.md](docs/governance/ownership.md).