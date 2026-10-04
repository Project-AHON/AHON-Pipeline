# 0002: Team workflow

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

Five people work in one repository and need agreed rules for branches, reviews, commits and ownership. CI already checks branch prefixes, issue links in PRs and a changed `.md` file, and assigns a reviewer automatically.

## Decision

- PRs target `main` only.
- Branches use `type/ISSUE_NUMBER-short-description`. The prefix (`feature/`, `bugfix/`, `hotfix/`, `docs/`, `chore/`) is required; the issue number is recommended.
- Commit messages and PR titles use a type and summary, such as `docs: add naming standard`. This is recommended, not required.
- One approval is needed to merge. The auto-assigned reviewer is the default, and more reviewers can be added by hand as a backup.
- The author merges by default, and can ask the reviewer to merge if they cannot get to a computer. Merge commit and squash are both fine.
- Ownership of each area is listed in `docs/governance/ownership.md`, outside `CONTRIBUTING.md`.

## Why

- CI already enforces the prefixes, so the rules match what the repo does.
- One approval keeps a team of five moving, and the rotation spreads the review work.
- A separate ownership page can change as the work moves between phases without editing the contributing guide.

## Alternatives considered

- **A `dev` branch alongside `main`:** rejected. A second long-lived branch adds merge work for a project this size.
- **Two approvals:** rejected. Slower, with little gain for a small team.
- **Required issue number and commit format:** rejected. CI cannot check them yet, and they are recommended instead.

## Consequences

- `CONTRIBUTING.md` and `docs/governance/ownership.md` hold these rules.
- Ownership cells are empty until the team fills them in.

## Open

- Whether to protect `main` in GitHub settings so the one-approval rule is enforced and not only agreed.