# Add PR Markdown Validation

## Summary

Added a GitHub Actions CI check that requires each pull request to
include a Markdown (`.md`) file named exactly after the pull request
title.

## Changes Made

-   Added a `Validate PR Markdown file` step to the `repository-checks`
    job.
-   Passed the pull request title, base commit SHA, and head commit SHA
    to the validation script through environment variables.
-   Used Git to identify Markdown files added or modified in the pull
    request.
-   Configured the check to fail when the required Markdown file is
    missing.
-   Limited this validation step to pull request events; it does not run
    on regular pushes.

## How It Works

1.  GitHub Actions checks out the repository.
2.  The validation step reads the pull request title and commit SHAs.
3.  It builds the expected filename by appending `.md` to the pull
    request title.
4.  It compares that filename against the Markdown files added or
    modified in the pull request.
5.  The check passes if a matching file is included, otherwise it fails
    and prints an error.

## Example

For a pull request titled `Add Project Noah Ingest`, the required file
is:

`Add Project Noah Ingest.md`

The filename must match the pull request title exactly, including
capitalization and spaces.

## Purpose

This check encourages consistent documentation for pull requests and
helps ensure that changes are accompanied by a corresponding Markdown
file.
