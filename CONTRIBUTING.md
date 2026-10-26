# Contributing

## Set up the development environment

```bash
uv python install 3.12
uv sync --locked --python 3.12
cp .env.example .env
```

Unit tests do not require secrets or access to a remote instance.

## Required checks

```bash
make check
git diff --check
```

A dependency change must also update `uv.lock`:

```bash
uv lock
uv sync --locked --python 3.12
```

## Conventions

- use Python 3.12+, type annotations, and a maximum line length of 100 characters;
- keep business logic testable and avoid hidden remote access;
- report operator errors with `PipelineError` and temporary failures with
  `TransientPipelineError` or the shared HTTP classifier;
- write structured results to stdout and human-readable progress to stderr;
- never write to DHIS2 without an explicit `--apply` option;
- never commit tokens, passwords, sensitive payloads, or the `state/` directory;
- never commit runtime YAML files; update only sanitized `.yaml.example` templates;
- write documentation, comments, logs, and user-facing messages in English.

## Tests

Add a regression test for every corrected incident. Cover separately:

- nominal behavior;
- DHIS2 conflicts;
- temporary network errors;
- checkpoint recovery;
- report and quarantine contents when they are part of the contract.

Integration tests must use a non-production DHIS2 instance and remain separate from the local test
suite.

## Documentation

Every new operator option must update at least:

- the CLI `--help` output;
- `docs/commands.md`;
- `docs/configuration.md` when adding an environment variable or YAML property;
- `docs/operations.md` when changing restart or diagnostic behavior.

## Pull requests

A pull request must explain the problem, the safety strategy, the checks performed, and any server
migration steps. Avoid unrelated refactoring in a focused change.
