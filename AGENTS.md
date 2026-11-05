# AGENTS.md

Guidance for coding agents working in `arbiter-proto` — the protobuf / gRPC contract of the Sentio Network Arbiter (and its payload store). Build and code generation are described in `README.md`.

## Design docs

Design specs, implementation plans and reports for this project are maintained in the maintainers' internal design-docs repository, not in this repo. Maintainers: the location is in your global agent instructions and in `CLAUDE.local.md` when present.

- Do not add specs or plans under `docs/` (no `docs/superpowers/`, `docs/specs`, `docs/plans`). When a workflow skill (brainstorming, writing-plans, …) would save a spec or plan into this repo, save it to the internal design-docs location instead.
- Cite a design in proto comments, docs and commit messages by its name or date (`internal design doc 2026-09-23-dynamic-si-table-set-design`), never by a file path.
- What stays here: README and wire-compatibility records that the code cites (`docs/compatibility/`).
