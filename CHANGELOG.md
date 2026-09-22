# Changelog

All notable changes to this project.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/).

---

## [Unreleased]

### Added

- `LICENSE` (Apache-2.0).
- `CONTRIBUTING.md`, including the single-source **ownership contract** for
  documentation.
- `docs/README.md` — documentation index with role-based reading paths and a
  document-status table.
- `docs/reference/glossary.md` — now defines terms that were previously used
  throughout the guide but never defined (`TRIAGE`, gate, model, declaration,
  `.sqlx`, estate, domain wave, cutover).
- `docs/reference/configuration.md` — every environment variable and which
  module reads it.
- `docs/testing/README.md` — how to run the suite, and a gate-by-gate
  "proves / does not prove" table.
- `docs/cutover-checklist.md` — per-wave operational checklist, consolidating
  deployment knowledge that was previously only in the unlinked tutorial.
- Navigation footers (`← Prev · Index · Next →`) and `Last reviewed` dates
  across all documentation.

### Changed

- **Documentation restructured.** Chapters renumbered into a gapless sequence;
  reference material moved to `docs/reference/`; the tutorial and the test
  report — previously ~43% of the documentation and linked from nowhere — moved
  to `docs/tutorials/` and `docs/testing/reports/` and wired into the index.
- Root `README.md` cut to a landing page; the step-by-step narrative it
  duplicated now lives only in the playbook.
- `.gitignore` hardened: added `.df-credentials.json` and other credential
  patterns; stopped ignoring `uv.lock` so the lockfile can be committed.

### Fixed

- **Cron semantics.** The guide claimed the sample workflow ran at 02:00. The
  DataWorks source cron `00 02 00 * * ?` is Quartz `s m h`, so it means
  **00:02**, and the generated Airflow expression `02 00 * * *` is also 00:02.
  Corrected everywhere, with the rules documented once in
  `docs/reference/variables-and-macros.md`.
- **TRIAGE behaviour.** Some pages claimed flagged items *stop* the pipeline.
  They do not — they are written to `review/*.md` and the run continues.
- **Verify gate.** Documented as row-count-only; it also performs MD5
  checksums, per-column numeric aggregates and DAG parity checks.
- **Rule-based vs AI.** The guide presented generation as Gemini-driven. The
  shipped generator is rule-based and deterministic; Gemini is an optional
  assist for flagged items only.
- **Airflow 2/3 output.** The dual `version_2/dags/` and `version_3/dags/`
  output was only documented in an unlinked file; now in the tools chapter,
  configuration reference and `code/README.md`.
- **Working directory.** Command snippets disagreed on where to run from; all
  now run from `code/` and use `uv run <script>`.
- Duplicate content removed: the CSV override appendix (a second full copy of
  the reference page), a second routing matrix, a duplicated type map, and
  three divergent repository file maps.
- Broken prose cross-references to a non-existent "appendix A of docs/06" and
  to renamed/deleted files.
- Broken `sed` command in the tutorial (an unescaped `/` terminated the
  expression).

### Security

- Redacted a personal email address from the test report.
- Replaced hardcoded personal filesystem paths (`/home/...`) with `$REPO_ROOT`
  and scratch paths with `$WORK_DIR`.
- Replaced the demo GCP project name with `$GCP_PROJECT_ID` throughout.
- Replaced the working MySQL password in `.env.example` with `change-me`.
- Added `.df-credentials.json` to `.gitignore` — the generator writes it.

---

## Background

This guide began as an internal working draft ("DataWorks → GCP Pipeline
Migration Guide", v3.1 and v3.2). Those drafts are not public; references to
them have been removed from the documentation. The recorded end-to-end test
runs from 2026-08-15 through 2026-09-11 are preserved in
[`docs/testing/reports/`](docs/testing/reports/).
