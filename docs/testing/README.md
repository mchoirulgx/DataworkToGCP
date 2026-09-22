# Testing

> **Last reviewed:** 2026-09-22

How to run the test suite, what each pipeline gate actually proves, and where
the recorded end-to-end runs live.

---

## Running the tests

All commands run from the `code/` directory.

```bash
cd code
uv sync --dev              # installs pytest
uv run pytest tests/ -v    # run the suite
```

Lint:

```bash
uv run ruff check .
```

---

## What the tests cover

The suite in [`code/tests/test_migrate.py`](../../code/tests/test_migrate.py)
is **pure Python** — it needs no GCP project, no Alibaba credentials, and no
network. It exercises:

- **Generator** — topological sort, cross-cycle and cross-workflow dependency
  detection, partition flagging, routing guardrails, CSV overrides, duplicate
  writer dedupe, end-to-end project generation for both sample FlowSpecs, and
  Airflow 2 vs 3 DAG variants (including a real `compile()` syntax check).
- **Translator** — type translation, `${var}` and `$[yyyymmdd±N]` resolution,
  `INSERT`/`CREATE` parsing, flagged-function detection.
- **CSV layer** — row generation, field contract, round-trip.
- **Verifier** — result dataclasses and DAG parity checks.

> [!WARNING]
> **Known coverage gaps.** `extract/extract_dataworks.py`,
> `migrate/migrate.py`, `migrate/reconstruct.py` and the verifier's
> subprocess layer currently have **no test coverage**. The extractor's
> pagination and checkpoint/resume logic — the most failure-prone code in the
> repo — is untested. Treat recorded end-to-end runs, not the unit suite, as
> the evidence for those paths.

---

## What each gate proves

The migration pipeline runs four gates. Each stops the pipeline on failure.

| Gate | Proves | Does **not** prove |
| --- | --- | --- |
| `generate` | Every node was routed; artifacts were produced; anything ambiguous was flagged into `review/*.md`. | That the routing decision was *correct* for your business. |
| `compile` | The generated SQL is valid GoogleSQL and the Dataform dependency graph resolves. | That the SQL is semantically equivalent to the ODPS original. |
| `run` | The tables actually build in BigQuery and assertions pass. | That the numbers match MaxCompute. |
| `verify` | Row counts, MD5 checksums and numeric aggregates are present and non-empty; the DAG's schedule and outputs match the source FlowSpec. | Parity against live MaxCompute — that requires source access. |

> [!IMPORTANT]
> Passing all four gates means the migration is **structurally** sound, not
> that it is **numerically** correct. Per-table reconciliation against the
> source is still a human responsibility — see
> [Cutover checklist](../cutover-checklist.md).

---

## Recorded runs

| Report | Date | Scope |
| --- | --- | --- |
| [2026-08-19 · migration toolkit end-to-end](reports/2026-08-19-migration-toolkit.md) | 2026-08-19 | Full manual + automatic path against real BigQuery, extractor crash recovery, workflow selector, complex multi-node workflow. |

> [!NOTE]
> Reports are **dated, frozen records**. They are not updated when the code
> changes — a newer report supersedes an older one. Figures quoted in a report
> are true as of that run only; never copy a count out of a report into the
> guide.

---

[Index](../README.md)
