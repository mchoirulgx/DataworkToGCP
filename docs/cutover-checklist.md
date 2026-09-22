# Cutover checklist

> **Last reviewed:** 2026-09-22

What to verify before, during and after you move a workflow's production
schedule from DataWorks to Cloud Composer.

> [!TIP]
> Copy this page per **domain wave** and tick it off. Do not cut over an entire
> estate at once.

## Contents

- [1. Before cutover](#1-before-cutover)
- [2. Cutover night](#2-cutover-night)
- [3. After cutover](#3-after-cutover)
- [4. Rollback](#4-rollback)

---

## 1. Before cutover

### Artifacts

- [ ] All four gates pass: `generate → compile → run → verify`.
- [ ] `review/*.md` has been read **in full** and every item is either resolved
      or consciously accepted. The gates do not block on these.
- [ ] Every `TRIAGE` node has a decided target — nothing left unrouted.
- [ ] No unresolved `${var}` placeholders remain in generated SQL or DAGs.
- [ ] Flagged ODPS built-ins (`DATEADD`, `SPLIT_PART`, `EXPLODE`, UDFs …) have
      been hand-translated and reviewed.

### Schedules

> [!WARNING]
> **Re-verify every schedule by hand.** The translator strips the seconds field
> mechanically. Read the generated cron field by field — `02 00 * * *` is
> **00:02**, not 02:00. See
> [variables & macros](reference/variables-and-macros.md#3-cron-translation).

- [ ] Generated cron matches the intended wall-clock time.
- [ ] Timezone matches the source workflow (the generator uses the FlowSpec
      timezone; the hand-written sample DAG does not).
- [ ] `depends_on_past` / cross-cycle dependencies reproduce the DataWorks
      self-dependency behaviour.
- [ ] `bizdate` semantics checked for off-by-one against a known run.

### Data

- [ ] Source tables are loaded in BigQuery and refreshing on their own
      (Datastream / DTS / Dataflow), not hand-loaded once.
- [ ] Partitioned source tables: the partition redesign is agreed — MaxCompute
      string partitions do not map natively. See
      [type mapping](reference/type-mapping.md).
- [ ] **Row-count parity** against MaxCompute for every migrated table.
- [ ] **Value parity** on a sampled set of business-critical columns.

### Deployment

- [ ] Generated project pushed to the Dataform repository; release config created.
- [ ] `DATAFORM_REPOSITORY_ID` and `DATAFORM_GIT_BRANCH` point at that repo.
- [ ] Correct DAG variant chosen for the Composer image — `version_3/dags/` for
      Airflow 3, `version_2/dags/` for Airflow 2. **Check the image label; do
      not assume.**
- [ ] Composer service account can read the Dataform repo and write the target
      datasets.

---

## 2. Cutover night

- [ ] Run both systems in **parallel** for at least one full cycle and diff the
      outputs before switching off the source.
- [ ] Pause the DataWorks workflow — do not delete it.
- [ ] Unpause the Airflow DAG.
- [ ] Watch the first scheduled run end to end.
- [ ] Confirm the first run produced the expected row count.
- [ ] Confirm Dataform assertions passed.

---

## 3. After cutover

- [ ] Row counts stable across three consecutive runs.
- [ ] Runtime and cost within expectations.
- [ ] Alerting wired to the new DAG, not the old workflow.
- [ ] Downstream consumers repointed and notified.
- [ ] DataWorks workflow left paused for the agreed rollback window before
      decommissioning.

---

## 4. Rollback

Rollback is only real if you can do it without a rebuild.

- [ ] The DataWorks workflow is **paused, not deleted**, for the whole window.
- [ ] Source tables in MaxCompute are still being populated.
- [ ] A named owner can re-enable the source workflow.
- [ ] The rollback trigger is agreed in advance — e.g. parity breach, two
      consecutive failures, or a runtime blowout.

> [!CAUTION]
> Passing all four gates proves the migration is **structurally** sound, not
> **numerically** correct. Only reconciliation against the source proves the
> numbers. Do not skip §1 "Data" because the pipeline is green.

---

[← Prev: 05 · Tools architecture](05-tools-architecture.md) · [Index](README.md) · [Next: Resources →](resources.md)
