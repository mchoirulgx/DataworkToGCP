# Reference · Variables, macros & cron

> **Last reviewed:** 2026-09-22

Canonical reference for DataWorks scheduling variables, their Airflow
equivalents, and how cron expressions are translated.

> [!IMPORTANT]
> This page is the **single source of truth** for `bizdate`, date macros, and
> cron translation. Other documents link here — they must not restate these
> tables.

## Contents

- [1. DataWorks variable → Airflow macro](#1-dataworks-variable--airflow-macro)
- [2. The `bizdate` landmine](#2-the-bizdate-landmine)
- [3. Cron translation](#3-cron-translation)
- [4. Bracket offsets `$[yyyymmdd±N]`](#4-bracket-offsets-yyyymmddn)

---

## 1. DataWorks variable → Airflow macro

> **Indicative mapping.** Validate against your actual DataWorks scheduling
> configuration and timezone before relying on it.

| DataWorks variable | Typical meaning | Airflow equivalent (indicative) |
| --- | --- | --- |
| `${bizdate}` | Business date = run date − 1 day (`yyyymmdd`) | `{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}` |
| `${yyyymmdd}` | Run/scheduled date (no dash) | `{{ ds_nodash }}` |
| `$[yyyymmdd]` | Brackets = supports date/time arithmetic | `macros.ds_add` / `data_interval` with offset |
| `${cyctime}` | Cycle (scheduled) time, incl. hour/min | `{{ logical_date }}` / `{{ data_interval_start }}` |
| Self / cross-cycle dependency | Depends on previous cycle's instance | `depends_on_past=True` or prior-run sensor |

---

## 2. The `bizdate` landmine

DataWorks business date **≠** Airflow `logical_date` / `data_interval`
semantics. A naive copy produces **off-by-one-day** errors across every
migrated pipeline. Translate deliberately — never copy-paste.

---

## 3. Cron translation

DataWorks uses **Quartz-style six-field cron**, seconds first:

```
 ┌───────────── second       (0-59)
 │  ┌────────── minute       (0-59)
 │  │  ┌─────── hour         (0-23)
 │  │  │  ┌──── day of month
 │  │  │  │ ┌── month
 │  │  │  │ │ ┌ day of week  (? = unspecified)
 00 02 00 * * ?
```

Airflow uses **five-field cron**, minutes first. `sanitize_cron()`
([`generator.py`](../../code/migrate/generator.py)) drops the seconds field and
maps `?` → `*`:

| | Expression | Means |
| --- | --- | --- |
| DataWorks source | `00 02 00 * * ?` | second 00, **minute 02**, **hour 00** → **00:02 daily** |
| Generated Airflow | `02 00 * * *` | **minute 02, hour 00** → **00:02 daily** |

> [!WARNING]
> **Read the fields, don't eyeball them.** `00 02 00 * * ?` looks like "02:00"
> but is **00:02**. This is the single most common misreading when reviewing
> generated schedules.

### Known inconsistency

The hand-written reference DAG
[`code/gcp/dags/house_buying_daily.py`](../../code/gcp/dags/house_buying_daily.py)
is scheduled `0 2 * * *` (**02:00**), which does **not** match the source
workflow in `sample_flowspec_house_buying.json` (`00 02 00 * * ?` = **00:02**).
The generated DAG is the correct one. The hand-written sample predates the
generator and has not been reconciled.

### Seconds are dropped, not rounded

Any sub-minute precision in the source schedule is discarded. Every migrated
schedule must be **re-verified at cutover** — see
[03 · Migration playbook](../03-migration-playbook.md).

---

## 4. Bracket offsets `$[yyyymmdd±N]`

DataWorks bracket syntax supports arithmetic. The translator resolves offsets
into the corresponding Airflow macro:

| Source | Meaning | Generated |
| --- | --- | --- |
| `$[yyyymmdd]` | Run date | `{{ ds_nodash }}` |
| `$[yyyymmdd-1]` | Run date − 1 day | `{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}` |
| `$[yyyymmdd+7]` | Run date + 7 days | `{{ macros.ds_format(macros.ds_add(ds, 7), '%Y-%m-%d', '%Y%m%d') }}` |

Unresolved variables are **not** guessed — they are written to the generated
`review/*.md` report for a human to resolve.

---

[← Prev: Reference index](README.md) · [Index](../README.md) · [Next: Type mapping →](type-mapping.md)
