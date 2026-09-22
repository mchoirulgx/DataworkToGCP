# Reference · MaxCompute → BigQuery type mapping

> **Last reviewed:** 2026-09-22

Canonical type-conversion table and the two areas that need real design rather
than lookup.

> [!IMPORTANT]
> This page is the **single source of truth** for type and partition mapping.
> Other documents link here — they must not restate this table.

---

## 1. Type map

| MaxCompute | BigQuery | Notes |
| --- | --- | --- |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | `INT64` | All integer widths collapse to INT64 |
| `FLOAT` / `DOUBLE` | `FLOAT64` | Single precision folds into FLOAT64 |
| `DECIMAL(p,s)` | `NUMERIC` / `BIGNUMERIC` | Choose by precision/scale; BIGNUMERIC for wide ranges |
| `STRING` / `VARCHAR` / `CHAR` | `STRING` | Watch source length limits |
| `DATETIME` | `DATETIME` or `TIMESTAMP` | Millisecond → microsecond; pick per timezone handling |
| `DATE` | `DATE` | Direct |
| `TIMESTAMP` | `TIMESTAMP` | Verify timezone semantics |
| `BOOLEAN` | `BOOL` | Direct |
| `BINARY` | `BYTES` | Direct |
| `MAP` / `ARRAY` / `STRUCT` | `ARRAY` / `STRUCT` | Remodel; BigQuery has no native MAP — use repeated STRUCT |
| Partition column (`pt`/`ds` string) | Partitioning / clustering | String partitions don't map to native partitioning — **redesign** |

---

## 2. Two areas that need design, not lookup

### 2.1 Type collapse & precision

Integer widths all collapse to `INT64` and `FLOAT`/`DOUBLE` collapse to
`FLOAT64`. This is lossless in range but loses the *intent* of the original
width. `DECIMAL(p,s)` is the one to watch: choose `NUMERIC` (38 digits, scale 9)
or `BIGNUMERIC` (76 digits) deliberately based on the source precision.

### 2.2 Partition-model mismatch

MaxCompute *string* partitions such as `pt/ds='20240101'` do **not** map onto
BigQuery's `DATE`/`TIMESTAMP`/`INT` partitioning. Many become **clustering
keys** or require partition redesign.

The generator does not silently guess here — partitioned source tables are
**flagged** in the generated `review/*.md` report for a human decision.

---

[← Prev: Variables & macros](variables-and-macros.md) · [Index](../README.md) · [Next: DataWorks OpenAPI →](dataworks-openapi.md)
