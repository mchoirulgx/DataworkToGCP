# Reference · DataWorks OpenAPI

> **Last reviewed:** 2026-09-22

Canonical reference for the two DataWorks API surfaces, their request/response
shapes, and error handling.

> [!IMPORTANT]
> This page is the **single source of truth** for API operations and fields.
> Other documents keep at most a one-paragraph warning and link here.

## Contents

- [1. Two clients (mandatory)](#1-two-clients-mandatory)
- [2. Key request fields](#2-key-request-fields)
- [3. Error codes & resilience](#3-error-codes--resilience)

---

## 1. Two clients (mandatory)

Two API clients are required. They live in **different SDK packages** with
**different endpoints**. Using the wrong one silently returns errors.

| Purpose | Package | Operations |
| --- | --- | --- |
| Orchestration | `alibabacloud-dataworks-public20240518` | `ListWorkflows`, `GetWorkflow`, `ListNodes`, `GetNode`, `ListWorkflowDefinitions`, `GetWorkflowDefinition` |
| Table metadata | `alibabacloud-dataworks-public20200518` | `GetMetaTableColumn`, `GetMetaTablePartition`, `GetMetaTableBasicInfo`, `ListTables` |

> [!WARNING]
> A common failure is calling `get_meta_table_column()` on the `2024-05-18`
> client. The metadata operations exist only on the `2020-05-18` client.

---

## 2. Key request fields

SDK v8+ / current:

| Operation | Request fields | Response access |
| --- | --- | --- |
| `ListWorkflows` | `project_id` (req), `env_type`, `name`, `page_number`, `page_size` | `resp.body.paging_info.workflows` |
| `GetWorkflow` | `env_type`, `id` | `resp.body.workflow` (trigger, tasks, parameters, dependencies) |
| `ListNodes` | `project_id` (req), `container_id` (workflow id), `page_number`, `page_size` | `resp.body.paging_info.nodes` |
| `GetNode` | `project_id`, `id` | `resp.body.node.spec` (**FlowSpec string** — parse JSON) |
| `GetMetaTableColumn` | `table_guid` (e.g. `odps.proj.table`), `page_num`, `page_size` | `resp.body.data.column_list` |
| `GetMetaTablePartition` | `table_guid`, `page_number`, `page_size` | `resp.body.data.data_entity_list` (MaxCompute/EMR only) |

> **Note:** `Id` was `Long` in SDKs < 8.0.0 and is `String` in ≥ 8.0.0 for the
> node APIs — cast defensively.

---

## 3. Error codes & resilience

| Error | Meaning | Handling |
| --- | --- | --- |
| `Throttling.User` / `Throttling.API` | QPS limit reached | Static pacing + exponential backoff (`2s → 4s → 8s`), then retry same ID |
| `Invalid.Tenant.ConnectionNotExists` | Wrong tenant/connection | Check workspace/tenant config |
| Result too large (>10,000 rows / >10 MB) | Query-result display limit | Use Tunnel download / export instead |

For a large estate, raise a support ticket to **temporarily lift the
`dataworks-public` QPS limit** during the migration window.

---

[← Prev: Type mapping](type-mapping.md) · [Index](../README.md) · [Next: FlowSpec JSON →](flowspec-json.md)
