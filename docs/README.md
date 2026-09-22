# Documentation index

> **Last reviewed:** 2026-09-22

Everything you need to migrate Alibaba Cloud DataWorks pipelines to Google
Cloud. Start with a reading path below rather than reading top-to-bottom.

---

## Reading paths

Pick the one that matches why you are here.

| You are… | Read, in order |
| --- | --- |
| **New to the stack** | [01 · Concepts](01-concepts.md) → [04 · Case study](04-case-study-house-buying.md) → [Tutorial · End-to-end on GCP](tutorials/end-to-end-on-gcp.md) |
| **Planning / executing a migration** | [03 · Migration playbook](03-migration-playbook.md) → [02 · Architecture & routing](02-architecture-and-routing.md) → [Cutover checklist](cutover-checklist.md) |
| **Running the toolkit** | [`code/README.md`](../code/README.md) → [05 · Tools architecture](05-tools-architecture.md) |
| **Extending the toolkit** | [05 · Tools architecture §6](05-tools-architecture.md) → [`CONTRIBUTING.md`](../CONTRIBUTING.md) |

New to the vocabulary? Keep the [glossary](reference/glossary.md) open in a
second tab.

---

## The guide

Read in order. Each chapter ends with a link to the next.

| # | Chapter | What it covers |
| --- | --- | --- |
| 01 | [Concepts](01-concepts.md) | Plain-English explanation of both stacks and why this is a *decomposition*, not a 1:1 copy. |
| 02 | [Architecture & routing](02-architecture-and-routing.md) | How DataWorks concepts map to GCP services. **Owns the routing matrix.** |
| 03 | [Migration playbook](03-migration-playbook.md) | The canonical step-by-step process. **Owns the numbered steps.** |
| 04 | [Case study · house buying](04-case-study-house-buying.md) | One real workflow converted end to end. |
| — | [Tutorial · End-to-end on GCP](tutorials/end-to-end-on-gcp.md) | Copy-pasteable terminal walkthrough, one command at a time. |
| 05 | [Tools architecture](05-tools-architecture.md) | How the toolkit works internally. **Owns the gates and known limits.** |
| — | [Cutover checklist](cutover-checklist.md) | What to verify the night you switch production over. |
| — | [Resources](resources.md) | Curated external documentation. |

## Reference

Consulted, not read front to back.

| Page | What it answers |
| --- | --- |
| [Glossary](reference/glossary.md) | What does this term mean? |
| [Configuration](reference/configuration.md) | What environment variables exist? |
| [Variables & macros](reference/variables-and-macros.md) | How do `bizdate` and cron translate? |
| [Type mapping](reference/type-mapping.md) | What does this MaxCompute type become? |
| [DataWorks OpenAPI](reference/dataworks-openapi.md) | Which API call, which client, which fields? |
| [FlowSpec JSON](reference/flowspec-json.md) | What does this property in the extracted JSON mean? |
| [Migration matrix CSV](reference/migration-matrix-csv.md) | How do I override a routing decision? |

## Testing

| Page | What it is |
| --- | --- |
| [Testing overview](testing/README.md) | How to run the suite and what each gate proves. |
| [Report · 2026-08-19](testing/reports/2026-08-19-migration-toolkit.md) | Recorded end-to-end run against real BigQuery. |

---

## Document status

| Document | Audience | Volatility | Last reviewed |
| --- | --- | --- | --- |
| 01 · Concepts | Everyone | Low — evergreen | 2026-09-22 |
| 02 · Architecture & routing | Engineer | Medium | 2026-09-22 |
| 03 · Migration playbook | Engineer | Medium | 2026-09-22 |
| 04 · Case study | Newcomer | Low | 2026-09-22 |
| 05 · Tools architecture | Maintainer | **High** — tracks the code | 2026-09-22 |
| Tutorial | Newcomer | **High** — tracks the CLI | 2026-09-22 |
| Cutover checklist | Engineer | Medium | 2026-09-22 |
| reference/glossary | Everyone | Low | 2026-09-22 |
| reference/configuration | Engineer | **High** — tracks `.env.example` | 2026-09-22 |
| reference/variables-and-macros | Engineer | Medium | 2026-09-22 |
| reference/type-mapping | Engineer | Low | 2026-09-22 |
| reference/dataworks-openapi | Engineer | **High** — tracks Alibaba's API | 2026-09-22 |
| reference/flowspec-json | Engineer | **High** — tracks Alibaba's API | 2026-09-22 |
| reference/migration-matrix-csv | Engineer | Medium | 2026-09-22 |
| resources | Everyone | Medium — link rot | 2026-09-22 |
| testing/reports/* | Maintainer | Frozen — dated record | n/a |

> [!NOTE]
> **High-volatility pages track something that moves.** Re-verify them against
> the code or the vendor docs before relying on them. See
> [`CONTRIBUTING.md`](../CONTRIBUTING.md) for the single-source ownership rules
> that keep these pages from drifting apart.

---

[← Back to repository root](../README.md)
