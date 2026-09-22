# Contributing

Thanks for improving this guide.

---

## Development setup

All commands run from the `code/` directory.

```bash
cd code
uv sync --dev              # install dependencies + pytest
cp .env.example .env       # then fill in your values
```

You will also need, outside Python:

| Tool | Why | Install |
| --- | --- | --- |
| `uv` | Python project manager | https://docs.astral.sh/uv/ |
| Dataform CLI | `compile` / `run` gates | `npm i -g @dataform/cli` |
| `bq` | `verify` gate | Google Cloud SDK |
| MySQL | Resumable extraction checkpoint | Docker is fine for local work |

## Before you open a PR

```bash
cd code
uv run pytest tests/ -v
uv run ruff check .
```

Both must pass. See [docs/testing](docs/testing/README.md) for what the suite
does and does not cover.

---

## Documentation rules

The docs are the product here, so they have rules.

### 1. One topic, one owner

Every fact lives in exactly **one** file. Everywhere else links to it. This is
the rule that stops the guide drifting into contradicting itself.

| Topic | Sole owner | Everyone else |
| --- | --- | --- |
| Routing matrix | [`docs/02-architecture-and-routing.md`](docs/02-architecture-and-routing.md) | link |
| The canonical migration steps | [`docs/03-migration-playbook.md`](docs/03-migration-playbook.md) | link **by step name** |
| Gates & pipeline internals | [`docs/05-tools-architecture.md`](docs/05-tools-architecture.md) | link |
| CLI invocations & flags | [`code/README.md`](code/README.md) | link |
| Environment variables | [`code/.env.example`](code/.env.example) → [`docs/reference/configuration.md`](docs/reference/configuration.md) | link |
| CSV override layer | [`docs/reference/migration-matrix-csv.md`](docs/reference/migration-matrix-csv.md) | link |
| `bizdate` / cron / macros | [`docs/reference/variables-and-macros.md`](docs/reference/variables-and-macros.md) | link |
| Type & partition mapping | [`docs/reference/type-mapping.md`](docs/reference/type-mapping.md) | link |
| API operations & fields | [`docs/reference/dataworks-openapi.md`](docs/reference/dataworks-openapi.md) | link |
| Terminology | [`docs/reference/glossary.md`](docs/reference/glossary.md) | link |
| Test evidence & counts | [`docs/testing/reports/`](docs/testing/reports/) | link — **never restate a number** |

> [!IMPORTANT]
> If you catch yourself pasting a table that already exists somewhere else,
> stop and write a link instead.

### 2. File conventions

- One `# H1` per file, at the top.
- `> **Last reviewed:** YYYY-MM-DD` immediately under the H1. Update it when you
  touch the file.
- No skipped heading levels.
- In-page `## Contents` TOC for anything over ~150 lines.
- Navigation footer as the last line, after a `---`:

  ```
  [← Prev: <title>](<path>) · [Index](<path>) · [Next: <title> →](<path>)
  ```

- Relative links only. Run the link check below before pushing.
- Product name is **DataWorks** (capital W) in prose.

### 3. Commands in docs

- Every shell snippet runs from `code/`. Say so once; don't repeat it per block.
- Use `uv run <script>`, never `uv run python <script>`.
- Data paths are therefore `../data/...`.

### 4. Never commit

- Real project IDs, account names, email addresses, or absolute home paths.
  Use `$GCP_PROJECT_ID`, `$REPO_ROOT`, `$WORK_DIR`.
- `.env`, credentials, or `.df-credentials.json`.
- Numbers copied out of a dated test report into the guide.

### 5. Link check

```bash
npx --yes markdown-link-check --quiet README.md
```

Or check every file:

```bash
find . -name '*.md' -not -path './node_modules/*' \
  -exec npx --yes markdown-link-check --quiet {} \;
```

---

## Extending the toolkit

See [05 · Tools architecture §6](docs/05-tools-architecture.md) for how to add
a node type, an ODPS function rewrite, or a variable. Every change there needs
a matching test in [`code/tests/test_migrate.py`](code/tests/test_migrate.py).

---

## Reporting a problem

Open an issue with:

- what you ran (the exact command, from `code/`),
- what you expected,
- what happened, including the failing gate and any `review/*.md` output,
- versions: `uv --version`, `dataform --version`, Python, Composer image.
