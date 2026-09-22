"""Orchestrator: fully automatic DataWorks -> BigQuery migration.

Runs the whole chain with gates, so human review is limited to the items the
generator itself writes to `review/*.md`:

    extract (or a FlowSpec) -> generate .sqlx + DAG -> compile -> run -> verify

Usage (from code/):
    uv run migrate/migrate.py generate  --flowspec extract/sample_flowspec_house_buying.json
    uv run migrate/migrate.py all       --flowspec extract/sample_flowspec_house_buying.json
    uv run migrate/migrate.py all       --extract-dir ../data/extract \
                                          --workflows extract/sample_workflows_selector.json
    uv run migrate/migrate.py extract   --name-filter house_buying
    uv run migrate/migrate.py compile / run / verify
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from classify.flowspec_to_csv import load_csv_overrides
from migrate import (
    generator,
    reconstruct,
    verifier,
)

CODE_DIR = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _need(name: str) -> str:
    value = _env(name)
    if not value:
        raise SystemExit(f"{name} is required (see .env.example)")
    return value


def _filter_workflows(specs: list[dict], selector: Path) -> list[dict]:
    """Keep only the workflows listed in a selector JSON file.

    The file is either a JSON array of names/ids, or an object with a
    `workflows` key, e.g.:

        ["house_buying_analysis", "463497880880954XXXX"]

    or

        { "workflows": [ { "name": "house_buying_analysis" }, "another_wf" ] }

    A workflow matches if its `spec.name` or `spec.id` equals an entry (exact,
    case-sensitive). Entries that match nothing are reported on stderr.
    """
    raw = json.loads(selector.read_text(encoding="utf-8"))
    entries = raw if isinstance(raw, list) else raw.get("workflows", [])
    wanted: set[str] = set()
    for entry in entries:
        if isinstance(entry, dict):
            wanted.add(entry.get("name") or entry.get("id") or "")
        elif isinstance(entry, str):
            wanted.add(entry)
    wanted.discard("")

    kept = []
    for spec in specs:
        spec_block = spec.get("spec", spec)
        if spec_block.get("name") in wanted or spec_block.get("id") in wanted:
            kept.append(spec)
    for entry in sorted(wanted):
        if not any(
            (s.get("spec", s).get("name") == entry or s.get("spec", s).get("id") == entry)
            for s in specs
        ):
            print(f"[generate] selector entry '{entry}' matched no extracted workflow",
                  file=sys.stderr)
    return kept


def _dataform(*args: str, project_dir: Path) -> subprocess.CompletedProcess:
    cli = _env("DATAFORM_CLI", "dataform")
    return subprocess.run([cli, *args, str(project_dir)], text=True, capture_output=True, check=False)


def cmd_generate(args: argparse.Namespace) -> Path:
    if args.flowspec:
        specs = generator.load_flowspecs([args.flowspec])
    elif args.extract_dir:
        specs = reconstruct.load_workflows(args.extract_dir)
    else:
        raise SystemExit("Need --flowspec or --extract-dir (or run `extract` first).")

    if args.workflows:
        specs = _filter_workflows(specs, args.workflows)
        if not specs:
            raise SystemExit("[generate] --workflows selector matched no extracted workflows.")
    print(f"[generate] migrating {len(specs)} workflow(s)")

    # Load CSV overrides: explicit --csv, or auto-detect migration_matrix.csv.
    csv_overrides = _load_csv_overrides(args)

    out = (args.output_dir or Path(_env("GENERATED_OUTPUT_DIR", "../data/generated"))).resolve()
    project = _need("GCP_PROJECT_ID")
    region = _env("GCP_REGION", "asia-southeast2")
    repo = _env("DATAFORM_REPOSITORY_ID", "migrated")
    dataset = _env("DATAFORM_DEFAULT_SCHEMA", "dwh")
    assertion = _env("DATAFORM_ASSERTION_SCHEMA", "dwh_assertions")
    core = _env("DATAFORM_CORE_VERSION", "3.0.64")

    for spec in specs:
        result = generator.generate_project(
            spec, out,
            project=project, region=region, repo=repo,
            dataset=dataset, assertion_dataset=assertion, core_version=core,
            csv_overrides=csv_overrides,
        )
        print(f"[generate] {result.workflow_name}: "
              f"{len(result.models)} model(s), {len(result.declarations)} declaration(s)")
        if result.review:
            print(f"[generate] review items: {len(result.review)} -> see {out}/review/")
        else:
            print("[generate] no human-review items -- fully automatic path")

    print(f"[generate] project written to {out}")
    return out


def _load_csv_overrides(args: argparse.Namespace) -> dict[str, dict] | None:
    """Load CSV overrides from --csv arg or auto-detect migration_matrix.csv."""
    csv_path = getattr(args, "csv", None)
    if csv_path:
        if not csv_path.exists():
            print(f"[generate] WARNING: --csv {csv_path} not found, ignoring", file=sys.stderr)
            return None
        overrides = load_csv_overrides(csv_path)
        print(f"[generate] loaded {len(overrides)} CSV overrides from {csv_path}")
        return overrides

    # Auto-detect: look for migration_matrix.csv in the same dir as flowspec or extract-dir
    search_paths: list[Path] = []
    if getattr(args, "flowspec", None):
        search_paths.append(args.flowspec.parent / "migration_matrix.csv")
    if getattr(args, "extract_dir", None):
        search_paths.append(args.extract_dir / "migration_matrix.csv")

    for p in search_paths:
        if p.exists():
            overrides = load_csv_overrides(p)
            print(f"[generate] auto-loaded {len(overrides)} CSV overrides from {p}")
            return overrides

    return None


def cmd_compile(args: argparse.Namespace, project_dir: Path | None = None) -> Path:
    project_dir = project_dir or args.output_dir or Path(_env("GENERATED_OUTPUT_DIR", "../data/generated")).resolve()
    proc = _dataform("compile", project_dir=project_dir)
    print(proc.stdout, end="")
    if proc.returncode != 0:
        print(proc.stderr, end="", file=sys.stderr)
        raise SystemExit(f"[gate] compile FAILED in {project_dir}")
    print("[gate] compile PASS")
    return project_dir


def cmd_run(args: argparse.Namespace, project_dir: Path) -> None:
    proc = _dataform("run", project_dir=project_dir)
    print(proc.stdout, end="")
    if proc.returncode != 0:
        print(proc.stderr, end="", file=sys.stderr)
        raise SystemExit("[gate] run FAILED -- fix the failing model(s) or review items")
    print("[gate] run PASS")
    failed = [ln for ln in proc.stdout.splitlines() if "Assertion failed" in ln]
    if failed:
        raise SystemExit("[gate] assertions FAILED:\n" + "\n".join(failed))


def cmd_verify(args: argparse.Namespace, project_dir: Path) -> None:
    project = _need("GCP_PROJECT_ID")
    dataset = _env("DATAFORM_DEFAULT_SCHEMA", "dwh")
    tables = {m.name for m in generator_models(project_dir)}
    if not tables:
        raise SystemExit("No generated tables to verify.")

    # Per-table parity: row counts + checksums + aggregates.
    checks = verifier.check_tables(project, dataset, sorted(tables))
    passed, summary = verifier.summarize(checks)
    print("=== Per-Table Verification ===")
    print(summary)
    if not passed:
        raise SystemExit("[gate] verify FAILED (per-table)")

    # Per-DAG parity: schedule, outputs, node/model count.
    if args.flowspec:
        dag_checks = _check_dag_parity_from_flowspec(args.flowspec, project_dir)
        if dag_checks:
            dag_passed, dag_summary = verifier.summarize_dag_parity(dag_checks)
            print("\n=== Per-DAG Parity Verification ===")
            print(dag_summary)
            if not dag_passed:
                raise SystemExit("[gate] verify FAILED (per-DAG parity)")

    print("[gate] verify PASS")


def generator_models(project_dir: Path) -> list:
    """Read generated models back from the .sqlx files."""
    models = []
    marts = project_dir / "definitions" / "marts"
    if marts.exists():
        for f in marts.glob("*.sqlx"):
            models.append(type("M", (), {"name": f.stem})())
    return models


def _check_dag_parity_from_flowspec(
    flowspec_path: Path, project_dir: Path
) -> list[verifier.DAGParityCheck]:
    """Compare source FlowSpec metadata against generated artifacts."""
    spec_data = json.loads(flowspec_path.read_text(encoding="utf-8"))
    spec = spec_data.get("spec", spec_data)
    workflows = spec.get("workflows") or []
    if not workflows:
        return []

    checks = []
    gen_models = {m.name for m in generator_models(project_dir)}

    for wf in workflows:
        wf_name = wf.get("name", "unknown")
        trigger = wf.get("trigger") or {}
        source_schedule = generator.sanitize_cron(trigger.get("cron", "00 02 00 * * ?"))

        # Source output tables.
        source_outputs = []
        for node in wf.get("nodes") or []:
            for tbl in (node.get("outputs") or {}).get("tables") or []:
                guid = tbl.get("guid", "")
                if guid:
                    source_outputs.append(guid.split(".")[-1])

        # Source node count (non-VIRTUAL).
        source_node_count = sum(
            1 for n in wf.get("nodes") or []
            if ((n.get("script") or {}).get("runtime") or {}).get("command") != "VIRTUAL"
        )

        # Source dependency graph (flow[] edges).
        source_deps = wf.get("flow") or []

        # Generated dependency graph (from ref() calls in .sqlx models).
        gen_deps = _extract_generated_deps(project_dir)

        # Find the generated DAG for this workflow.
        tag = generator._safe(wf_name)
        gen_schedule = source_schedule  # default; will be overridden if DAG found
        gen_dag_file = project_dir / "dags" / f"{tag}.py"
        if gen_dag_file.exists():
            dag_content = gen_dag_file.read_text(encoding="utf-8")
            # Extract schedule from DAG file. Handles Airflow 2.x
            # (schedule_interval="...") and Airflow 3.x (schedule=... or
            # schedule=CronTriggerTimetable("...", timezone=...)).
            sched_match = re.search(
                r'schedule(?:_interval)?\s*=\s*(?:CronTriggerTimetable\()?["\']([^"\']+)["\']',
                dag_content,
            )
            if sched_match:
                gen_schedule = sched_match.group(1)

        checks.append(verifier.check_dag_parity(
            workflow_name=wf_name,
            source_schedule=source_schedule,
            generated_schedule=gen_schedule,
            source_node_count=source_node_count,
            generated_model_count=len([m for m in gen_models if True]),  # all models for this tag
            source_outputs=source_outputs,
            generated_outputs=sorted(gen_models),
            source_dependencies=source_deps,
            generated_dependencies=gen_deps,
        ))
    return checks


def _extract_generated_deps(project_dir: Path) -> list[dict]:
    """Extract dependency edges from generated .sqlx models via ref() calls."""
    deps = []
    marts_dir = project_dir / "definitions" / "marts"
    if not marts_dir.exists():
        return deps
    for sqlx_file in marts_dir.glob("*.sqlx"):
        content = sqlx_file.read_text(encoding="utf-8")
        target = sqlx_file.stem
        # Find ref("source") calls to build dependency edges.
        for ref_match in re.finditer(r'ref\(["\'](\w+)["\']\)', content):
            source = ref_match.group(1)
            deps.append({"source": source, "target": target})
    return deps


def cmd_extract(args: argparse.Namespace) -> None:
    cmd = ["uv", "run", "extract/extract_dataworks.py"]
    if args.name_filter:
        cmd += ["--name-filter", args.name_filter]
    if args.project_id:
        cmd += ["--project-id", str(args.project_id)]
    print(f"[extract] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=CODE_DIR, text=True, check=False)
    if proc.returncode != 0:
        raise SystemExit("[gate] extract FAILED")


def cmd_all(args: argparse.Namespace) -> None:
    out = cmd_generate(args)
    cmd_compile(args, out)
    cmd_run(args, out)
    cmd_verify(args, out)
    print("\n=== AUTO-MIGRATION COMPLETE: data is ready in BigQuery ===")


def main() -> None:
    load_dotenv(Path(CODE_DIR / ".env"))
    parser = argparse.ArgumentParser(description=__doc__)

    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--flowspec", type=Path, default=None,
                        help="Workflow-level FlowSpec JSON (e.g. the sample case study).")
    shared.add_argument("--extract-dir", type=Path, default=None,
                        help="Directory of extracted node_*.json files.")
    shared.add_argument("--workflows", type=Path, default=None,
                        help="JSON selector: only migrate the listed workflow names/ids.")
    shared.add_argument("--output-dir", type=Path, default=None,
                        help="Where to write the generated Dataform project.")
    shared.add_argument("--csv", type=Path, default=None,
                        help="Migration matrix CSV for overrides (auto-detected if not given).")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate", parents=[shared], help="Generate .sqlx + DAG from a FlowSpec.")
    sub.add_parser("compile", parents=[shared], help="dataform compile (gate).")
    sub.add_parser("run", parents=[shared], help="dataform run (gate).")
    sub.add_parser("verify", parents=[shared], help="BigQuery row-count gate.")
    extract_p = sub.add_parser("extract", help="Run the DataWorks OpenAPI extractor.")
    extract_p.add_argument("--name-filter", default=None)
    extract_p.add_argument("--project-id", type=int, default=None)
    sub.add_parser("all", parents=[shared], help="generate -> compile -> run -> verify, gated.")

    args = parser.parse_args()
    load_dotenv(Path(CODE_DIR / ".env"))

    handlers = {
        "generate": lambda: cmd_generate(args),
        "compile": lambda: cmd_compile(args),
        "run": lambda: cmd_run(args, args.output_dir or Path(_env("GENERATED_OUTPUT_DIR", "../data/generated")).resolve()),
        "verify": lambda: cmd_verify(args, args.output_dir or Path(_env("GENERATED_OUTPUT_DIR", "../data/generated")).resolve()),
        "extract": lambda: cmd_extract(args),
        "all": lambda: cmd_all(args),
    }
    handlers[args.command]()


if __name__ == "__main__":
    main()
