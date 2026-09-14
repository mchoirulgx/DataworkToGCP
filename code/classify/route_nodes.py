"""Route every DataWorks node to its GCP target, based on the routing matrix.

The routing matrix (see docs/02-architecture-and-routing.md) decides, in
priority order, where each node's logic should live on the GCP stack:

    1. Expressible as set-based SQL?       -> Dataform (.sqlx)
    2. Light, procedural Python?           -> PythonOperator (Composer)
    3. Data-heavy Python (big joins etc.)? -> Dataflow (Beam), triggered by Composer
    4. Genuinely PySpark?                  -> Dataproc
    plus: Shell -> audit each; Data Integration -> DTS/Datastream/Dataflow;
          VIRTUAL/control -> EmptyOperator / branching operators.

This script reads extracted node FlowSpecs and prints a routing table. Large
PyODPS nodes are flagged for human triage instead of auto-routed.

Usage:
    uv run classify/route_nodes.py --extract-dir ../data/extract
    uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

# DataWorks FlowSpec runtime.command -> (gcp_target, reason)
# SQL-heavy Python goes to Dataform; the rest are heuristic and need review.
ROUTING: dict[str, tuple[str, str]] = {
    "ODPS_SQL": ("Dataform .sqlx", "In-warehouse SQL -> BigQuery via Dataform"),
    "ODPS_SPARK": ("Dataproc (PySpark)", "Genuinely distributed Spark only"),
    "PYODPS": ("TRIAGE", "PyODPS -> push SQL down to Dataform; else Dataflow/PythonOperator"),
    "PYTHON": ("TRIAGE", "Light -> PythonOperator; data-heavy -> Dataflow"),
    "DIDE_SHELL": ("TRIAGE", "Shell script -> BashOperator or PythonOperator; audit each before porting"),
    "VIRTUAL": ("EmptyOperator", "No-op / flow control"),
    "DIDE_DO_WHILE": ("Branching / loop operators", "Control flow in Airflow"),
    "DIDE_FOR_EACH": ("Branching / loop operators", "Control flow in Airflow"),
    "DATA_INTEGRATION": ("BigQuery DTS / Datastream / Dataflow", "Source->sink sync, not SQL"),
    "DI": ("BigQuery DTS / Datastream / Dataflow", "Source->sink sync, not SQL"),
    "MYSQL": ("Datastream / Dataflow", "DB ops -> BigQuery via sync pipeline"),
    "POSTGRESQL": ("Datastream / Dataflow", "DB ops -> BigQuery via sync pipeline"),
}

# Heuristics that make a PYODPS/PYTHON node look "data-heavy".
HEAVY_MARKERS = ["read_sql(", "o2o", ".apply(", "spark", "join", "groupby", "pivot"]


def flatten_specs(spec: dict) -> list[tuple[str, dict]]:
    """Return (node_id, node_dict) pairs from a FlowSpec-like document."""
    result: list[tuple[str, dict]] = []
    inner = spec.get("spec", spec)
    if "nodes" in inner:
        for node in inner.get("nodes", []) or []:
            result.append((node.get("id", "?"), node))
    for workflow in inner.get("workflows", []) or []:
        for node in workflow.get("nodes", []) or []:
            result.append((node.get("id", "?"), node))
    return result


def classify(node: dict) -> tuple[str, str]:
    command = ((node.get("script") or {}).get("runtime") or {}).get("command", "")
    target, reason = ROUTING.get(command, ("PythonOperator (review)", "Unknown command -> human review"))

    if command in ("PYODPS", "PYTHON"):
        content = (node.get("script") or {}).get("content", "") or ""
        lowered = content.lower()
        # Really just SQL submitted through Python? Push it down to Dataform.
        if "o2o" in lowered or content.strip().lstrip().lower().startswith(("sql", "select", "insert")):
            target, reason = "Dataform .sqlx", "PyODPS that is really SQL -> push down to BigQuery SQL"
        elif any(marker in lowered for marker in HEAVY_MARKERS):
            target, reason = "Dataflow (Beam)", "Data-heavy Python -> keep off the Composer worker"
        else:
            target, reason = "PythonOperator", "Lightweight procedural Python in Composer"

    return target, reason


def route_nodes(flowspec_paths: list[Path], csv_out: Path | None) -> None:
    rows = []
    for path in flowspec_paths:
        spec = json.loads(path.read_text(encoding="utf-8"))
        for node_id, node in flatten_specs(spec):
            target, reason = classify(node)
            rows.append({
                "source_file": path.name,
                "node_id": node_id,
                "node_name": node.get("name", ""),
                "command": ((node.get("script") or {}).get("runtime") or {}).get("command", ""),
                "gcp_target": target,
                "reason": reason,
            })

    if not rows:
        raise SystemExit("No nodes found in the given files.")

    width = max(len(r["gcp_target"]) for r in rows)
    print(f"{'SOURCE':<42} {'NODE':<24} {'COMMAND':<16} {'GCP TARGET':<{width}}")
    print("-" * (42 + 24 + 16 + width + 9))
    for r in rows:
        print(f"{r['source_file']:<42} {r['node_name']:<24} {r['command']:<16} {r['gcp_target']:<{width}}")
        if r["gcp_target"] == "TRIAGE":
            print(f"    ! Triage required: {r['reason']}")

    if csv_out:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        with csv_out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nRouting table written to {csv_out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--extract-dir", type=Path,
                       help="Directory of extracted node_*.json files.")
    group.add_argument("--flowspec", type=Path,
                       help="Single FlowSpec JSON file (e.g. the sample case study).")
    parser.add_argument("--csv-out", type=Path, default=None)
    args = parser.parse_args()

    if args.extract_dir:
        paths = sorted(args.extract_dir.glob("node_*.json"))
        if not paths:
            raise SystemExit(f"No node_*.json files found under {args.extract_dir}")
    else:
        paths = [args.flowspec]

    route_nodes(paths, args.csv_out)


if __name__ == "__main__":
    main()
