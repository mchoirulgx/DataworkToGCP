"""Convert FlowSpec JSON(s) into a human-editable migration_matrix.csv.

The CSV is the human override layer: the JSON remains the structural source
of truth (code, flow graph, DDL), while the CSV lets the user override
routing decisions, mark nodes to skip, adjust schedules, and add review notes.

Workflow:
    1. Extract -> node_*.json / workflow_*.json  (FlowSpec)
    2. flowspec_to_csv -> migration_matrix.csv   (this script)
    3. User edits CSV (gcp_target, schedule, skip, review_notes)
    4. generator reads JSON + CSV overrides -> .sqlx + DAG

Usage (from code/):
    uv run classify/flowspec_to_csv.py --flowspec extract/sample_flowspec_house_buying.json
    uv run classify/flowspec_to_csv.py --extract-dir ../data/extract --csv-out migration_matrix.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from classify.route_nodes import classify
from migrate.generator import sanitize_cron

CSV_FIELDS = [
    "workflow",
    "node_id",
    "node_name",
    "command",
    "input_tables",
    "output_tables",
    "schedule",
    "gcp_target",
    "review_notes",
    "skip",
]


def _extract_tables(tables: list[dict]) -> str:
    """Join output/input table guids into a comma-separated string."""
    names = []
    for t in tables:
        guid = t.get("guid", "")
        if guid:
            names.append(guid.split(".")[-1])
    return ",".join(names)


def _extract_schedule(workflow: dict) -> str:
    """Extract the cron schedule from a workflow's trigger."""
    trigger = workflow.get("trigger") or {}
    cron = trigger.get("cron", "00 02 00 * * ?")
    return sanitize_cron(cron)


def flowspec_to_rows(flowspec: dict) -> list[dict]:
    """Convert a single FlowSpec document into CSV rows."""
    spec = flowspec.get("spec", flowspec)
    rows: list[dict] = []

    for workflow in spec.get("workflows") or []:
        wf_name = workflow.get("name", "")
        schedule = _extract_schedule(workflow)

        for node in workflow.get("nodes") or []:
            node_id = node.get("id", "")
            if not node_id:
                continue
            command = ((node.get("script") or {}).get("runtime") or {}).get("command", "")
            target, _reason = classify(node)

            input_tables = _extract_tables(
                (node.get("inputs") or {}).get("tables") or []
            )
            output_tables = _extract_tables(
                (node.get("outputs") or {}).get("tables") or []
            )

            rows.append({
                "workflow": wf_name,
                "node_id": node_id,
                "node_name": node.get("name", ""),
                "command": command,
                "input_tables": input_tables,
                "output_tables": output_tables,
                "schedule": schedule,
                "gcp_target": target,
                "review_notes": "",
                "skip": "",
            })

    return rows


def load_csv_overrides(csv_path: Path) -> dict[str, dict]:
    """Load an edited migration_matrix.csv and return {node_id: overrides}."""
    overrides: dict[str, dict] = {}
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            node_id = row.get("node_id", "")
            if not node_id:
                continue
            overrides[node_id] = row
    return overrides


def write_csv(rows: list[dict], csv_path: Path) -> None:
    """Write rows to a CSV file."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--extract-dir", type=Path,
                       help="Directory of extracted workflow_*.json files.")
    group.add_argument("--flowspec", type=Path,
                       help="Single FlowSpec JSON file.")
    parser.add_argument("--csv-out", type=Path, default=None,
                        help="Output CSV path (default: <input-dir>/migration_matrix.csv)")
    args = parser.parse_args()

    # Load FlowSpec(s)
    if args.flowspec:
        specs = [json.loads(args.flowspec.read_text(encoding="utf-8"))]
        default_csv = args.flowspec.parent / "migration_matrix.csv"
    else:
        specs = []
        for p in sorted(args.extract_dir.glob("workflow_*.json")):
            specs.append(json.loads(p.read_text(encoding="utf-8")))
        if not specs:
            # Fallback: try flowspec-style files
            for p in sorted(args.extract_dir.glob("*.json")):
                if not p.name.startswith("node_"):
                    specs.append(json.loads(p.read_text(encoding="utf-8")))
        default_csv = args.extract_dir / "migration_matrix.csv"

    csv_path = args.csv_out or default_csv

    # Convert to rows
    all_rows: list[dict] = []
    for spec in specs:
        all_rows.extend(flowspec_to_rows(spec))

    if not all_rows:
        raise SystemExit("No nodes found in the given FlowSpec(s).")

    write_csv(all_rows, csv_path)
    print(f"Migration matrix written to {csv_path} ({len(all_rows)} nodes)")


if __name__ == "__main__":
    main()
