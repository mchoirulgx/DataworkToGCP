"""Extract DataWorks pipeline metadata via the OpenAPI.

Why two SDK clients?
    DataWorks splits its API across two versions on purpose:
      * 2024-05-18  -> orchestration: workflows, nodes, code (FlowSpec)
      * 2020-05-18  -> Data Map metadata: table DDL, partitions, basic info
    A common bug is calling the metadata operations on the 2024-05-18 client;
    those operations only exist on the 2020-05-18 client.

What this script does
    1. Lists every workflow in the workspace (paginated).
    2. Reads each workflow's DAG-level configuration (schedule, trigger, params).
    3. Lists the nodes inside each workflow (paginated) and reads the full
       FlowSpec of each node (code + command type + inputs/outputs).
    4. For every output table, pulls its column / partition DDL.

Resilience
    * MySQL checkpoint (PyMySQL): workflows / nodes / tables already in the DB
      are skipped on a rerun (resume after a crash); only missing items are
      appended (INSERT IGNORE), and their output files are regenerated from the
      checkpoint so the extract dir stays consistent.
    * Fixed pacing (sleep) plus exponential backoff on "Throttling.*" errors.

Run (from the code/ directory):
    uv run extract/extract_dataworks.py --name-filter house_buying
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pymysql
from alibabacloud_dataworks_public20200518 import models as meta_models
from alibabacloud_dataworks_public20200518.client import Client as MetaClient
from alibabacloud_dataworks_public20240518 import models as orch_models
from alibabacloud_dataworks_public20240518.client import Client as OrchClient
from alibabacloud_tea_openapi.models import Config
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from migrate.reconstruct import nodes_to_flowspec, spec_nodes

log = logging.getLogger("extract_dataworks")


def _is_throttle(exc: BaseException) -> bool:
    code = getattr(exc, "code", "") or ""
    return str(code).startswith("Throttling")


@retry(
    retry=retry_if_exception(_is_throttle),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    reraise=True,
)
def _call(fn, *args, **kwargs):
    return fn(*args, **kwargs)


@dataclass
class Clients:
    orch: OrchClient
    meta: MetaClient


def make_clients(region: str, access_key: str, secret_key: str) -> Clients:
    cfg = Config(
        access_key_id=access_key,
        access_key_secret=secret_key,
        region_id=region,
    )
    return Clients(orch=OrchClient(cfg), meta=MetaClient(cfg))


# ---------------------------------------------------------------------------
# Pagination helpers — token-based (guide §12 / §16: next_token pagination)
# ---------------------------------------------------------------------------
def _paged_list(pager, request_factory, page_size: int = 50, max_pages: int = 10_000):
    """Yield every item returned by a paginated List* API.

    Uses next_token-based pagination as specified in the guide (§12).
    Falls back to page_number/page_size if next_token is not available.
    """
    next_token: str | None = None
    page = 1
    while page <= max_pages:
        if next_token:
            request = request_factory(next_token=next_token, page_size=page_size)
        else:
            request = request_factory(page_number=page, page_size=page_size)
        response = _call(pager, request)
        paging = response.body.paging_info
        items = paging.workflows if hasattr(paging, "workflows") else paging.nodes
        yield from items
        # Prefer next_token if the API provides it; fall back to offset-based.
        next_token = getattr(paging, "next_token", None)
        if next_token:
            page += 1
            continue
        total = int(paging.total_count or 0)
        if page * page_size >= total or not items:
            return
        page += 1


# ---------------------------------------------------------------------------
# MySQL checkpoint (PyMySQL)
# ---------------------------------------------------------------------------
def init_store(host: str, port: int, user: str, password: str,
               database: str) -> pymysql.Connection:
    conn = pymysql.connect(host=host, port=port, user=user,
                           password=password, database=database,
                           charset="utf8mb4")
    with conn.cursor() as cur:
        for table in ("workflows", "nodes", "tables"):
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "id VARCHAR(255) PRIMARY KEY, data LONGTEXT) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
    conn.commit()
    return conn


def _to_dict(value):
    """Convert SDK model objects / lists into JSON-serializable dicts."""
    if hasattr(value, "to_map"):
        return value.to_map()
    if isinstance(value, list):
        return [_to_dict(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_dict(v) for k, v in value.items()}
    return value


def save_json(conn: pymysql.Connection, table: str, key: str, value: dict) -> None:
    """Append-only write: never touches a row that already exists."""
    with conn.cursor() as cur:
        cur.execute(f"INSERT IGNORE INTO {table} VALUES (%s, %s)",
                    (key, json.dumps(value, ensure_ascii=False)))
    conn.commit()


def load_all(conn: pymysql.Connection, table: str) -> dict[str, dict]:
    """Read every checkpoint row: {id: parsed data}."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, data FROM {table}")
        return {row[0]: json.loads(row[1]) for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# CSV checkpoint (guide §16: append row-by-row to CSV for crash safety)
# ---------------------------------------------------------------------------
def _csv_checkpoint_path(output_dir: Path, table: str) -> Path:
    return output_dir / f"checkpoint_{table}.csv"


def csv_save(output_dir: Path, table: str, key: str, value: dict) -> None:
    """Append-only CSV checkpoint: one row per extracted item.

    Guide §16: "Append row-by-row to CSV the moment each node is processed --
    never hold thousands of scripts in RAM."
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = _csv_checkpoint_path(output_dir, table)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if not file_exists:
            writer.writerow(["id", "data"])
        writer.writerow([key, json.dumps(value, ensure_ascii=False)])


def csv_load_all(output_dir: Path, table: str) -> dict[str, dict]:
    """Read every CSV checkpoint row: {id: parsed data}."""
    path = _csv_checkpoint_path(output_dir, table)
    if not path.exists():
        return {}
    result: dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if "id" in row and "data" in row:
                result[row["id"]] = json.loads(row["data"])
    return result


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------
def extract(project_id: int, env: str, clients: Clients, store: pymysql.Connection,
            output_dir: Path, name_filter: str | None, delay: float) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0

    # Checkpoint state: anything already stored is treated as done and skipped.
    # Both MySQL and CSV checkpoints are used (guide §16: CSV for crash safety).
    done_wfs = load_all(store, "workflows")
    done_nodes = load_all(store, "nodes")
    done_tables = load_all(store, "tables")
    # Also load CSV checkpoints and merge (CSV is the guide-recommended store).
    csv_wfs = csv_load_all(output_dir, "workflows")
    csv_nodes = csv_load_all(output_dir, "nodes")
    csv_tables = csv_load_all(output_dir, "tables")
    done_wfs.update(csv_wfs)
    done_nodes.update(csv_nodes)
    done_tables.update(csv_tables)
    node_ids = set(done_nodes)
    nodes_by_wf: dict[str, list[dict]] = {}
    for node in done_nodes.values():
        nodes_by_wf.setdefault(str(node.get("workflow_id", "")), []).append(node)

    workflows = _paged_list(
        clients.orch.list_workflows,
        lambda page_number, page_size: orch_models.ListWorkflowsRequest(
            project_id=project_id,
            env_type=env,
            name=name_filter,
            page_number=page_number,
            page_size=page_size,
        ),
    )
    workflows = list(workflows)

    for wf in workflows:
        wf_id = str(getattr(wf, "id", ""))
        wf_name = getattr(wf, "name", "")
        log.info("Workflow %s (%s)", wf_name, wf_id)
        if not wf_id:
            continue

        if wf_id in done_wfs:
            # Resume: this workflow was fully extracted (its row is only written
            # after every node). Skip all OpenAPI calls and regenerate the output
            # files from the checkpoint so the extract dir is consistent.
            wf_row = done_wfs[wf_id]
            log.info("Workflow %s (%s) already extracted -> resume from checkpoint", wf_name, wf_id)
            wf_node_specs = []
            for node in nodes_by_wf.get(wf_id, []):
                (output_dir / f"node_{node['id']}.json").write_text(
                    json.dumps(node, ensure_ascii=False, indent=2))
                wf_node_specs.extend(spec_nodes(node.get("spec") or {}))
            wf_doc = nodes_to_flowspec(
                wf_row.get("name") or wf_name, wf_id, wf_node_specs,
                trigger=wf_row.get("trigger"),
                variables=wf_row.get("parameters"),
            )
            (output_dir / f"workflow_{wf_id}.json").write_text(
                json.dumps(wf_doc, ensure_ascii=False, indent=2))
            continue

        workflow_detail = None
        try:
            detail = _call(clients.orch.get_workflow,
                           orch_models.GetWorkflowRequest(env_type=env, id=wf_id))
            if detail.body.workflow is not None:
                workflow_detail = {
                    "id": wf_id,
                    "name": getattr(detail.body.workflow, "name", wf_name),
                    "trigger": _to_dict(detail.body.workflow.trigger),
                    "parameters": getattr(detail.body.workflow, "parameters", None),
                    "tasks": _to_dict(detail.body.workflow.tasks),
                    "dependencies": _to_dict(detail.body.workflow.dependencies),
                }
        except Exception as exc:  # noqa: BLE001 - keep going on one bad workflow
            log.warning("get_workflow failed for %s: %s", wf_id, exc)

        nodes = _paged_list(
            clients.orch.list_nodes,
            lambda page_number, page_size, wf_id=wf_id: orch_models.ListNodesRequest(
                project_id=project_id,
                container_id=wf_id,
                page_number=page_number,
                page_size=page_size,
            ),
        )
        wf_node_specs: list[dict] = []
        for node in nodes:
            node_id = str(getattr(node, "id", ""))
            node_name = getattr(node, "name", "")
            if not node_id:
                continue
            if node_id in node_ids:
                # Resume: node was extracted before a crash -> skip the API and
                # regenerate its file from the checkpoint.
                stored = done_nodes[node_id]
                (output_dir / f"node_{node_id}.json").write_text(
                    json.dumps(stored, ensure_ascii=False, indent=2))
                wf_node_specs.extend(spec_nodes(stored.get("spec") or {}))
                log.info("  node %s already extracted -> skip", node_name)
                continue
            time.sleep(delay)  # static pacing: stay under QPS
            try:
                resp = _call(clients.orch.get_node,
                             orch_models.GetNodeRequest(project_id=str(project_id), id=node_id))
                node_body = resp.body.node
                spec_text = node_body.spec
                if isinstance(spec_text, str):
                    spec = json.loads(spec_text)
                else:
                    spec = spec_text
                wf_node_specs.extend(spec_nodes(spec))
                save_json(store, "nodes", node_id,
                          {"id": node_id, "workflow_id": wf_id, "name": node_name,
                           "spec": spec})
                csv_save(output_dir, "nodes", node_id,
                         {"id": node_id, "workflow_id": wf_id, "name": node_name,
                          "spec": spec})
                (output_dir / f"node_{node_id}.json").write_text(
                    json.dumps({"id": node_id, "workflow_id": wf_id, "name": node_name,
                                "spec": spec}, ensure_ascii=False, indent=2))

                extract_table_meta(node_id, spec, clients, store, delay, done_tables, output_dir)
            except Exception as exc:  # noqa: BLE001
                log.warning("get_node failed for %s (%s): %s", node_name, node_id, exc)
            count += 1

        # Mark the workflow done only after every node is stored: a crash before
        # this point leaves it unfinished so a rerun re-processes (and skips the
        # nodes already in the checkpoint via the node-level resume above).
        save_json(store, "workflows", wf_id, workflow_detail or {"id": wf_id, "name": wf_name})
        csv_save(output_dir, "workflows", wf_id, workflow_detail or {"id": wf_id, "name": wf_name})

        # Workflow-level FlowSpec (trigger from GetWorkflow + rebuilt flow[]).
        # Without the trigger the generator would lose the schedule; this is the
        # file --extract-dir / migrate.py consume for real estates.
        wf_doc = nodes_to_flowspec(
            wf_name, wf_id, wf_node_specs,
            trigger=(workflow_detail or {}).get("trigger"),
            variables=(workflow_detail or {}).get("parameters"),
        )
        (output_dir / f"workflow_{wf_id}.json").write_text(
            json.dumps(wf_doc, ensure_ascii=False, indent=2))

    log.info("Done. Extracted %s nodes.", count)

    # Auto-generate migration_matrix.csv from extracted workflow JSONs.
    _generate_csv(output_dir)

    return count


def _generate_csv(output_dir: Path) -> None:
    """Generate migration_matrix.csv from all workflow_*.json in output_dir."""
    from classify.flowspec_to_csv import flowspec_to_rows, write_csv

    workflow_files = sorted(output_dir.glob("workflow_*.json"))
    if not workflow_files:
        log.info("No workflow_*.json found -> skipping CSV generation")
        return

    all_rows: list[dict] = []
    for wf_path in workflow_files:
        spec = json.loads(wf_path.read_text(encoding="utf-8"))
        all_rows.extend(flowspec_to_rows(spec))

    if all_rows:
        csv_path = output_dir / "migration_matrix.csv"
        write_csv(all_rows, csv_path)
        log.info("Migration matrix written to %s (%s nodes)", csv_path, len(all_rows))


def extract_table_meta(node_id: str, spec: dict, clients: Clients,
                       store: pymysql.Connection, delay: float,
                       done_tables: set[str], output_dir: Path) -> None:
    """Pull DDL + basic info for every table a node writes to (skipping tables already stored).

    Three metadata API calls per table (guide §12):
      1. GetMetaTableColumn  -> column DDL
      2. GetMetaTablePartition -> partition info
      3. GetMetaTableBasicInfo -> table description, owner, lifecycle, etc.
    """
    try:
        nodes = spec.get("spec", {}).get("nodes", []) or []
    except AttributeError:
        nodes = []
    for node in nodes:
        outputs = node.get("outputs", {}) or {}
        for table in outputs.get("tables", []) or []:
            guid = table.get("guid", "")
            if not guid:
                continue
            if guid in done_tables:
                continue
            time.sleep(delay)
            try:
                cols_resp = _call(clients.meta.get_meta_table_column,
                                  meta_models.GetMetaTableColumnRequest(
                                      table_guid=guid,
                                      page_num=1,
                                      page_size=100,
                                  ))
                columns = [c.to_map() for c in
                           (cols_resp.body.data.column_list or [])]
                time.sleep(delay)
                parts_resp = _call(clients.meta.get_meta_table_partition,
                                   meta_models.GetMetaTablePartitionRequest(
                                       table_guid=guid,
                                       page_number=1,
                                       page_size=100,
                                   ))
                partitions = [p.to_map() for p in
                              (parts_resp.body.data.data_entity_list or [])]
                time.sleep(delay)
                basic_info: dict = {}
                try:
                    basic_resp = _call(clients.meta.get_meta_table_basic_info,
                                       meta_models.GetMetaTableBasicInfoRequest(
                                           table_guid=guid,
                                       ))
                    if basic_resp.body.data:
                        basic_info = basic_resp.body.data.to_map()
                except Exception as exc:  # noqa: BLE001
                    log.warning("get_meta_table_basic_info failed for %s: %s", guid, exc)
                save_json(store, "tables", guid,
                          {"guid": guid, "columns": columns, "partitions": partitions,
                           "basic_info": basic_info})
                csv_save(output_dir, "tables", guid,
                         {"guid": guid, "columns": columns, "partitions": partitions,
                          "basic_info": basic_info})
                log.info("  table meta: %s (%s cols, %s partitions, basic_info=%s)",
                         guid, len(columns), len(partitions),
                         "yes" if basic_info else "no")
            except Exception as exc:  # noqa: BLE001
                log.warning("table meta failed for %s: %s", guid, exc)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", type=int,
                        default=int(os.getenv("DATAWORKS_PROJECT_ID", "0")))
    parser.add_argument("--env", default=os.getenv("DATAWORKS_ENV", "Prod"))
    parser.add_argument("--name-filter", default=None,
                        help="Only extract workflows whose name contains this string.")
    parser.add_argument("--output-dir", default=os.getenv("EXTRACT_OUTPUT_DIR", "../data/extract"))
    parser.add_argument("--delay", type=float,
                        default=float(os.getenv("CALL_DELAY_SECONDS", "0.5")))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    access_key = os.getenv("ALIBABA_ACCESS_KEY", "")
    secret_key = os.getenv("ALIBABA_SECRET_KEY", "")
    region = os.getenv("ALIBABA_REGION", "ap-southeast-5")

    if not access_key or not secret_key:
        raise SystemExit("ALIBABA_ACCESS_KEY / ALIBABA_SECRET_KEY not set. Copy .env.example to .env first.")
    if not args.project_id:
        raise SystemExit("DATAWORKS_PROJECT_ID not set (or pass --project-id).")

    output_dir = Path(args.output_dir)
    store = init_store(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "dataworks_extract"),
    )
    clients = make_clients(region, access_key, secret_key)

    log.info("Extracting project=%s env=%s region=%s -> %s",
             args.project_id, args.env, region, output_dir)
    extract(args.project_id, args.env, clients, store, output_dir, args.name_filter, args.delay)


if __name__ == "__main__":
    main()
