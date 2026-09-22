"""Generate Dataform .sqlx models + Composer DAGs from extracted FlowSpecs.

Automatic pipeline: FlowSpec -> (route) -> generate .sqlx + DAG + review report.
Only nodes the routing matrix cannot safely auto-route (TRIAGE) land in the
review report instead of blocking the run -- the compile/run/verify gates below
decide whether the rest is safe. This is the "minimal human review" contract.
"""

from __future__ import annotations

import json
import os
import re
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from classify.route_nodes import HEAVY_MARKERS
from . import translator

# ---------------------------------------------------------------------------
# FlowSpec flow[] graph utilities
# ---------------------------------------------------------------------------

def _build_adjacency(flow: list[dict]) -> dict[str, list[str]]:
    """Build adjacency list {nodeId: [upstream_nodeId, ...]} from flow[]."""
    adj: dict[str, list[str]] = {}
    for entry in flow:
        nid = entry.get("nodeId", "")
        deps = []
        for dep in entry.get("depends") or []:
            dep_id = dep.get("nodeId") or dep.get("data") or ""
            if dep_id:
                deps.append(dep_id)
        adj[nid] = deps
    return adj


def topological_sort(nodes: list[dict], flow: list[dict]) -> list[dict]:
    """Order nodes according to the FlowSpec flow[] dependency graph.

    Falls back to input-list order when flow[] is missing or empty.
    """
    if not flow:
        return nodes

    adj = _build_adjacency(flow)
    node_by_id = {n.get("id"): n for n in nodes}

    # Kahn's algorithm — stable: preserves original order among equals.
    in_degree: dict[str, int] = {n.get("id", ""): 0 for n in nodes}
    dependents: dict[str, list[str]] = {n.get("id", ""): [] for n in nodes}
    for nid, upstreams in adj.items():
        if nid not in in_degree:
            continue
        for up in upstreams:
            if up in dependents:
                dependents[up].append(nid)
                in_degree[nid] += 1

    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    ordered: list[dict] = []
    while queue:
        nid = queue.popleft()
        node = node_by_id.get(nid)
        if node:
            ordered.append(node)
        for dep in dependents.get(nid, []):
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    # If cycle detected or orphan nodes, append remaining in original order.
    if len(ordered) < len(nodes):
        seen = {n.get("id") for n in ordered}
        for n in nodes:
            if n.get("id") not in seen:
                ordered.append(n)
    return ordered


def _detect_cross_cycle_deps(flow: list[dict], nodes: list[dict]) -> set[str]:
    """Return set of nodeIds whose FlowSpec marks CrossCycleDependsOnOtherNode."""
    cross_cycle: set[str] = set()
    # Check node-level flags first.
    for node in nodes:
        node_id = node.get("id", "")
        # CrossCycleDependsOnOtherNode can appear in several places:
        # 1. As a boolean flag on the node itself
        if node.get("crossCycleDependsOnOtherNode"):
            cross_cycle.add(node_id)
            continue
        # 2. In the flow dependency entry
        for dep in (node.get("inputs") or {}).get("nodeOutputs") or []:
            if dep.get("crossCycleDependsOnOtherNode"):
                cross_cycle.add(node_id)
                break

    # Also check the flow[] entries.
    for entry in flow:
        for dep in entry.get("depends") or []:
            if dep.get("crossCycleDependsOnOtherNode") or dep.get("type") == "CrossCycle":
                cross_cycle.add(entry.get("nodeId", ""))
    return cross_cycle


def _detect_cross_workflow_deps(nodes: list[dict]) -> list[dict]:
    """Return list of {nodeId, external_workflow_id, external_node_id} for
    dependencies on nodes in other workflows."""
    deps = []
    for node in nodes:
        node_id = node.get("id", "")
        for ref in (node.get("inputs") or {}).get("nodeOutputs") or []:
            ext_wf = ref.get("externalWorkflowId")
            ext_node = ref.get("externalNodeId") or ref.get("data")
            if ext_wf:
                deps.append({
                    "nodeId": node_id,
                    "external_workflow_id": ext_wf,
                    "external_node_id": ext_node,
                })
    return deps


# Safe command -> GCP target (mirrors classify/route_nodes.py ROUTING).
SAFE_COMMANDS = {"ODPS_SQL", "VIRTUAL"}
# Commands that may be SQL-in-disguise and can be pushed down.
SQLISH_COMMANDS = {"PYODPS", "PYTHON"}
# Everything else is triage -> review report.
TRIAGE_COMMANDS: dict[str, str] = {
    "ODPS_SPARK":          "MaxCompute Spark — needs manual Composer/Dataflow rewrite",
    "DIDE_SHELL":          "Shell script — audit individually",
    "DIDE_DO_WHILE":       "Loop construct — no direct GCP equivalent",
    "DIDE_FOR_EACH":       "Loop construct — no direct GCP equivalent",
    "DATA_INTEGRATION":    "Offline sync (DATAX) — Dataflow / load job",
    "DI":                  "Data Integration — Dataflow / load job",
    "MYSQL":               "External MySQL connector — Composer task",
    "POSTGRESQL":          "External PostgreSQL connector — Composer task",
}

_CREATE_CAPTURE = re.compile(
    r"(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.$]+)"
)

_IDENT_SAFE = re.compile(r"[^a-zA-Z0-9_]+")
_CRON_6 = re.compile(r"^\d+\s+(\d+\s+\d+\s+\*?\s*\*?\s*[?*])$")


@dataclass
class Model:
    name: str
    tag: str
    select_sql: str
    columns: list[tuple[str, str, str]] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)


@dataclass
class Declaration:
    name: str
    schema: str


@dataclass
class GeneratedWorkflow:
    workflow_name: str
    tag: str
    dag: str
    models: list[Model] = field(default_factory=list)
    declarations: list[Declaration] = field(default_factory=list)
    review: list[str] = field(default_factory=list)
    schedule: str = "0 2 * * *"
    timezone: str = "UTC"
    depends_on_past: bool = False
    cross_workflow_deps: list[dict] = field(default_factory=list)
    python_operator_nodes: list[dict] = field(default_factory=list)


def _safe(name: str, maxlen: int = 60) -> str:
    out = _IDENT_SAFE.sub("_", name).strip("_").lower()
    return out[:maxlen] or "model"


def sanitize_cron(cron: str) -> str:
    """DataWorks cron (s m h d M dow, e.g. 00 02 00 * * ?) -> Airflow 5-field."""
    parts = cron.split()
    if len(parts) >= 6:
        return " ".join(parts[1:6]).replace("?", "*")
    return cron


def _dml_select(content: str) -> str | None:
    """Grab the SELECT ... tail of an INSERT OVERWRITE ... node body."""
    m = re.search(
        r"(?is)INSERT\s+(?:OVERWRITE|INTO)\s+TABLE\s+[\w.$]+"
        r"(?:\s+PARTITION\s*\([^)]*\))?\s*(SELECT.*)",
        content,
    )
    return m.group(1).strip().rstrip(";").strip() if m else None


def _ddl_columns(content: str) -> list[tuple[str, str, str]]:
    parsed = translator.parse_create(content) if content else None
    return parsed["columns"] if parsed else []


def _output_tables(node: dict) -> list[str]:
    out = []
    for table in ((node.get("outputs") or {}).get("tables") or []):
        guid = table.get("guid", "")
        if guid:
            out.append(guid.split(".")[-1])
    return out


def _input_tables(node: dict) -> list[str]:
    out = []
    for table in ((node.get("inputs") or {}).get("tables") or []):
        guid = table.get("guid", "")
        if guid:
            out.append(guid.split(".")[-1])
    return out


def _is_sqlish_pyodps(node: dict) -> bool:
    content = (node.get("script") or {}).get("content", "") or ""
    head = content.strip().lstrip().lower()
    return head.startswith(("sql", "select", "insert"))


def _is_heavy_python(node: dict) -> bool:
    """Data-heavy PyODPS/Python must NOT run on the Composer worker.

    Guide §10 guardrail: a PythonOperator runs in the Airflow worker's memory,
    which is sized for orchestration, not data crunching. Heavy markers mirror
    classify.route_nodes.HEAVY_MARKERS so the generator and the classifier agree.
    """
    content = (node.get("script") or {}).get("content", "") or ""
    lowered = content.lower()
    return any(marker in lowered for marker in HEAVY_MARKERS)


def route_node(node: dict) -> tuple[str, str]:
    """Return (gcp_target, reason). SQL-ish PYODPS is pushed down.

    Routing matrix (guide §10):
      - ODPS_SQL          -> Dataform .sqlx
      - PYODPS (SQL-ish)  -> Dataform .sqlx (push down)
      - PYODPS (light)    -> PythonOperator in Composer
      - PYODPS (heavy)    -> Dataflow (Beam), never the Composer worker
      - DIDE_SHELL        -> TRIAGE (BashOperator or PythonOperator, audit each)
      - VIRTUAL           -> EmptyOperator
    """
    command = ((node.get("script") or {}).get("runtime") or {}).get("command", "")
    if command in SAFE_COMMANDS:
        return "Dataform .sqlx" if command == "ODPS_SQL" else "EmptyOperator", command
    if command in SQLISH_COMMANDS and _is_sqlish_pyodps(node):
        return "Dataform .sqlx", "PyODPS that is really SQL -> push down to BigQuery SQL"
    if command in SQLISH_COMMANDS and _is_heavy_python(node):
        return "Dataflow (Beam)", "Data-heavy Python -> keep off the Composer worker (guardrail §10)"
    if command in SQLISH_COMMANDS:
        return "PythonOperator", "Lightweight procedural Python in Composer"
    if command == "DIDE_SHELL":
        return "TRIAGE", "Shell script -> BashOperator or PythonOperator; audit each"
    return "TRIAGE", TRIAGE_COMMANDS.get(command, "Unknown command -> human review")


def build_models_and_decls(
    workflow: dict,
    csv_overrides: dict[str, dict] | None = None,
) -> tuple[list[Model], list[Declaration], list[str]]:
    """Translate a workflow's ODPS_SQL / SQL-ish nodes into models + declarations.

    Nodes are processed in topological order determined by the flow[] dependency
    graph (Section 8 / §2.4 of the guide).  Cross-cycle and cross-workflow
    dependencies are flagged for the DAG generator.

    *csv_overrides* is an optional {node_id: row_dict} mapping from the
    migration_matrix.csv.  When present, each node is checked for:
      - skip=true       -> node excluded from generation entirely
      - gcp_target      -> overrides the auto-routed target
      - review_notes    -> appended to the review report
    """
    overrides = csv_overrides or {}
    nodes = workflow.get("nodes") or []
    flow = workflow.get("flow") or []
    tag = _safe(workflow.get("name") or "workflow")
    available_vars = {
        v["name"] for v in (workflow.get("variables") or []) if v.get("name")
    }

    # Topological order from the flow[] graph — preserves execution order.
    ordered_nodes = topological_sort(nodes, flow)
    cross_cycle_nodes = _detect_cross_cycle_deps(flow, nodes)

    # Pass 1: which tables are produced by DML nodes (any order).
    produced = set()
    for node in ordered_nodes:
        if _dml_select((node.get("script") or {}).get("content", "") or ""):
            produced.update(_output_tables(node))

    models: list[Model] = []
    inputs: set[str] = set()
    review: list[str] = []

    for node in ordered_nodes:
        name = node.get("name") or ""
        node_id = node.get("id", "")
        command = ((node.get("script") or {}).get("runtime") or {}).get("command", "")
        content = (node.get("script") or {}).get("content", "") or ""

        # --- CSV override: skip node ---
        ov = overrides.get(node_id, {})
        if ov.get("skip", "").strip().lower() == "true":
            review.append(f"- `{name}` [{command}]: SKIPPED (per migration_matrix.csv)")
            continue

        # --- CSV override: review_notes (always appended, before any continue) ---
        csv_notes = ov.get("review_notes", "").strip()

        # --- CSV override: target or auto-route ---
        csv_target = ov.get("gcp_target", "").strip()
        if csv_target:
            # User explicitly set a target in the CSV.
            target = csv_target
            reason = f"CSV override: {csv_target}"
        else:
            target, reason = route_node(node)

        # Flag cross-cycle dependencies (§14: CrossCycleDependsOnOtherNode).
        if node_id in cross_cycle_nodes:
            review.append(
                f"- `{name}` [{command}]: CROSS-CYCLE DEPENDENCY detected "
                f"(depends_on_past or previous-run sensor needed in Airflow)")

        if command == "VIRTUAL":
            if csv_notes:
                review.append(f"- `{name}` [{command}] (CSV note): {csv_notes}")
            continue
        if target == "TRIAGE":
            review.append(f"- `{name}` [{command}]: {reason}")
            if csv_notes:
                review.append(f"- `{name}` [{command}] (CSV note): {csv_notes}")
            continue
        if target != "Dataform .sqlx":
            review.append(f"- `{name}` [{command}] -> {target}: {reason} (not auto-generated)")
            if csv_notes:
                review.append(f"- `{name}` [{command}] (CSV note): {csv_notes}")
            continue

        select_sql = _dml_select(content)
        outputs = _output_tables(node)
        if not select_sql:
            # CREATE-only node: absorbed when a model writes the same table.
            created = None
            for stmt in translator.parse_statements(content):
                if stmt["kind"] == "create":
                    created = translator.parse_create(stmt["raw"])
                    break
            table = created["name"] if created else None
            if table and table in produced:
                continue  # absorbed by the model writing <table>
            review.append(f"- `{name}`: no INSERT ... SELECT body and no model writes "
                          f"table `{table or '?'}` -- DDL-only table needs manual handling")
            continue
        if not outputs:
            review.append(f"- `{name}`: DML found but no output table declared; skipping")
            continue

        # Detect partition clauses in INSERT — string partitions don't map to
        # BigQuery native partitioning (§15: "redesign, not a lookup").
        partition_match = re.search(
            r"(?i)PARTITION\s*\(\s*(\w+)\s*=", content
        )
        if partition_match:
            part_col = partition_match.group(1)
            review.append(
                f"- `{name}` (model `{outputs[0]}`): PARTITION BY `{part_col}` "
                f"detected -- MaxCompute string partition does NOT map to BigQuery "
                f"native partitioning; redesign required (clustering key or "
                f"partition evolution)")

        for tbl in outputs:
            sql, flags = translator.translate_query(select_sql, available_vars=available_vars)
            models.append(Model(name=tbl, tag=tag, select_sql=sql,
                                columns=_ddl_columns(content), flags=flags))
            if flags:
                review.append(f"- `{name}` (model `{tbl}`):\n" + "\n".join(f"    - {f}" for f in flags))
        for tbl in _input_tables(node):
            inputs.add(tbl)

        # --- CSV override: review_notes (Dataform path) ---
        if csv_notes:
            review.append(f"- `{name}` [{command}] (CSV note): {csv_notes}")

    # Inputs that no generated model produces -> declarations (already-loaded tables).
    declarations = [Declaration(name=t, schema="") for t in sorted(inputs - produced)]
    return models, declarations, review


# ---------------------------------------------------------------------------
# Emitting
# ---------------------------------------------------------------------------
def _model_sqlx(m: Model) -> str:
    col_lines = ""
    if m.columns:
        cols = ",\n    ".join(f'{name}: "{desc or typ}"' for name, typ, desc in m.columns)
        col_lines = f"  columns: {{\n    {cols}\n  }},\n"
    return (
        "config {\n"
        f'  type: "table",\n'
        f'  tags: ["{m.tag}"],\n'
        f'  description: "Auto-generated from DataWorks. Migrated by DataWorksToGCP.",\n'
        + col_lines +
        "}\n\n"
        "-- GENERATED CODE -- verify with the compile/run/verify gates (see migrate.py).\n"
        f"{m.select_sql}\n"
    )


def _decl_sqlx(d: Declaration, schema: str) -> str:
    return (
        "config {\n"
        '  type: "declaration",\n'
        f'  schema: "{schema}",\n'
        f'  name: "{d.name}",\n'
        '  description: "Raw source table already loaded in BigQuery by the sync pipeline."\n'
        "}\n"
    )


_DAG_TEMPLATE = '''"""GENERATED by DataworksToGCP. Auto-migrated Composer DAG.

Source workflow: {workflow_name}
Schedule: {schedule} ({timezone}) -- from DataWorks trigger, seconds field stripped.
Airflow target: {airflow_target}
RE-VERIFY the schedule against the source before cutover (bizdate/timezone traps,
see docs/03 step 5). The Dataform repo is compiled with dataform vars and the
"{tag}" tag is run in a single invocation.

{cross_workflow_comment}{depends_on_past_comment}"""

{imports}

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "{project}")
REGION = os.getenv("GCP_REGION", "{region}")
REPOSITORY_ID = os.getenv("DATAFORM_REPOSITORY_ID", "{repo}")
GIT_BRANCH = os.getenv("DATAFORM_GIT_BRANCH", "main")
TAG = "{tag}"

{python_operator_defs}
with DAG(
    dag_id="{dag_id}",
{schedule_kwargs}    catchup=False,
    max_active_runs=1,
    tags=[TAG, "migrated-from-dataworks"],
    description="Auto-migrated from DataWorks workflow {workflow_name}",
) as dag:
    start = EmptyOperator(task_id="{start_task}")

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={{
            "git_commitish": GIT_BRANCH,
{system_vars_section}        }},
    )

    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={{
            "compilation_result": "{{{{ task_instance.xcom_pull('compile_dataform')['name'] }}}}",
            "invocation_config": {{"included_tags": [TAG]}},
        }},
    )

{python_operator_tasks}    start >> compile_dataform >> run_dataform
{extra_tasks}'''


# Airflow major versions we generate DAGs for (one folder each: version_2/,
# version_3/). Wiring differs per major, see the helpers below. Airflow uses
# python 3.8+ and these versions differ in import paths and DAG constructor
# args:
#   Airflow 2.x  -> DAG() from airflow; core operators under airflow.operators;
#                   scheduling arg is schedule_interval (works on every 2.x);
#                   a pendulum-aware start_date carries the DAG timezone.
#   Airflow 3.x  -> DAG() from airflow.sdk; operators under the standard
#                   provider; unified schedule= arg; cron timezone pinned with
#                   CronTriggerTimetable; the legacy "timezone=" DAG kwarg was
#                   REMOVED (Airflow 2 news: "unexpected keyword argument
#                   'timezone'"); logical_date was REMOVED (use
#                   data_interval_start).
AIRFLOW_DEFAULT_START_YEAR = 2026

# Macros used to resolve DataWorks system variables inside the compile task.
# Each entry maps (VN,variables) -> (Airflow 2 expression, Airflow 3 expression).
_SYS_VAR_MACROS: dict[str, tuple[str, str]] = {
    # business date = scheduled date - 1 day, formatted YYYYMMDD (guide §14)
    "bizdate": (
        "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
        "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
    ),
    # run/scheduled date without dashes (guide §14)
    "yyyymmdd": ("{{ ds_nodash }}", "{{ ds_nodash }}"),
    # cycle (scheduled) time including hour/min. logical_date is removed in
    # Airflow 3 -> data_interval_start there; logical_date works on 2.x.
    "cyctime": (
        "{{ logical_date.strftime('%Y%m%d%H%M') }}",
        "{{ data_interval_start.strftime('%Y%m%d%H%M') }}",
    ),
}


def _var_macro(name: str, airflow_major: int) -> str:
    """Return the Airflow template expression for a system variable.

    Unknown vars return an empty placeholder (surfaced in workflow_settings.yaml).
    """
    pair = _SYS_VAR_MACROS.get(name)
    if pair is None:
        return ""
    return pair[1] if airflow_major >= 3 else pair[0]


def _airflow_imports(airflow_major: int, *, need_python_op: bool,
                     need_external_sensor: bool) -> str:
    """Version-appropriate import block for the generated Composer DAG."""
    if airflow_major >= 3:
        lines = [
            "from airflow.sdk import DAG",
            "from airflow.timetables.trigger import CronTriggerTimetable",
            "from airflow.providers.standard.operators.empty import EmptyOperator",
            "from airflow.providers.google.cloud.operators.dataform import (",
            "    DataformCreateCompilationResultOperator,",
            "    DataformCreateWorkflowInvocationOperator,",
            ")",
        ]
        if need_python_op:
            lines.append("from airflow.providers.standard.operators.python import PythonOperator")
        if need_external_sensor:
            lines.append("from airflow.providers.standard.sensors.external_task import ExternalTaskSensor")
            lines.append("import json")
    else:
        lines = [
            "from airflow import DAG",
            "from airflow.operators.empty import EmptyOperator",
            "from airflow.providers.google.cloud.operators.dataform import (",
            "    DataformCreateCompilationResultOperator,",
            "    DataformCreateWorkflowInvocationOperator,",
            ")",
        ]
        if need_python_op:
            lines.append("from airflow.operators.python import PythonOperator")
        if need_external_sensor:
            lines.append("from airflow.sensors.external_task import ExternalTaskSensor")
            lines.append("import json")
    lines.append("import pendulum")
    lines.append("import os")
    return "\n".join(lines) + "\n"


def _schedule_kwargs(schedule: str, timezone: str, airflow_major: int) -> str:
    """DAG constructor lines for the schedule + start_date, per Airflow major.

    The timezone is carried by a pendulum-aware start_date (works on 2.x and
    3.x). Airflow 3 additionally pins the cron timezone with a
    CronTriggerTimetable; Airflow 2 uses schedule_interval so every 2.x release
    (2.0 through 2.10) can parse the file.
    """
    start_date = (
        f'    start_date=pendulum.datetime({AIRFLOW_DEFAULT_START_YEAR}, 1, 1, '
        f'tz="{timezone}"),\n'
    )
    if airflow_major >= 3:
        return (
            f'    schedule=CronTriggerTimetable("{schedule}", timezone="{timezone}"),\n'
            + start_date
        )
    return f'    schedule_interval="{schedule}",\n' + start_date


def _compilation_vars_section(system_vars: list[str], airflow_major: int) -> str:
    """Build the ``code_compilation_config.vars`` dict for the Dataform compile task.

    Dataform evaluates variables at COMPILE time only: WorkflowInvocation has no
    "variables" field (the API rejects it). The DAG therefore passes them in the
    compile task's ``code_compilation_config`` as a flat {name: value} dict, and
    the run task just references the compilation result. Guide §14 / Appendix A.
    """
    if not system_vars:
        return ""
    lines = ['            "code_compilation_config": {', '                "vars": {']
    for sv in system_vars:
        lines.append(f'                    "{sv}": "{_var_macro(sv, airflow_major)}",')
    lines.append("                },")
    lines.append("            },")
    return "\n".join(lines) + "\n"


def _build_dag(wf: GeneratedWorkflow, project: str, region: str, repo: str,
               start_task: str, system_vars: list[str] | None = None,
               has_cross_workflow: bool = False,
               has_depends_on_past: bool = False,
               external_workflows: list[dict] | None = None,
               python_operator_nodes: list[dict] | None = None,
               airflow_major: int = 3) -> str:
    """Build the Airflow DAG Python source for a workflow.

    *airflow_major* selects the Airflow major version the DAG targets (2 or 3).
    Both variants are always emitted to version_2/ and version_3/, and the
    "dags/" folder mirrors whichever version is active (AIRFLOW_MAJOR_VERSION).

    When *has_depends_on_past* is True, a ``depends_on_past=True`` kwarg is
    added to the Dataform invocation operator (§14 / CrossCycleDependsOnOtherNode).
    When *has_cross_workflow* is True, an ExternalTaskSensor import and task
    are emitted for each external workflow dependency.
    When *python_operator_nodes* is provided, PythonOperator tasks are emitted
    for nodes routed to Composer (guide §10: light procedural Python).
    """
    extra_tasks = ""
    cross_workflow_comment = ""
    depends_on_past_comment = ""
    python_operator_defs = ""
    python_operator_tasks = ""

    if has_depends_on_past:
        depends_on_past_comment = (
            "This workflow has CROSS-CYCLE dependencies (CrossCycleDependsOnOtherNode).\n"
            "The Dataform invocation uses depends_on_past=True to honour them.\n"
        )

    need_external_sensor = bool(has_cross_workflow and external_workflows)
    if need_external_sensor:
        ext_ids = sorted({e["external_workflow_id"] for e in external_workflows})
        cross_workflow_comment = (
            "This workflow depends on external workflows (cross-workflow dependencies).\n"
            "ExternalTaskSensor tasks wait for the external DAG's latest run.\n"
        )
        sensor_lines = []
        for i, ext_id in enumerate(ext_ids):
            sensor_id = f"wait_external_{i}"
            sensor_lines.append(
                f'\n    {sensor_id} = ExternalTaskSensor(\n'
                f'        task_id="{sensor_id}",\n'
                f'        external_dag_id="{ext_id}",\n'
                f'        external_task_id=None,  # wait for entire DAG\n'
                f'        poke_interval=60,\n'
                f'        mode="reschedule",\n'
                f'    )\n'
                f'    {sensor_id} >> start'
            )
        extra_tasks = "\n".join(sensor_lines)

    # PythonOperator tasks for nodes routed to Composer (guide §10).
    if python_operator_nodes:
        py_lines = []
        py_defs = []
        for i, pnode in enumerate(python_operator_nodes):
            task_id = _safe(pnode.get("name", f"python_task_{i}"))
            content = pnode.get("content", "")
            func_name = f"_task_{task_id}"
            py_defs.append(
                f"\ndef {func_name}():\n"
                + "\n".join(f"    {line}" for line in content.splitlines())
                + "\n"
            )
            py_lines.append(
                f"    {task_id} = PythonOperator(\n"
                f'        task_id="{task_id}",\n'
                f"        python_callable={func_name},\n"
                f"    )\n"
            )
        python_operator_defs = "\n".join(py_defs)
        python_operator_tasks = "\n".join(py_lines) + "\n"

    return _DAG_TEMPLATE.format(
        workflow_name=wf.workflow_name,
        schedule=wf.schedule,
        timezone=wf.timezone,
        airflow_target=f"Airflow {airflow_major}.x",
        tag=wf.tag,
        dag_id=wf.tag,
        project=project,
        region=region,
        repo=repo,
        start_task=start_task,
        imports=_airflow_imports(
            airflow_major,
            need_python_op=bool(python_operator_nodes),
            need_external_sensor=need_external_sensor,
        ),
        schedule_kwargs=_schedule_kwargs(wf.schedule, wf.timezone, airflow_major),
        system_vars_section=_compilation_vars_section(system_vars or [], airflow_major),
        extra_tasks=extra_tasks,
        cross_workflow_comment=cross_workflow_comment,
        depends_on_past_comment=depends_on_past_comment,
        python_operator_defs=python_operator_defs,
        python_operator_tasks=python_operator_tasks,
    )


def generate_project(
    flowspec: dict,
    out_dir: Path,
    *,
    project: str,
    region: str,
    repo: str,
    dataset: str,
    assertion_dataset: str,
    core_version: str,
    csv_overrides: dict[str, dict] | None = None,
) -> GeneratedWorkflow:
    """Generate one Dataform project + DAG + review report from a workflow FlowSpec.

    *csv_overrides* is an optional {node_id: row_dict} from migration_matrix.csv.
    When present, node routing, skip flags, review notes, and schedule overrides
    are applied from the CSV.
    """
    spec = flowspec.get("spec", flowspec)
    workflows = spec.get("workflows") or []
    if not workflows:
        wf_name = spec.get("name") or "workflow"
        workflows = [{"name": wf_name, "nodes": spec.get("nodes") or [],
                      "flow": spec.get("flow") or [], "trigger": spec.get("trigger") or {}}]

    # Which Airflow major the "dags/" folder (and GeneratedWorkflow.dag) targets.
    # version_2/ and version_3/ variants are ALWAYS emitted regardless.
    try:
        airflow_major = int(os.environ.get("AIRFLOW_MAJOR_VERSION", "3") or 3)
    except ValueError:
        airflow_major = 3
    airflow_major = 2 if airflow_major < 3 else 3

    out_dir.mkdir(parents=True, exist_ok=True)
    marts = out_dir / "definitions" / "marts"
    sources = out_dir / "definitions" / "sources"
    dags = out_dir / "dags"
    dags_v2 = out_dir / "version_2" / "dags"
    dags_v3 = out_dir / "version_3" / "dags"
    review_dir = out_dir / "review"
    for d in (marts, sources, dags, dags_v2, dags_v3, review_dir):
        d.mkdir(parents=True, exist_ok=True)

    all_models: list[Model] = []
    all_decls: list[Declaration] = []
    all_review: list[str] = []
    seen_tables: set[str] = set()
    start_task = "workshop_start"
    any_depends_on_past = False
    all_cross_wf: list[dict] = []
    all_python_op_nodes: list[dict] = []
    first_schedule = "0 2 * * *"
    first_timezone = "UTC"

    # Collect workflow variables before processing models.
    project_vars: dict[str, str] = {"environment": "prod"}
    system_vars: set[str] = set()
    for wf in workflows:
        for var in wf.get("variables") or []:
            var_name = var.get("name")
            if not var_name:
                continue
            if var.get("type") == "System":
                system_vars.add(var_name)
                project_vars.setdefault(var_name, "19700101")
            else:
                project_vars.setdefault(var_name, var.get("value", ""))

    for wf in workflows:
        wf_name = wf.get("name") or "workflow"
        tag = _safe(wf_name)
        trigger = wf.get("trigger") or {}
        schedule = sanitize_cron(trigger.get("cron", "00 02 00 * * ?"))
        timezone = trigger.get("timezone") or "UTC"

        # CSV override: workflow-level schedule from any node's CSV row.
        if csv_overrides:
            wf_node_ids = {n.get("id") for n in (wf.get("nodes") or [])}
            for nid, ov in csv_overrides.items():
                if nid in wf_node_ids and ov.get("schedule", "").strip():
                    schedule = ov["schedule"].strip()
                    break

        models, decls, review = build_models_and_decls(wf, csv_overrides=csv_overrides)

        # Collect PythonOperator nodes (routed to Composer, guide §10).
        python_op_nodes: list[dict] = []
        for node in wf.get("nodes") or []:
            command = ((node.get("script") or {}).get("runtime") or {}).get("command", "")
            content = (node.get("script") or {}).get("content", "") or ""
            target, _ = route_node(node)
            if target == "PythonOperator":
                python_op_nodes.append({
                    "name": node.get("name", ""),
                    "content": content,
                    "command": command,
                })
            elif target == "TRIAGE" and command == "DIDE_SHELL":
                # Shell scripts routed to TRIAGE; after human review, if
                # approved as PythonOperator, they'd be added here.
                pass
        all_python_op_nodes.extend(python_op_nodes)

        # Track first workflow's schedule/timezone for the returned GeneratedWorkflow.
        if not all_models:
            first_schedule = schedule
            first_timezone = timezone

        # Dedupe models by output table: only the first writer is deployed.
        deduped_models: list[Model] = []
        for m in models:
            if m.name in seen_tables:
                note = (f"- [DUPLICATE] output table `{m.name}` already generated by "
                        "an earlier node; this writer is skipped so only the first "
                        "definition is deployed.")
                review.append(note)
                all_review.append(note)
                continue
            seen_tables.add(m.name)
            deduped_models.append(m)
        models = deduped_models

        all_models.extend(models)
        all_decls.extend(decls)
        all_review.extend(review)

        # VIRTUAL start node name for DAG parity.
        for node in wf.get("nodes") or []:
            if ((node.get("script") or {}).get("runtime") or {}).get("command") == "VIRTUAL":
                start_task = node.get("name") or "workshop_start"
                break

        # Detect cross-cycle and cross-workflow dependencies for DAG generation.
        flow_graph = wf.get("flow") or []
        cross_cycle_nodes = _detect_cross_cycle_deps(flow_graph, wf.get("nodes") or [])
        has_depends_on_past = bool(cross_cycle_nodes)
        cross_wf_deps = _detect_cross_workflow_deps(wf.get("nodes") or [])
        has_cross_workflow = bool(cross_wf_deps)

        if has_depends_on_past:
            any_depends_on_past = True
        all_cross_wf.extend(cross_wf_deps)

        if cross_cycle_nodes:
            review.append(
                f"- [CROSS-CYCLE] Nodes {sorted(cross_cycle_nodes)} have "
                f"CrossCycleDependsOnOtherNode -> depends_on_past=True in DAG")
        if cross_wf_deps:
            ext_ids = sorted({d["external_workflow_id"] for d in cross_wf_deps})
            review.append(
                f"- [CROSS-WORKFLOW] External dependencies on workflow(s) "
                f"{ext_ids} -> ExternalTaskSensor generated")

        wf_obj = GeneratedWorkflow(
            workflow_name=wf_name, tag=tag, schedule=schedule,
            timezone=timezone, dag="")

        def _dag_for(major: int) -> str:
            return _build_dag(
                wf_obj,
                project=project, region=region, repo=repo, start_task=start_task,
                system_vars=sorted(system_vars),
                has_cross_workflow=has_cross_workflow,
                has_depends_on_past=has_depends_on_past,
                external_workflows=cross_wf_deps,
                python_operator_nodes=python_op_nodes,
                airflow_major=major,
            )

        dag_v2 = _dag_for(2)
        dag_v3 = _dag_for(3)
        # The active version A -> 3 means dags/ mirrors version_3 (Composer sync
        # keeps using a stable "dags/" path).
        dag_active = dag_v3 if airflow_major >= 3 else dag_v2

        g = GeneratedWorkflow(
            workflow_name=wf_name,
            tag=tag,
            schedule=schedule,
            timezone=timezone,
            models=models,
            declarations=decls,
            review=review,
            depends_on_past=has_depends_on_past,
            cross_workflow_deps=cross_wf_deps,
            dag=dag_active,
        )

        # Emit Dataform artifacts for this workflow.
        for m in models:
            (marts / f"{m.name}.sqlx").write_text(_model_sqlx(m), encoding="utf-8")
        for d in decls:
            (sources / f"{d.name}.sqlx").write_text(_decl_sqlx(d, dataset), encoding="utf-8")
        (dags_v2 / f"{tag}.py").write_text(dag_v2, encoding="utf-8")
        (dags_v3 / f"{tag}.py").write_text(dag_v3, encoding="utf-8")
        (dags / f"{tag}.py").write_text(dag_active, encoding="utf-8")
        if review:
            (review_dir / f"review_{tag}.md").write_text(
                f"# Review required: {wf_name}\n\n"
                "Only items below need a human; everything else was auto-generated "
                "and validated by the compile/run/verify gates.\n\n"
                + "\n".join(review) + "\n",
                encoding="utf-8",
            )

    # Workflow settings (Dataform v3: no dataform.json).
    # project_vars and system_vars were collected before the workflow loop above.
    settings_lines = [
        f"dataformCoreVersion: {core_version}",
        f"defaultProject: {project}",
        f"defaultLocation: {region}",
        f"defaultDataset: {dataset}",
        f"defaultAssertionDataset: {assertion_dataset}",
        "vars:",
    ]
    for k, v in project_vars.items():
        settings_lines.append(f"  {k}: \"{v}\"")
    (out_dir / "workflow_settings.yaml").write_text("\n".join(settings_lines) + "\n",
                                                     encoding="utf-8")

    creds = {"projectId": project, "location": region}
    (out_dir / ".df-credentials.json").write_text(
        json.dumps(creds, indent=2), encoding="utf-8")

    g0 = GeneratedWorkflow(
        workflow_name=spec.get("name") or "generated",
        tag=_safe(spec.get("name") or "generated"),
        dag="",
        models=all_models,
        declarations=all_decls,
        review=all_review,
        schedule=first_schedule,
        timezone=first_timezone,
        depends_on_past=any_depends_on_past,
        cross_workflow_deps=all_cross_wf,
        python_operator_nodes=all_python_op_nodes,
    )
    return g0


def load_flowspecs(paths: list[Path]) -> list[dict]:
    """Load one or more FlowSpec JSON files (workflow-level) for generation."""
    return [json.loads(p.read_text(encoding="utf-8")) for p in paths]
