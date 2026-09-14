"""Unit + integration tests for the DataWorks -> GCP migration toolkit.

Covers every gap fixed after the v3.1 guide audit:
  1. flow[] topological sort
  2. Cross-cycle dependency detection
  3. Cross-workflow dependency detection
  4. Partition clause flagging
  5. Verifier: checksums, aggregates, per-DAG parity
  6. _decl_sqlx schema bug fix
  7. Duplicate docstring fix
  8. End-to-end generation with both sample FlowSpecs
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from classify.flowspec_to_csv import (
    CSV_FIELDS,
    flowspec_to_rows,
    load_csv_overrides,
    write_csv,
)
from migrate import generator, translator, verifier
from migrate.generator import (
    _build_adjacency,
    _detect_cross_cycle_deps,
    _detect_cross_workflow_deps,
    topological_sort,
)
from migrate.verifier import (
    DAGParityCheck,
    TableCheck,
    check_dag_parity,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "extract"


def _load(name: str) -> dict:
    return json.loads((SAMPLE_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture
def house_buying_spec() -> dict:
    return _load("sample_flowspec_house_buying.json")


@pytest.fixture
def ecommerce_spec() -> dict:
    return _load("sample_flowspec_complex_ecommerce.json")


# ===================================================================
# 1. Topological sort (flow[] dependency ordering)
# ===================================================================

class TestTopologicalSort:
    """Tests for topological_sort()."""

    def test_linear_chain(self, house_buying_spec):
        wf = house_buying_spec["spec"]["workflows"][0]
        ordered = topological_sort(wf["nodes"], wf["flow"])
        names = [n.get("name") for n in ordered]
        assert names == ["workshop_start", "ddl_result_table", "insert_result_table"]

    def test_parallel_fan_out(self, ecommerce_spec):
        wf = ecommerce_spec["spec"]["workflows"][0]
        ordered = topological_sort(wf["nodes"], wf["flow"])
        names = [n.get("name") for n in ordered]
        assert names[0] == "pipeline_start"
        assert names[-1] == "pipeline_end"
        # sync_raw_orders and sync_raw_users must both appear before dwd_order_facts
        idx_orders = names.index("sync_raw_orders")
        idx_users = names.index("sync_raw_users")
        idx_facts = names.index("dwd_order_facts")
        assert idx_orders < idx_facts
        assert idx_users < idx_facts

    def test_empty_flow_falls_back_to_input_order(self):
        nodes = [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]
        ordered = topological_sort(nodes, [])
        assert [n["name"] for n in ordered] == ["A", "B"]

    def test_cycle_falls_back_to_input_order(self):
        nodes = [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]
        flow = [
            {"nodeId": "a", "depends": [{"nodeId": "b"}]},
            {"nodeId": "b", "depends": [{"nodeId": "a"}]},
        ]
        ordered = topological_sort(nodes, flow)
        # Both nodes still present despite cycle
        assert len(ordered) == 2

    def test_build_adjacency(self):
        flow = [
            {"nodeId": "a", "depends": []},
            {"nodeId": "b", "depends": [{"nodeId": "a"}]},
            {"nodeId": "c", "depends": [{"nodeId": "a"}, {"nodeId": "b"}]},
        ]
        adj = _build_adjacency(flow)
        assert adj["a"] == []
        assert adj["b"] == ["a"]
        assert adj["c"] == ["a", "b"]


# ===================================================================
# 2. Cross-cycle dependency detection
# ===================================================================

class TestCrossCycleDeps:
    """Tests for _detect_cross_cycle_deps()."""

    def test_no_cross_cycle(self, house_buying_spec):
        wf = house_buying_spec["spec"]["workflows"][0]
        result = _detect_cross_cycle_deps(wf.get("flow", []), wf["nodes"])
        assert result == set()

    def test_detects_cross_cycle_flag(self):
        nodes = [
            {"id": "a", "name": "A"},
            {"id": "b", "name": "B", "crossCycleDependsOnOtherNode": True},
        ]
        flow = [
            {"nodeId": "a", "depends": []},
            {"nodeId": "b", "depends": [{"nodeId": "a"}]},
        ]
        result = _detect_cross_cycle_deps(flow, nodes)
        assert "b" in result

    def test_detects_cross_cycle_in_flow_dep(self):
        nodes = [
            {"id": "a", "name": "A"},
            {"id": "b", "name": "B"},
        ]
        flow = [
            {"nodeId": "a", "depends": []},
            {"nodeId": "b", "depends": [{"nodeId": "a", "type": "CrossCycle"}]},
        ]
        result = _detect_cross_cycle_deps(flow, nodes)
        assert "b" in result


# ===================================================================
# 3. Cross-workflow dependency detection
# ===================================================================

class TestCrossWorkflowDeps:
    """Tests for _detect_cross_workflow_deps()."""

    def test_no_cross_workflow(self, house_buying_spec):
        wf = house_buying_spec["spec"]["workflows"][0]
        result = _detect_cross_workflow_deps(wf["nodes"])
        assert result == []

    def test_detects_external_dep(self):
        nodes = [
            {"id": "a", "name": "A", "inputs": {
                "nodeOutputs": [{"data": "x", "externalWorkflowId": "ext_wf_123"}]
            }},
        ]
        result = _detect_cross_workflow_deps(nodes)
        assert len(result) == 1
        assert result[0]["external_workflow_id"] == "ext_wf_123"


# ===================================================================
# 4. Partition clause flagging
# ===================================================================

class TestPartitionFlagging:
    """Verify partition clauses are detected and flagged in review."""

    def test_partition_flagged_in_review(self, ecommerce_spec):
        wf = ecommerce_spec["spec"]["workflows"][0]
        _models, _decls, review = generator.build_models_and_decls(wf)
        partition_flags = [r for r in review if "PARTITION BY" in r]
        # ods_raw_orders, ods_raw_users, dwd_order_facts, dws_agg_daily all have pt=
        assert len(partition_flags) >= 4
        for flag in partition_flags:
            assert "MaxCompute string partition does NOT map" in flag

    def test_no_partition_in_simple_spec(self, house_buying_spec):
        wf = house_buying_spec["spec"]["workflows"][0]
        _models, _decls, review = generator.build_models_and_decls(wf)
        partition_flags = [r for r in review if "PARTITION BY" in r]
        assert len(partition_flags) == 0


# ===================================================================
# 5. Verifier: TableCheck + DAGParityCheck
# ===================================================================

class TestVerifier:
    """Tests for verifier dataclasses and parity functions (no BigQuery needed)."""

    def test_table_check_fields(self):
        tc = TableCheck(
            name="test_table", row_count=100,
            checksum="abc123", aggregates={"sum_x": 500},
            ok=True,
        )
        assert tc.name == "test_table"
        assert tc.row_count == 100
        assert tc.checksum == "abc123"
        assert tc.aggregates == {"sum_x": 500}
        assert tc.ok is True

    def test_table_check_failure(self):
        tc = TableCheck(name="bad", row_count=0, ok=False, detail="empty table")
        assert tc.ok is False
        assert tc.detail == "empty table"

    def test_dag_parity_pass(self):
        check = check_dag_parity(
            workflow_name="test_wf",
            source_schedule="0 2 * * *",
            generated_schedule="0 2 * * *",
            source_node_count=3,
            generated_model_count=2,
            source_outputs=["table_a", "table_b"],
            generated_outputs=["table_a", "table_b"],
        )
        assert check.ok is True
        assert check.detail == "all checks passed"

    def test_dag_parity_schedule_mismatch(self):
        check = check_dag_parity(
            workflow_name="test_wf",
            source_schedule="0 3 * * *",
            generated_schedule="0 2 * * *",
            source_node_count=3,
            generated_model_count=2,
            source_outputs=["table_a"],
            generated_outputs=["table_a"],
        )
        assert check.ok is False
        assert "Schedule mismatch" in check.detail

    def test_dag_parity_output_mismatch(self):
        check = check_dag_parity(
            workflow_name="test_wf",
            source_schedule="0 2 * * *",
            generated_schedule="0 2 * * *",
            source_node_count=3,
            generated_model_count=2,
            source_outputs=["table_a", "table_b", "table_c"],
            generated_outputs=["table_a", "table_b"],
        )
        assert check.ok is False
        assert "Missing output tables" in check.detail

    def test_summarize_produces_markdown(self):
        checks = [
            TableCheck(name="t1", row_count=10, checksum="abc", ok=True),
            TableCheck(name="t2", row_count=0, ok=False, detail="empty"),
        ]
        passed, md = verifier.summarize(checks)
        assert passed is False
        assert "| `t1` |" in md
        assert "| `t2` |" in md
        assert "FAIL" in md

    def test_summarize_dag_parity(self):
        checks = [
            DAGParityCheck(
                workflow_name="wf1", source_schedule="0 2 * * *",
                generated_schedule="0 2 * * *",
                source_node_count=3, generated_model_count=2,
                source_outputs=["a", "b"], generated_outputs=["a", "b"],
                ok=True,
            ),
        ]
        passed, md = verifier.summarize_dag_parity(checks)
        assert passed is True
        assert "| `wf1` |" in md


# ===================================================================
# 6. _decl_sqlx schema bug fix
# ===================================================================

class TestDeclSqlxBug:
    """Verify _decl_sqlx uses schema correctly, not 'd.name and schema'."""

    def test_schema_value(self):
        d = generator.Declaration(name="my_table", schema="")
        result = generator._decl_sqlx(d, "my_dataset")
        assert 'schema: "my_dataset"' in result
        assert 'name: "my_table"' in result

    def test_empty_schema(self):
        d = generator.Declaration(name="my_table", schema="")
        result = generator._decl_sqlx(d, "")
        assert 'schema: ""' in result


# ===================================================================
# 7. Duplicate docstring fix
# ===================================================================

class TestTranslatorDocstring:
    """Verify translate_query has one docstring, not two."""

    def test_single_docstring(self):
        import inspect
        source = inspect.getsource(translator.translate_query)
        docstring_count = source.count('"""Translate')
        assert docstring_count == 1, f"Expected 1 docstring, found {docstring_count}"


# ===================================================================
# 8. End-to-end generation
# ===================================================================

class TestEndToEndGeneration:
    """Full generation from FlowSpec -> artifacts, no GCP needed."""

    def _generate(self, spec_name: str) -> generator.GeneratedWorkflow:
        spec = _load(spec_name)
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
            )
            # Verify output files exist
            assert (out / "workflow_settings.yaml").exists()
            assert (out / ".df-credentials.json").exists()
            assert (out / "dags").is_dir()
            assert (out / "version_2" / "dags").is_dir()
            assert (out / "version_3" / "dags").is_dir()
            assert (out / "definitions" / "marts").is_dir()
            assert (out / "definitions" / "sources").is_dir()
            assert len(list((out / "version_2" / "dags").glob("*.py"))) >= 1
            assert len(list((out / "version_3" / "dags").glob("*.py"))) >= 1
            # Verify generated DAG file
            dag_files = list((out / "dags").glob("*.py"))
            assert len(dag_files) >= 1
            dag_content = dag_files[0].read_text(encoding="utf-8")
            assert "DAG(" in dag_content
            assert "DataformCreateCompilationResultOperator" in dag_content
            assert "DataformCreateWorkflowInvocationOperator" in dag_content
            return result

    def test_house_buying(self):
        result = self._generate("sample_flowspec_house_buying.json")
        assert result.workflow_name == "house_buying_analysis"
        assert len(result.models) == 1
        assert result.models[0].name == "result_table"
        assert len(result.declarations) == 1
        assert result.declarations[0].name == "bank_data"
        assert result.depends_on_past is False
        assert result.cross_workflow_deps == []
        assert result.schedule == "02 00 * * *"
        assert result.timezone == "Asia/Jakarta"
        # No review items for this simple case
        assert len(result.review) == 0

    def test_complex_ecommerce(self):
        result = self._generate("sample_flowspec_complex_ecommerce.json")
        assert result.workflow_name == "ecommerce_daily_transform"
        # 4 ODPS_SQL models
        assert len(result.models) == 4
        model_names = {m.name for m in result.models}
        assert model_names == {"ods_raw_orders", "ods_raw_users", "dwd_order_facts", "dws_agg_daily"}
        # 2 declarations (source tables not written by any model)
        assert len(result.declarations) == 2
        decl_names = {d.name for d in result.declarations}
        assert decl_names == {"mysql_users", "oss_raw_orders"}
        # Review items: 4 partition flags + 2 PYODPS + 1 DIDE_SHELL + 1 virtual
        review_text = "\n".join(result.review)
        assert "PARTITION BY" in review_text
        assert "PYODPS" in review_text
        assert "DIDE_SHELL" in review_text

    def test_model_sqlx_content(self, house_buying_spec):
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            generator.generate_project(
                house_buying_spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
            )
            sqlx = (out / "definitions" / "marts" / "result_table.sqlx").read_text()
            assert 'type: "table"' in sqlx
            assert "tags:" in sqlx
            assert "ref(" in sqlx

    def test_workflow_settings_yaml(self, house_buying_spec):
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            generator.generate_project(
                house_buying_spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
            )
            settings = (out / "workflow_settings.yaml").read_text()
            assert "dataformCoreVersion:" in settings
            assert "defaultProject: test-project" in settings
            assert "defaultDataset: dwh" in settings
            assert "bizdate:" in settings

    def test_dag_has_external_task_sensor(self):
        spec = {
            "spec": {
                "name": "cross_wf_test",
                "workflows": [{
                    "id": "wf1",
                    "name": "cross_wf_test",
                    "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
                    "variables": [],
                    "nodes": [
                        {"id": "n1", "name": "task1", "script": {
                            "runtime": {"command": "ODPS_SQL"},
                            "content": "INSERT OVERWRITE TABLE t1 SELECT 1"
                        }, "inputs": {
                            "nodeOutputs": [{"data": "n0", "externalWorkflowId": "ext_wf"}]
                        }, "outputs": {"tables": [{"guid": "dwh.t1"}]}},
                    ],
                    "flow": [{"nodeId": "n1", "depends": []}],
                }],
            }
        }
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
            )
            dag_files = list((out / "dags").glob("*.py"))
            assert len(dag_files) == 1
            content = dag_files[0].read_text()
            assert "ExternalTaskSensor" in content
            assert "ext_wf" in content
            assert "CROSS-WORKFLOW" in "\n".join(
                (out / "review" / f"review_{generator._safe('cross_wf_test')}.md").read_text().splitlines()
                if (out / "review" / f"review_{generator._safe('cross_wf_test')}.md").exists()
                else []
            )

    def test_cross_cycle_depends_on_past(self):
        spec = {
            "spec": {
                "name": "cycle_test",
                "workflows": [{
                    "id": "wf2",
                    "name": "cycle_test",
                    "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
                    "variables": [],
                    "nodes": [
                        {"id": "n1", "name": "task1", "crossCycleDependsOnOtherNode": True,
                         "script": {"runtime": {"command": "ODPS_SQL"},
                                    "content": "INSERT OVERWRITE TABLE t1 SELECT 1"},
                         "outputs": {"tables": [{"guid": "dwh.t1"}]}},
                    ],
                    "flow": [{"nodeId": "n1", "depends": []}],
                }],
            }
        }
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
            )
            assert result.depends_on_past is True
            dag_content = (out / "dags" / f"{result.tag}.py").read_text()
            assert "depends_on_past" in dag_content or "CROSS-CYCLE" in "\n".join(result.review)


# ===================================================================
# 9. Translator: existing features still work
# ===================================================================

class TestTranslator:
    """Regression tests for translator.py."""

    def test_translate_type_basic(self):
        assert translator.translate_type("INT") == "INT64"
        assert translator.translate_type("BIGINT") == "INT64"
        assert translator.translate_type("STRING") == "STRING"
        assert translator.translate_type("FLOAT") == "FLOAT64"
        assert translator.translate_type("BOOLEAN") == "BOOL"
        assert translator.translate_type("BINARY") == "BYTES"

    def test_translate_type_decimal(self):
        assert translator.translate_type("DECIMAL(10,2)") == "NUMERIC"
        assert translator.translate_type("DECIMAL(50,10)") == "BIGNUMERIC"

    def test_translate_type_complex(self):
        result = translator.translate_type("MAP<STRING, BIGINT>")
        assert "ARRAY<STRUCT<" in result
        assert "INT64" in result

    def test_translate_query_ref(self):
        sql, _flags = translator.translate_query("SELECT * FROM my_table")
        assert 'ref("my_table")' in sql

    def test_translate_query_bizdate_resolved(self):
        sql, _flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '${bizdate}'",
            available_vars={"bizdate"},
        )
        assert 'dataform.projectConfig.vars["bizdate"]' in sql

    def test_translate_query_bizdate_unresolved(self):
        sql, flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '${bizdate}'",
        )
        assert "${bizdate}" in sql
        assert any("Unresolved variable" in f for f in flags)

    def test_translate_query_flag_functions(self):
        _sql, flags = translator.translate_query("SELECT DATEADD(d, 1, dt) FROM t")
        assert any("DATEADD" in f for f in flags)

    def test_bracket_var_plain_resolved(self):
        sql, _flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '$[yyyymmdd]'",
            available_vars={"yyyymmdd"},
        )
        assert 'dataform.projectConfig.vars["yyyymmdd"]' in sql

    def test_bracket_var_negative_offset(self):
        sql, flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '$[yyyymmdd-1]'",
            available_vars={"yyyymmdd"},
        )
        assert "setUTCDate(d.getUTCDate()-1)" in sql
        assert not any("Unresolved" in f for f in flags)

    def test_bracket_var_positive_offset(self):
        sql, _flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '$[yyyymmdd+7]'",
            available_vars={"yyyymmdd"},
        )
        assert "setUTCDate(d.getUTCDate()+7)" in sql

    def test_bracket_var_offset_unresolved_flags(self):
        _sql, flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '$[foo-1]'",
            vars_available=True,
        )
        assert any("Unresolved bracket variable $[foo]" in f for f in flags)

    def test_bracket_var_offset_on_non_date_var_flags(self):
        sql, flags = translator.translate_query(
            "SELECT * FROM t WHERE dt = '$[cyctime-1]'",
            vars_available=True,
        )
        # cyctime is KNOWN but not YYYYMMDD -> arithmetic not supported
        assert "cyctime" in sql  # left verbatim, not silently corrupted
        assert any("not supported" in f for f in flags)

    def test_parse_insert(self):
        result = translator.parse_insert(
            "INSERT OVERWRITE TABLE my_table SELECT * FROM src"
        )
        assert result is not None
        assert result["table"] == "my_table"

    def test_parse_create(self):
        result = translator.parse_create(
            "CREATE TABLE IF NOT EXISTS t (id INT, name STRING COMMENT 'name col')"
        )
        assert result is not None
        assert result["name"] == "t"
        assert len(result["columns"]) == 2
        assert result["columns"][0] == ("id", "INT64", "")
        assert result["columns"][1] == ("name", "STRING", "name col")


# ===================================================================
# 9b. Routing guardrail: data-heavy PYODPS/Python never lands on the
#     Composer worker (guide §10).
# ===================================================================

class TestRoutingGuardrail:
    """Verify PYODPS/PYTHON routing: SQL-ish -> Dataform, light -> PythonOperator,
    data-heavy -> Dataflow (Beam)."""

    def test_heavy_pyodps_goes_to_dataflow(self):
        node = {"script": {"runtime": {"command": "PYODPS"},
                           "content": "o2o.execute(sql)  # big join over millions of rows"}}
        target, _reason = generator.route_node(node)
        assert target == "Dataflow (Beam)"

    def test_heavy_python_goes_to_dataflow(self):
        node = {"script": {"runtime": {"command": "PYTHON"},
                           "content": "df = spark.read_sql('SELECT * FROM t').join(...)"}}
        target, _reason = generator.route_node(node)
        assert target == "Dataflow (Beam)"

    def test_light_pyodps_goes_to_python_operator(self):
        node = {"script": {"runtime": {"command": "PYODPS"},
                           "content": "print('loaded')"}}
        target, _reason = generator.route_node(node)
        assert target == "PythonOperator"

    def test_sqlish_pyodps_goes_to_dataform(self):
        node = {"script": {"runtime": {"command": "PYODPS"},
                           "content": "select * from t"}}
        target, _reason = generator.route_node(node)
        assert target == "Dataform .sqlx"

    def test_heavy_node_not_emitted_as_python_operator(self):
        """A data-heavy PYODPS node must not produce a PythonOperator in the DAG."""
        spec = {
            "spec": {
                "name": "heavy_test",
                "workflows": [{
                    "id": "w1", "name": "heavy_test",
                    "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
                    "variables": [],
                    "nodes": [
                        {"id": "n1", "name": "big_transform",
                         "script": {"runtime": {"command": "PYODPS"},
                                    "content": "o2o.execute('SELECT * FROM x JOIN y ON x.id = y.id')"},
                         "outputs": {"tables": [{"guid": "dwh.out"}]}},
                    ],
                    "flow": [{"nodeId": "n1", "depends": []}],
                }],
            }
        }
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out, project="test", region="asia-southeast2",
                repo="migrated", dataset="dwh", assertion_dataset="dwh_assertions",
                core_version="3.0.64",
            )
            assert result.python_operator_nodes == []
            dag = (out / "dags" / "heavy_test.py").read_text()
            assert "PythonOperator(" not in dag  # no task instantiated
            assert "_task_big_transform" not in dag
            assert "Dataflow" in "\n".join(result.review)

    def test_light_node_emitted_as_python_operator(self):
        """A lightweight PYODPS node is emitted as a PythonOperator in the DAG."""
        spec = {
            "spec": {
                "name": "light_test",
                "workflows": [{
                    "id": "w2", "name": "light_test",
                    "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
                    "variables": [],
                    "nodes": [
                        {"id": "n1", "name": "notify",
                         "script": {"runtime": {"command": "PYTHON"},
                                    "content": "print('loaded')"},
                         "outputs": {"tables": []}},
                    ],
                    "flow": [{"nodeId": "n1", "depends": []}],
                }],
            }
        }
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out, project="test", region="asia-southeast2",
                repo="migrated", dataset="dwh", assertion_dataset="dwh_assertions",
                core_version="3.0.64",
            )
            assert len(result.python_operator_nodes) == 1
            dag = (out / "dags" / "light_test.py").read_text()
            assert "PythonOperator" in dag
            assert "_task_notify" in dag


# ===================================================================
# 10. flowspec_to_csv: JSON -> CSV conversion
# ===================================================================

class TestFlowspecToCsv:
    """Tests for classify/flowspec_to_csv.py."""

    def test_house_buying_to_rows(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        assert len(rows) == 3
        node_ids = {r["node_id"] for r in rows}
        assert node_ids == {"n_start", "n_ddl", "n_insert"}

    def test_house_buying_row_fields(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        ddl_row = next(r for r in rows if r["node_id"] == "n_ddl")
        assert ddl_row["workflow"] == "house_buying_analysis"
        assert ddl_row["node_name"] == "ddl_result_table"
        assert ddl_row["command"] == "ODPS_SQL"
        assert ddl_row["gcp_target"] == "Dataform .sqlx"
        # sanitize_cron("00 02 00 * * ?") -> "02 00 * * *"
        assert ddl_row["schedule"] == "02 00 * * *"
        assert ddl_row["skip"] == ""
        assert ddl_row["review_notes"] == ""

    def test_ecommerce_to_rows(self, ecommerce_spec):
        rows = flowspec_to_rows(ecommerce_spec)
        assert len(rows) == 10
        node_ids = {r["node_id"] for r in rows}
        assert "n_start" in node_ids
        assert "n_end" in node_ids
        assert "n_transform" in node_ids

    def test_virtual_node_routed_to_empty_operator(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        start_row = next(r for r in rows if r["node_id"] == "n_start")
        assert start_row["gcp_target"] == "EmptyOperator"

    def test_write_and_load_csv_roundtrip(self, house_buying_spec, tmp_path):
        rows = flowspec_to_rows(house_buying_spec)
        csv_path = tmp_path / "matrix.csv"
        write_csv(rows, csv_path)
        assert csv_path.exists()

        loaded = load_csv_overrides(csv_path)
        assert len(loaded) == 3
        assert "n_ddl" in loaded
        assert loaded["n_ddl"]["gcp_target"] == "Dataform .sqlx"

    def test_csv_fields_match_schema(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        for row in rows:
            assert set(row.keys()) == set(CSV_FIELDS)


# ===================================================================
# 11. CSV overrides applied during generation
# ===================================================================

class TestCsvOverrides:
    """Tests for CSV override logic in generator.generate_project()."""

    def _generate_with_csv(self, spec_name: str, csv_rows: list[dict]) -> generator.GeneratedWorkflow:
        spec = _load(spec_name)
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            # Write CSV
            csv_path = out / "migration_matrix.csv"
            write_csv(csv_rows, csv_path)
            overrides = load_csv_overrides(csv_path)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
                csv_overrides=overrides,
            )
            return result

    def test_skip_node_excludes_model(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        # Skip the INSERT node (n_insert) -> only DDL node generates a model (absorbed)
        for r in rows:
            if r["node_id"] == "n_insert":
                r["skip"] = "true"
        result = self._generate_with_csv("sample_flowspec_house_buying.json", rows)
        # n_insert is skipped, so result_table is NOT generated (no INSERT...SELECT)
        model_names = {m.name for m in result.models}
        assert "result_table" not in model_names
        assert any("SKIPPED" in r for r in result.review)

    def test_override_gcp_target(self, ecommerce_spec):
        rows = flowspec_to_rows(ecommerce_spec)
        # Override n_quality (PYODPS) from TRIAGE to PythonOperator
        for r in rows:
            if r["node_id"] == "n_quality":
                r["gcp_target"] = "PythonOperator"
        result = self._generate_with_csv("sample_flowspec_complex_ecommerce.json", rows)
        review_text = "\n".join(result.review)
        assert "CSV override: PythonOperator" in review_text

    def test_review_notes_appears_in_review(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        for r in rows:
            if r["node_id"] == "n_insert":
                r["review_notes"] = "Check partition strategy before deploy"
        result = self._generate_with_csv("sample_flowspec_house_buying.json", rows)
        review_text = "\n".join(result.review)
        assert "Check partition strategy" in review_text

    def test_schedule_override_in_dag(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        # Override schedule to 0 4 * * *
        for r in rows:
            r["schedule"] = "0 4 * * *"
        spec = _load("sample_flowspec_house_buying.json")
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            csv_path = out / "migration_matrix.csv"
            write_csv(rows, csv_path)
            overrides = load_csv_overrides(csv_path)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
                csv_overrides=overrides,
            )
            assert result.schedule == "0 4 * * *"
            dag_content = (out / "dags" / f"{result.tag}.py").read_text()
            assert 'CronTriggerTimetable("0 4 * * *"' in dag_content
            v2_content = (out / "version_2" / "dags" / f"{result.tag}.py").read_text()
            assert 'schedule_interval="0 4 * * *"' in v2_content

    def test_no_csv_uses_auto_route(self, house_buying_spec):
        """Without CSV overrides, behavior is identical to before."""
        spec = _load("sample_flowspec_house_buying.json")
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
                csv_overrides=None,
            )
            assert len(result.models) == 1
            assert result.models[0].name == "result_table"

    def test_skip_all_nodes_empty_generation(self, house_buying_spec):
        rows = flowspec_to_rows(house_buying_spec)
        for r in rows:
            r["skip"] = "true"
        result = self._generate_with_csv("sample_flowspec_house_buying.json", rows)
        assert len(result.models) == 0
        assert len(result.declarations) == 0
        assert all("SKIPPED" in r for r in result.review)


# ===================================================================
# 12. End-to-end: JSON -> CSV -> edit -> generate -> verify
# ===================================================================

class TestEndToEndCsvFlow:
    """Full pipeline: FlowSpec JSON -> CSV -> user edit -> generation with overrides."""

    def test_full_flow_house_buying(self):
        """Simulate the complete user workflow."""
        spec = _load("sample_flowspec_house_buying.json")
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)

            # Step 1: Generate CSV from FlowSpec
            rows = flowspec_to_rows(spec)
            csv_path = out / "migration_matrix.csv"
            write_csv(rows, csv_path)
            assert csv_path.exists()

            # Step 2: User edits CSV (simulate)
            loaded = load_csv_overrides(csv_path)
            # User adds a review note to the INSERT node
            loaded["n_insert"]["review_notes"] = "Verify bank_data source is synced"
            # User overrides schedule
            for nid in loaded:
                loaded[nid]["schedule"] = "0 5 * * *"
            write_csv(list(loaded.values()), csv_path)

            # Step 3: Generate with CSV overrides
            overrides = load_csv_overrides(csv_path)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
                csv_overrides=overrides,
            )

            # Step 4: Verify overrides applied
            assert result.schedule == "0 5 * * *"
            review_text = "\n".join(result.review)
            assert "Verify bank_data source is synced" in review_text
            # Models still generated (nothing skipped)
            assert len(result.models) == 1
            assert result.models[0].name == "result_table"

    def test_full_flow_ecommerce_with_skips(self):
        """Ecommerce with some nodes skipped and target overrides."""
        spec = _load("sample_flowspec_complex_ecommerce.json")
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)

            # Step 1: Generate CSV
            rows = flowspec_to_rows(spec)
            csv_path = out / "migration_matrix.csv"
            write_csv(rows, csv_path)

            # Step 2: User edits — skip alert_failure, override quality_check target
            loaded = load_csv_overrides(csv_path)
            loaded["n_notify"]["skip"] = "true"
            loaded["n_quality"]["gcp_target"] = "PythonOperator"
            loaded["n_quality"]["review_notes"] = "Lightweight check, no SQL pushdown"
            write_csv(list(loaded.values()), csv_path)

            # Step 3: Generate
            overrides = load_csv_overrides(csv_path)
            result = generator.generate_project(
                spec, out,
                project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh",
                assertion_dataset="dwh_assertions", core_version="3.0.64",
                csv_overrides=overrides,
            )

            # Step 4: Verify
            review_text = "\n".join(result.review)
            assert "SKIPPED" in review_text
            assert "alert_failure" in review_text
            assert "CSV override: PythonOperator" in review_text
            assert "Lightweight check" in review_text
            # 4 ODPS_SQL models still generated
            assert len(result.models) == 4


# ===================================================================
# 13. Airflow 2.x / 3.x DAG variants (version_2/ + version_3/ folders)
# ===================================================================

class TestAirflowVersionDags:
    """DAG output must support Airflow 2.x and 3.x in separate folders.

    Two parsing/runtime problems were reported on Airflow 3:
      1. TypeError: __init__() got an unexpected keyword argument 'timezone'
         (the 'timezone' DAG kwarg was removed in Airflow 3).
      2. ValueError: Unknown field for WorkflowInvocation: variables
         (Dataform vars are compile-time -> code_compilation_config.vars dict).
    These tests pin the generated DAGs to version-correct APIs and the
    compile-time var wiring.
    """

    def _generate(self, spec):
        # Captures generated *.py contents into a {rel_path: text} dict so they
        # survive the TemporaryDirectory teardown below.
        with tempfile.TemporaryDirectory() as tmpd:
            out = Path(tmpd)
            result = generator.generate_project(
                spec, out, project="test-project", region="asia-southeast2",
                repo="migrated", dataset="dwh", assertion_dataset="dwh_assertions",
                core_version="3.0.64")
            files = {
                str(p.relative_to(out)): p.read_text()
                for p in sorted(out.rglob("*.py"))
            }
            return files, result

    def test_both_version_folders_generated(self, house_buying_spec):
        files, _ = self._generate(house_buying_spec)
        v2 = "version_2/dags/house_buying_analysis.py"
        v3 = "version_3/dags/house_buying_analysis.py"
        active = "dags/house_buying_analysis.py"
        assert v2 in files and v3 in files and active in files
        # dags/ mirrors version_3 by default (AIRFLOW_MAJOR_VERSION=3).
        assert files[active] == files[v3]

    def test_version_2_dag_uses_airflow2_api(self, house_buying_spec):
        files, _ = self._generate(house_buying_spec)
        dag = files["version_2/dags/house_buying_analysis.py"]
        assert "from airflow import DAG" in dag
        assert "from airflow.operators.empty import EmptyOperator" in dag
        assert 'schedule_interval="02 00 * * *"' in dag
        assert 'start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta")' in dag
        # Problem A: no direct timezone kwarg on DAG().
        assert "    timezone=" not in dag
        # Problem B: vars moved out of workflow_invocation.
        assert '"variables"' not in dag
        compile(dag, "v2_dag.py", "exec")  # must be valid Python

    def test_version_3_dag_uses_airflow3_api(self, house_buying_spec):
        files, _ = self._generate(house_buying_spec)
        dag = files["version_3/dags/house_buying_analysis.py"]
        assert "from airflow.sdk import DAG" in dag
        assert "from airflow.timetables.trigger import CronTriggerTimetable" in dag
        assert "from airflow.providers.standard.operators.empty import EmptyOperator" in dag
        assert 'schedule=CronTriggerTimetable("02 00 * * *", timezone="Asia/Jakarta")' in dag
        assert "from airflow.operators.empty" not in dag
        assert "    timezone=" not in dag
        assert '"variables"' not in dag
        compile(dag, "v3_dag.py", "exec")

    def test_vars_go_to_compilation_config(self, house_buying_spec):
        files, _ = self._generate(house_buying_spec)
        for folder in ("version_2", "version_3"):
            dag = files[f"{folder}/dags/house_buying_analysis.py"]
            assert '"code_compilation_config": {' in dag
            assert '"vars": {' in dag
            assert '"bizdate": "{{ macros.ds_format(macros.ds_add(ds, -1),' in dag
            assert '"invocation_config": {"included_tags": [TAG]},' in dag
            assert '"variables"' not in dag

    def test_cyctime_macro_differs_by_version(self):
        spec = {"spec": {"name": "cyc", "workflows": [{
            "id": "w", "name": "cyc",
            "trigger": {"cron": "00 02 00 * * ?", "timezone": "Asia/Jakarta"},
            "variables": [{"id": "v", "name": "cyctime", "scope": "Workflow",
                           "type": "System", "value": "${cyctime}"}],
            "nodes": [{"id": "n1", "name": "t1", "script": {
                "runtime": {"command": "ODPS_SQL"},
                "content": "INSERT OVERWRITE TABLE t1 SELECT 1"},
                "outputs": {"tables": [{"guid": "dwh.t1"}]}}],
            "flow": [{"nodeId": "n1", "depends": []}],
        }]}}
        files, _ = self._generate(spec)
        v2 = files["version_2/dags/cyc.py"]
        v3 = files["version_3/dags/cyc.py"]
        # logical_date is removed in Airflow 3 -> data_interval_start there.
        assert "logical_date.strftime" in v2
        assert "data_interval_start.strftime" in v3
        assert "logical_date.strftime" not in v3
        compile(v2, "cyc_v2.py", "exec")
        compile(v3, "cyc_v3.py", "exec")

    def test_python_operator_import_differs_by_version(self):
        spec = {"spec": {"name": "light", "workflows": [{
            "id": "w", "name": "light",
            "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
            "variables": [],
            "nodes": [{"id": "n1", "name": "notify",
                       "script": {"runtime": {"command": "PYTHON"},
                                  "content": "print('x')"},
                       "outputs": {"tables": []}}],
            "flow": [{"nodeId": "n1", "depends": []}],
        }]}}
        files, result = self._generate(spec)
        assert len(result.python_operator_nodes) == 1
        v2 = files["version_2/dags/light.py"]
        v3 = files["version_3/dags/light.py"]
        assert "from airflow.operators.python import PythonOperator" in v2
        assert "from airflow.providers.standard.operators.python import PythonOperator" in v3
        assert "_task_notify" in v2 and "_task_notify" in v3

    def test_external_task_sensor_import_differs_by_version(self):
        spec = {"spec": {"name": "cross", "workflows": [{
            "id": "w", "name": "cross",
            "trigger": {"cron": "00 02 00 * * ?", "timezone": "UTC"},
            "variables": [],
            "nodes": [{"id": "n1", "name": "t1", "script": {
                "runtime": {"command": "ODPS_SQL"},
                "content": "INSERT OVERWRITE TABLE t1 SELECT 1"},
                "inputs": {"nodeOutputs": [{"data": "n0", "externalWorkflowId": "ext_wf"}]},
                "outputs": {"tables": [{"guid": "dwh.t1"}]}}],
            "flow": [{"nodeId": "n1", "depends": []}],
        }]}}
        files, result = self._generate(spec)
        assert result.cross_workflow_deps and len(result.cross_workflow_deps) == 1
        v2 = files["version_2/dags/cross.py"]
        v3 = files["version_3/dags/cross.py"]
        assert "from airflow.sensors.external_task import ExternalTaskSensor" in v2
        assert (
            "from airflow.providers.standard.sensors.external_task import ExternalTaskSensor"
            in v3
        )
        assert "wait_external_0" in v2 and "wait_external_0" in v3
