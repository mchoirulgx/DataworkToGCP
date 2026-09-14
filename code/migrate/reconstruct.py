"""Reconstruct workflow-level FlowSpecs from the extractor's per-node output.

Real DataWorks extraction writes one `node_<id>.json` per node plus, since the
latest extractor change, one `workflow_<id>.json` per workflow (carrying the
GetWorkflow trigger that per-node files lack). This module is the single place
that understands both shapes and turns them into the workflow-level FlowSpec the
generator consumes:

    node files (grouped by workflow_id) + node inputs.nodeOutputs
            ->  workflow-level FlowSpec with a rebuilt `flow[]` dependency graph
"""

from __future__ import annotations

import json
from pathlib import Path


def spec_nodes(spec: dict) -> list[dict]:
    """Normalize any FlowSpec-ish document to a flat list of node dicts.

    Handles three shapes defensively (GetNode output varies by API version):
      * node-level:  {id, name, script, inputs, outputs, ...}
      * spec.nodes:  {"spec": {"nodes": [...]}}
      * workflow:    {"spec": {"workflows": [{"nodes": [...]}]}}
    """
    if not isinstance(spec, dict):
        return []
    if "script" in spec and "nodes" not in spec and "workflows" not in spec:
        return [spec]
    inner = spec.get("spec", spec)
    if not isinstance(inner, dict):
        return []
    nodes = inner.get("nodes")
    if isinstance(nodes, list):
        return nodes
    for workflow in inner.get("workflows") or []:
        wf_nodes = workflow.get("nodes")
        if isinstance(wf_nodes, list):
            return wf_nodes
    return []


def build_flow(nodes: list[dict]) -> list[dict]:
    """Rebuild the `flow[]` graph from each node's inputs.nodeOutputs."""
    flow = []
    for node in nodes:
        deps = []
        for ref in (node.get("inputs") or {}).get("nodeOutputs") or []:
            upstream = ref.get("data")
            if upstream:
                deps.append({"nodeId": upstream, "type": "Normal"})
        flow.append({"nodeId": node.get("id"), "depends": deps})
    return flow


def nodes_to_flowspec(
    wf_name: str,
    wf_id: str,
    nodes: list[dict],
    trigger: dict | None = None,
    variables: list | None = None,
) -> dict:
    """Assemble a workflow-level FlowSpec the generator can consume."""
    return {
        "kind": "CycleWorkflow",
        "version": "1.1.0",
        "metadata": {"uuid": wf_id},
        "spec": {
            "name": wf_name,
            "id": wf_id,
            "type": "CycleWorkflow",
            "workflows": [
                {
                    "id": wf_id,
                    "name": wf_name,
                    "trigger": trigger or {},
                    "variables": variables or [],
                    "nodes": nodes,
                    "flow": build_flow(nodes),
                }
            ],
        },
    }


def _dedupe_nodes(nodes: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for node in nodes:
        node_id = str(node.get("id") or "")
        if node_id and node_id in seen:
            continue
        if node_id:
            seen.add(node_id)
        result.append(node)
    return result


def load_workflows(extract_dir: Path) -> list[dict]:
    """Workflow-level files when present; otherwise rebuild from node files."""
    if not extract_dir.is_dir():
        raise SystemExit(f"Extract dir not found: {extract_dir}")

    wf_files = sorted(extract_dir.glob("workflow_*.json"))
    if wf_files:
        return [json.loads(f.read_text(encoding="utf-8")) for f in wf_files]

    node_files = sorted(extract_dir.glob("node_*.json"))
    if not node_files:
        raise SystemExit(f"No node_*.json / workflow_*.json under {extract_dir}")

    grouped: dict[str, list[dict]] = {}
    for path in node_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        wf_id = str(data.get("workflow_id") or "")
        for node in spec_nodes(data.get("spec") or {}):
            grouped.setdefault(wf_id, []).append(node)

    if not grouped:
        raise SystemExit(f"No nodes found under {extract_dir}")

    workflows = []
    for wf_id, nodes in sorted(grouped.items()):
        nodes = _dedupe_nodes(nodes)
        wf_name = f"workflow_{wf_id}" if wf_id else "workflow"
        workflows.append(nodes_to_flowspec(wf_name, wf_id, nodes))
    return workflows
