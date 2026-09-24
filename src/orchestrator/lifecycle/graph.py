"""Append-only DAG validation for initial and dynamically added nodes."""

from __future__ import annotations

from orchestrator.lifecycle.models import NodeSpec


class GraphError(ValueError):
    """A proposed graph append violates the frozen graph envelope."""


def validate_graph_append(
    current: tuple[NodeSpec, ...],
    additions: tuple[NodeSpec, ...],
    *,
    max_nodes: int,
    max_depth: int,
) -> tuple[NodeSpec, ...]:
    if not additions:
        raise GraphError("graph append must add at least one node")
    known = {node.node_id for node in current}
    adding = [node.node_id for node in additions]
    if len(adding) != len(set(adding)) or known.intersection(adding):
        raise GraphError("graph append contains a duplicate node ID")
    nodes = {node.node_id: node for node in current}
    nodes.update((node.node_id, node) for node in additions)
    if len(nodes) > max_nodes:
        raise GraphError("graph node count exceeds the frozen Run limit")
    for node in additions:
        missing = set(node.depends_on) - set(nodes)
        if missing:
            raise GraphError("graph dependency references a node that does not exist")
        if node.node_id in node.depends_on:
            raise GraphError("graph node cannot depend on itself")

    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def visit(node_id: str) -> int:
        if node_id in visiting:
            raise GraphError("graph dependencies must be acyclic")
        if node_id in depths:
            return depths[node_id]
        visiting.add(node_id)
        node = nodes[node_id]
        depth = 0 if not node.depends_on else 1 + max(visit(parent) for parent in node.depends_on)
        visiting.remove(node_id)
        if depth > max_depth:
            raise GraphError("graph depth exceeds the frozen Run limit")
        depths[node_id] = depth
        return depth

    for node_id in sorted(nodes):
        visit(node_id)
    return tuple(sorted(additions, key=lambda node: node.node_id))


__all__ = ["GraphError", "validate_graph_append"]
