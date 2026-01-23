"""Graph implementation to store and query mappings."""

from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

NodeT = TypeVar("NodeT")


class _BaseGraph[NodeT]:
    """Lightweight graph with support for directed and undirected edges."""

    def __init__(self) -> None:
        """Initialize empty adjacency and predecessor maps."""
        self._adj: dict[NodeT, set[NodeT]] = {}
        self._pred: dict[NodeT, set[NodeT]] = {}

    def _ensure_node(self, node: NodeT) -> None:
        """Ensure a node exists in adjacency and predecessor maps."""
        if node not in self._adj:
            self._adj[node] = set()
            self._pred[node] = set()

    def add_edge(self, a: NodeT, b: NodeT, bidirectional: bool = True) -> None:
        """Add an edge between nodes.

        Args:
            a (NodeT): Start node.
            b (NodeT): End node.
            bidirectional (bool): If True, adds both directions.
        """
        if a == b:
            self._ensure_node(a)
            return
        self._ensure_node(a)
        self._ensure_node(b)
        self._adj[a].add(b)
        self._pred[b].add(a)
        if bidirectional:
            self._adj[b].add(a)
            self._pred[a].add(b)

    def has_edge(self, a: NodeT, b: NodeT) -> bool:
        """Check if an edge exists between `a` and `b`.

        Args:
            a (NodeT): Start node.
            b (NodeT): End node.

        Returns:
            bool: True if an edge exists in either direction.
        """
        return b in self._adj.get(a, set()) or a in self._adj.get(b, set())

    def add_equivalence_class(self, nodes: Iterable[NodeT]) -> None:
        """Add an undirected equivalence class of nodes.

        Args:
            nodes (Iterable[NodeT]): Nodes to connect together.
        """
        unique = list(dict.fromkeys(nodes))
        if len(unique) <= 1:
            for node in unique:
                self._ensure_node(node)
            return
        base = unique[0]
        for other in unique[1:]:
            self.add_edge(base, other, bidirectional=True)

    def add_graph(self, other: "_BaseGraph[NodeT]") -> None:
        """Merge another graph's edges into this graph.

        Args:
            other (_BaseGraph[NodeT]): Graph to merge.
        """
        for node in other.nodes():
            self._ensure_node(node)
        for node in other.nodes():
            for neighbor in other.neighbors(node):
                # We assume if it's in neighbors, it's an edge.
                # We don't know if it was bidirectional in the source,
                # but we can just add it as directed here.
                # If the source had it bidirectional, we'll see the reverse edge later.
                self.add_edge(node, neighbor, bidirectional=False)

    def has_node(self, node: NodeT) -> bool:
        """Check if a node exists in the graph.

        Args:
            node (NodeT): Node to check.
        """
        return node in self._adj

    def neighbors(self, node: NodeT) -> set[NodeT]:
        """Return the neighbor set for a node.

        Args:
            node (NodeT): Node to inspect.
        """
        return self._adj.get(node, set()).copy()

    def remove_edge(self, a: NodeT, b: NodeT) -> None:
        """Remove an edge between `a` and `b` if present (both directions).

        Args:
            a (NodeT): Start node.
            b (NodeT): End node.
        """
        if a in self._adj:
            self._adj[a].discard(b)
        if b in self._pred:
            self._pred[b].discard(a)
        if b in self._adj:
            self._adj[b].discard(a)
        if a in self._pred:
            self._pred[a].discard(b)

    def get_component(self, start: NodeT) -> set[NodeT]:
        """Return the connected component containing `start`.

        Args:
            start (NodeT): Node to start the traversal from.

        Returns:
            set[NodeT]: Nodes in the connected component.
        """
        if start not in self._adj:
            return set()
        visited: set[NodeT] = set()
        queue: deque[NodeT] = deque([start])
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(nb for nb in self._adj[node] if nb not in visited)
        return visited

    def node_count(self) -> int:
        """Return the total number of nodes in the graph.

        Returns:
            int: Node count.
        """
        return len(self._adj)

    def nodes(self) -> set[NodeT]:
        """Return all nodes in the graph.

        Returns:
            set[NodeT]: Nodes in the graph.
        """
        return set(self._adj)

    def remove_node(self, node: NodeT) -> None:
        """Remove a node and all incident edges.

        Args:
            node (NodeT): Node to remove.
        """
        if node not in self._adj:
            return

        # Remove outgoing edges
        for neighbor in self._adj[node]:
            self._pred[neighbor].discard(node)

        # Remove incoming edges
        for predecessor in self._pred[node]:
            self._adj[predecessor].discard(node)

        del self._adj[node]
        del self._pred[node]


IdNode = tuple[str, str, str | None]  # (provider, id, scope)
EpisodeNode = tuple[str, str, str | None, str]  # (provider, id, scope, episode_range)


@dataclass(slots=True)
class ProvenanceContext:
    """Context used to record provenance events."""

    stage: str
    actor: str | None = None
    reason: str | None = None
    details: dict[str, Any] | None = None


@dataclass(slots=True)
class ProvenanceEvent:
    """Recorded event describing a mapping change."""

    seq: int
    action: str
    stage: str
    actor: str | None
    reason: str | None
    effective: bool
    details: dict[str, Any] | None = None


class IdMappingGraph(_BaseGraph[IdNode]):
    """Undirected graph of provider IDs."""

    def get_component_by_provider(
        self, start: IdNode
    ) -> dict[str, set[tuple[str, str | None]]]:
        """Get the connected component grouped by provider, preserving scope.

        Args:
            start (IdNode): Node to start traversal from.

        Returns:
            dict[str, set[tuple[str, str | None]]]: Providers mapped to IDs/scopes.
        """
        comp = self.get_component(start)
        grouped: dict[str, set[tuple[str, str | None]]] = {}
        for provider, entry_id, scope in comp:
            grouped.setdefault(provider, set()).add((entry_id, scope))
        return grouped


class EpisodeMappingGraph(_BaseGraph[EpisodeNode]):
    """Graph of episode range mappings."""

    def __init__(self) -> None:
        """Initialize empty graph with provenance tracking."""
        super().__init__()
        self._provenance: dict[
            tuple[EpisodeNode, EpisodeNode], list[ProvenanceEvent]
        ] = {}
        self._provenance_seq = 0
        self._provenance_context: ProvenanceContext | None = None

    def _node_key(self, node: EpisodeNode) -> tuple[str, str, str, str]:
        """Key function for sorting nodes."""
        provider, entry_id, scope, episode_range = node
        return (
            str(provider),
            str(entry_id),
            "" if scope is None else str(scope),
            str(episode_range),
        )

    def _edge_key(
        self, a: EpisodeNode, b: EpisodeNode
    ) -> tuple[EpisodeNode, EpisodeNode]:
        """Key function for sorting undirected edges."""
        left, right = sorted((a, b), key=self._node_key)
        return (left, right)

    def _record_event(
        self,
        action: str,
        a: EpisodeNode,
        b: EpisodeNode,
        context: ProvenanceContext | None,
        *,
        effective: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record a provenance event for an edge modification."""
        ctx = context or self._provenance_context
        stage = ctx.stage if ctx else "unknown"
        actor = ctx.actor if ctx else None
        reason = ctx.reason if ctx else None
        merged_details: dict[str, Any] | None = None
        if ctx and ctx.details:
            merged_details = dict(ctx.details)
        if details:
            merged_details = {**(merged_details or {}), **details}
        self._provenance_seq += 1
        event = ProvenanceEvent(
            seq=self._provenance_seq,
            action=action,
            stage=stage,
            actor=actor,
            reason=reason,
            effective=effective,
            details=merged_details,
        )
        key = self._edge_key(a, b)
        self._provenance.setdefault(key, []).append(event)

    def add_edge(
        self,
        a: EpisodeNode,
        b: EpisodeNode,
        bidirectional: bool = True,
        *,
        provenance: ProvenanceContext | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Add an edge between nodes with provenance.

        Args:
            a (EpisodeNode): Start node.
            b (EpisodeNode): End node.
            bidirectional (bool): If True, adds both directions.
            provenance (ProvenanceContext | None): Context for the addition.
            details (dict[str, Any] | None): Additional details for the event.
        """
        existed = self.has_edge(a, b)
        super().add_edge(a, b, bidirectional=bidirectional)
        self._record_event(
            "add",
            a,
            b,
            provenance,
            effective=not existed,
            details=details,
        )

    def remove_edge(
        self,
        a: EpisodeNode,
        b: EpisodeNode,
        *,
        provenance: ProvenanceContext | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Remove an edge between `a` and `b` if present (both directions).

        Args:
            a (EpisodeNode): Start node.
            b (EpisodeNode): End node.
            provenance (ProvenanceContext | None): Context for the removal.
            details (dict[str, Any] | None): Additional details for the event.
        """
        existed = self.has_edge(a, b)
        super().remove_edge(a, b)
        self._record_event(
            "remove",
            a,
            b,
            provenance,
            effective=existed,
            details=details,
        )

    @contextmanager
    def provenance_context(self, context: ProvenanceContext) -> Iterator[None]:
        """Temporarily set a default provenance context."""
        prior = self._provenance_context
        self._provenance_context = context
        try:
            yield
        finally:
            self._provenance_context = prior

    def iter_edges(self) -> list[tuple[EpisodeNode, EpisodeNode]]:
        """Return unique undirected edges for this graph.

        Returns:
            list[tuple[EpisodeNode, EpisodeNode]]: List of unique edges.
        """
        seen: set[tuple[EpisodeNode, EpisodeNode]] = set()
        edges: list[tuple[EpisodeNode, EpisodeNode]] = []
        for node in sorted(self.nodes(), key=self._node_key):
            for neighbor in sorted(self.neighbors(node), key=self._node_key):
                key = self._edge_key(node, neighbor)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(key)
        return edges

    def add_graph(
        self,
        other: _BaseGraph[EpisodeNode],
        *,
        provenance: ProvenanceContext | None = None,
    ) -> None:
        """Merge another graph's edges into this graph with provenance.

        Args:
            other (_BaseGraph[EpisodeNode]): Graph to merge.
            provenance (ProvenanceContext | None): Context for the additions.
        """
        if isinstance(other, EpisodeMappingGraph):
            for source_node, target_node in other.iter_edges():
                self.add_edge(
                    source_node,
                    target_node,
                    bidirectional=True,
                    provenance=provenance,
                )
            return
        for node in other.nodes():
            self._ensure_node(node)
        for node in other.nodes():
            for neighbor in other.neighbors(node):
                self.add_edge(
                    node,
                    neighbor,
                    bidirectional=False,
                    provenance=provenance,
                )

    def provenance_items(
        self,
    ) -> list[tuple[EpisodeNode, EpisodeNode, list[ProvenanceEvent]]]:
        """Return provenance entries for all observed edges.

        Returns:
            list[tuple[EpisodeNode, EpisodeNode, list[ProvenanceEvent]]]: List of edges
                with events.
        """
        items: list[tuple[EpisodeNode, EpisodeNode, list[ProvenanceEvent]]] = []
        seen: set[tuple[EpisodeNode, EpisodeNode]] = set()
        for edge in self.iter_edges():
            if edge in self._provenance:
                items.append((edge[0], edge[1], list(self._provenance[edge])))
                seen.add(edge)
        for edge, events in self._provenance.items():
            if edge in seen:
                continue
            items.append((edge[0], edge[1], list(events)))
        items.sort(key=lambda item: (self._node_key(item[0]), self._node_key(item[1])))
        return items

    def add_transitive_edges(
        self, *, provenance: ProvenanceContext | None = None
    ) -> int:
        """Add edges between all nodes in each connected component.

        Returns:
            int: Number of new edges added.
        """
        visited: set[EpisodeNode] = set()
        added = 0
        for node in self.nodes():
            if node in visited:
                continue
            component = self.get_component(node)
            visited.update(component)
            if len(component) < 2:
                continue
            nodes = sorted(component, key=self._node_key)
            for idx, source in enumerate(nodes):
                for target in nodes[idx + 1 :]:
                    if target in self._adj.get(source, set()):
                        continue
                    if any(c in (",", "|") for c in source[3]) or any(
                        c in (",", "|") for c in target[3]
                    ):
                        # Complex range, skip creating a transitive edge
                        continue
                    self.add_edge(
                        source,
                        target,
                        bidirectional=True,
                        provenance=provenance,
                    )
                    added += 1
        return added

    def get_component_by_provider(
        self, start: EpisodeNode
    ) -> dict[str, dict[str, dict[str | None, set[str]]]]:
        """Get the connected component grouped by provider -> entry -> scope.

        Args:
            start (EpisodeNode): Node to start traversal from.

        Returns:
            dict[str, dict[str, dict[str | None, set[str]]]]: Grouped mappings.
        """
        component = self.get_component(start)
        grouped: dict[str, dict[str, dict[str | None, set[str]]]] = {}
        for provider, entry_id, scope, episode_range in component:
            entry_group = grouped.setdefault(provider, {})
            scope_group = entry_group.setdefault(entry_id, {})
            scope_group.setdefault(scope, set()).add(episode_range)
        return grouped
