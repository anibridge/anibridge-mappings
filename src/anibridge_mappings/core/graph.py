"""Graph implementation to store and query mappings."""

from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any


class _BaseGraph[NodeT]:
    """Lightweight undirected graph."""

    def __init__(self) -> None:
        """Initialize empty adjacency map."""
        self._adj: dict[NodeT, set[NodeT]] = {}

    def _ensure_node(self, node: NodeT) -> None:
        """Ensure a node exists in the adjacency map."""
        if node not in self._adj:
            self._adj[node] = set()

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
        if bidirectional:
            self._adj[b].add(a)

    def has_edge(self, a: NodeT, b: NodeT) -> bool:
        """Check if an edge exists between `a` and `b`."""
        return b in self._adj.get(a, set()) or a in self._adj.get(b, set())

    def add_equivalence_class(self, nodes: Iterable[NodeT]) -> None:
        """Add an undirected equivalence class of nodes."""
        unique = list(dict.fromkeys(nodes))
        if len(unique) <= 1:
            for node in unique:
                self._ensure_node(node)
            return
        base = unique[0]
        for other in unique[1:]:
            self.add_edge(base, other, bidirectional=True)

    def add_graph(self, other: _BaseGraph[NodeT]) -> None:
        """Merge another graph's edges into this graph."""
        for node in other.nodes():
            self._ensure_node(node)
        for node in other.nodes():
            for neighbor in other.neighbors(node):
                self.add_edge(node, neighbor, bidirectional=False)

    def has_node(self, node: NodeT) -> bool:
        """Check if a node exists in the graph."""
        return node in self._adj

    def neighbors(self, node: NodeT) -> set[NodeT]:
        """Return the neighbor set for a node."""
        return self._adj.get(node, set()).copy()

    def remove_edge(self, a: NodeT, b: NodeT) -> None:
        """Remove an edge between `a` and `b` if present (both directions)."""
        if a in self._adj:
            self._adj[a].discard(b)
        if b in self._adj:
            self._adj[b].discard(a)

    def get_component(self, start: NodeT) -> set[NodeT]:
        """Return the connected component containing `start`."""
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
        """Return the total number of nodes in the graph."""
        return len(self._adj)

    def nodes(self) -> set[NodeT]:
        """Return all nodes in the graph."""
        return set(self._adj)

    def remove_node(self, node: NodeT) -> None:
        """Remove a node and all incident edges."""
        if node not in self._adj:
            return
        for neighbor in self._adj[node]:
            self._adj[neighbor].discard(node)
        del self._adj[node]


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
        return (provider, entry_id, scope or "", episode_range)

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
        """Return unique undirected edges for this graph."""
        seen: set[tuple[EpisodeNode, EpisodeNode]] = set()
        for node in self.nodes():
            for neighbor in self._adj.get(node, set()):
                seen.add(self._edge_key(node, neighbor))
        return sorted(seen, key=lambda e: (self._node_key(e[0]), self._node_key(e[1])))

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
        """Return provenance entries for all observed edges."""
        return sorted(
            ((a, b, list(events)) for (a, b), events in self._provenance.items()),
            key=lambda item: (self._node_key(item[0]), self._node_key(item[1])),
        )

    def is_edit_edge(self, a: EpisodeNode, b: EpisodeNode) -> bool:
        """Return True when the active edge state was added by an edit."""
        if not self.has_edge(a, b):
            return False

        edit = False
        for event in self._provenance.get(self._edge_key(a, b), []):
            if not event.effective:
                continue
            if event.action == "remove":
                edit = False
                continue
            edit = bool(event.details and event.details.get("edit"))
        return edit

    def add_transitive_edges(
        self,
        *,
        provenance: ProvenanceContext | None = None,
        blocked_scope_pairs: set[
            tuple[tuple[str, str, str | None], tuple[str, str, str | None]]
        ]
        | None = None,
    ) -> int:
        """Add edges between all nodes in each connected component.

        Args:
            provenance: Context for the additions.
            blocked_scope_pairs: Optional set of (source_scope, target_scope) tuples.
                Edges between nodes matching these scope pairs will be skipped.

        Returns:
            int: Number of new edges added.
        """
        existing_scope_pairs = self._build_scope_pair_index()
        scope_provider_entries = self._build_scope_provider_entry_index(
            existing_scope_pairs
        )

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
                src_scope = source[:3]
                for target in nodes[idx + 1 :]:
                    if source[0] == target[0]:
                        continue
                    if target in self._adj.get(source, set()):
                        continue
                    if any(c in (",", "|") for c in source[3] + target[3]):
                        continue
                    tgt_scope = target[:3]
                    pair = (src_scope, tgt_scope)
                    rpair = (tgt_scope, src_scope)
                    if blocked_scope_pairs and (
                        pair in blocked_scope_pairs or rpair in blocked_scope_pairs
                    ):
                        continue
                    if pair in existing_scope_pairs or rpair in existing_scope_pairs:
                        continue
                    if self._would_conflict_provider_entry(
                        src_scope, target, scope_provider_entries
                    ) or self._would_conflict_provider_entry(
                        tgt_scope, source, scope_provider_entries
                    ):
                        continue
                    self.add_edge(
                        source,
                        target,
                        bidirectional=True,
                        provenance=provenance,
                    )
                    added += 1
        return added

    def _build_scope_pair_index(
        self,
    ) -> set[tuple[tuple[str, str, str | None], tuple[str, str, str | None]]]:
        """Return scope pairs that already have at least one direct edge."""
        pairs: set[tuple[tuple[str, str, str | None], tuple[str, str, str | None]]] = (
            set()
        )
        for node in self.nodes():
            src_scope = node[:3]
            for neighbor in self._adj.get(node, set()):
                tgt_scope = neighbor[:3]
                if src_scope != tgt_scope:
                    pairs.add((src_scope, tgt_scope))
        return pairs

    @staticmethod
    def _build_scope_provider_entry_index(
        existing_scope_pairs: set[
            tuple[tuple[str, str, str | None], tuple[str, str, str | None]]
        ],
    ) -> dict[tuple[tuple[str, str, str | None], str], set[str]]:
        """Map (source_scope, target_provider) to the set of target entry IDs."""
        index: dict[tuple[tuple[str, str, str | None], str], set[str]] = {}
        for src_scope, tgt_scope in existing_scope_pairs:
            index.setdefault((src_scope, tgt_scope[0]), set()).add(tgt_scope[1])
            index.setdefault((tgt_scope, src_scope[0]), set()).add(src_scope[1])
        return index

    @staticmethod
    def _would_conflict_provider_entry(
        scope: tuple[str, str, str | None],
        candidate: EpisodeNode,
        index: dict[tuple[tuple[str, str, str | None], str], set[str]],
    ) -> bool:
        """Return True when adding candidate would introduce a cross-ID conflict."""
        key = (scope, candidate[0])
        existing = index.get(key)
        return existing is not None and candidate[1] not in existing

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
