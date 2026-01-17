"""Graph implementation to store and query mappings."""

from collections import deque
from collections.abc import Iterable
from typing import TypeVar

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
