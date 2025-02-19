from __future__ import annotations

from collections.abc import Callable, Generator


class Graph[V, E]:
    def __init__(self, root: Node[V, E], default_edge: E) -> None:
        self._root = root
        self._default_edge = default_edge

    @property
    def root(self) -> Node[V, E]:
        return self._root

    def find(self, func: Callable[[Node[V, E]], bool], edge: E | None = None) -> Node[V, E] | None:
        return self.root.find(func, edge or self._default_edge)

    def filter(self, func: Callable[[Node[V, E]], bool], edge: E | None = None) -> Generator[Node[V, E], None, None]:
        return self.root.filter(func, edge or self._default_edge)

    def traverse(self, edge: E | None = None) -> Generator[Node[V, E], None, None]:
        return self.root.traverse(edge or self._default_edge)

    def traverse_with_level(self, edge: E | None = None) -> Generator[tuple[Node[V, E], int], None, None]:
        return self.root.traverse_with_level(edge or self._default_edge)

class Node[V, E]:
    def __init__(self, id: str, value: V) -> None:
        self.id = id
        self.value = value
        self._edges: dict[E, list[Node[V, E]]] = {}

    def __repr__(self) -> str:
        edges = {edge: [n.id for n in nodes] for edge, nodes in self._edges.items()}
        return f"Node({self.id}, {self.value}, {edges})"

    def transition(self, value: V) -> None:
        self.value = value

    def add_edge(self, edge: E, node: Node[V, E]) -> None:
        self._edges.setdefault(edge, []).append(node)

    def rmv_edge(self, edge: E, node: Node[V, E]) -> None:
        self._edges[edge].remove(node)

    def has_edge(self, edge: E, node: Node[V, E]) -> bool:
        return node in self._edges.get(edge, [])

    def get_edge(self, edge: E) -> list[Node[V, E]]:
        return self._edges.get(edge, [])

    def find(self, func: Callable[[Node[V, E]], bool], edge: E) -> Node[V, E] | None:
        for node in self.traverse(edge):
            if func(node):
                return node

        return None

    def filter(self, func: Callable[[Node[V, E]], bool], edge: E) -> Generator[Node[V, E], None, None]:
        for node in self.traverse(edge):
            if func(node):
                yield node

    def traverse(self, edge: E) -> Generator[Node[V, E], None, None]:
        for node, _ in self._traverse(edge):
            yield node

    def traverse_with_level(self, edge: E) -> Generator[tuple[Node[V, E], int], None, None]:
        return self._traverse(edge)

    def _traverse(self, edge: E, visited: set[str] | None = None, level: int = 0) -> Generator[tuple[Node[V, E], int], None, None]:
        if visited is None:
            visited = set()

        if self.id in visited:
            return

        visited.add(self.id)
        yield self, level

        for node in self._edges.get(edge, []):
            yield from node._traverse(edge, visited, level + 1)
