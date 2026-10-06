"""Registry of task definitions (static configuration)."""
from __future__ import annotations

from typing import Iterable

from .models import TaskDefinition


class TaskRegistry:
    def __init__(self) -> None:
        self._defs: dict[str, TaskDefinition] = {}

    def register(self, definition: TaskDefinition) -> None:
        if definition.name in self._defs:
            raise ValueError(f"task {definition.name!r} already registered")
        self._defs[definition.name] = definition

    def get(self, name: str) -> TaskDefinition:
        try:
            return self._defs[name]
        except KeyError:
            raise KeyError(f"unknown task: {name!r}") from None

    def names(self) -> Iterable[str]:
        return tuple(self._defs)

    def validate(self) -> None:
        """Check dependency graph references exist and is acyclic."""
        for d in self._defs.values():
            for dep in d.dependencies:
                if dep not in self._defs:
                    raise ValueError(
                        f"task {d.name!r} depends on unknown task {dep!r}"
                    )
        # cycle detection via iterative DFS coloring
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {name: WHITE for name in self._defs}
        for root in self._defs:
            if color[root] != WHITE:
                continue
            stack = [(root, iter(self._defs[root].dependencies))]
            color[root] = GRAY
            while stack:
                node, it = stack[-1]
                advanced = False
                for dep in it:
                    if color[dep] == GRAY:
                        raise ValueError(
                            f"dependency cycle detected involving {dep!r}"
                        )
                    if color[dep] == WHITE:
                        color[dep] = GRAY
                        stack.append((dep, iter(self._defs[dep].dependencies)))
                        advanced = True
                        break
                if not advanced:
                    color[node] = BLACK
                    stack.pop()
