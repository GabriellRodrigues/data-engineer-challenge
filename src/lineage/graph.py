import re
from collections.abc import Iterable
from dataclasses import dataclass, field

CRITICAL_PROPERTY = "critical"


@dataclass(frozen=True)
class Node:
    layer: str
    name: str
    critical: bool = False
    published: bool = True

    @property
    def id(self) -> str:
        return f"{self.layer}.{self.name}"


@dataclass
class Lineage:
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: set[tuple[str, str]] = field(default_factory=set)

    def add(self, node: Node) -> None:
        self.nodes[node.id] = node

    def link(self, upstream: str, downstream: str) -> None:
        for node_id in (upstream, downstream):
            if node_id not in self.nodes:
                raise KeyError(f"Unknown lineage node '{node_id}'")
        self.edges.add((upstream, downstream))

    def _walk(self, start: str, forward: bool) -> set[str]:
        if start not in self.nodes:
            raise KeyError(f"Unknown lineage node '{start}'")
        seen, pending = set(), [start]
        while pending:
            current = pending.pop()
            for upstream, downstream in self.edges:
                source, target = (upstream, downstream) if forward else (downstream, upstream)
                if source == current and target not in seen:
                    seen.add(target)
                    pending.append(target)
        return seen

    def upstream(self, node_id: str) -> set[str]:
        return self._walk(node_id, forward=False)

    def downstream(self, node_id: str) -> set[str]:
        return self._walk(node_id, forward=True)

    def critical(self) -> list[str]:
        return sorted(node_id for node_id, node in self.nodes.items() if node.critical)

    def feeds_critical(self) -> set[str]:
        fed = {node_id for critical in self.critical() for node_id in self.upstream(critical)}
        return fed - set(self.critical())

    def impact(self, node_id: str) -> list[str]:
        return sorted(n for n in self.downstream(node_id) if self.nodes[n].critical)


def references(sql: str, names: Iterable[str], prefix: str = "") -> set[str]:
    return {name for name in names if re.search(rf"\b{re.escape(prefix + name)}\b", sql)}


def is_critical(contract: dict) -> bool:
    for prop in contract.get("customProperties") or []:
        if prop.get("property") == CRITICAL_PROPERTY:
            value = prop.get("value")
            if not isinstance(value, bool):
                raise ValueError(
                    f"Contract '{contract.get('name')}': customProperty '{CRITICAL_PROPERTY}' must be true or false"
                )
            return value
    return False


def _mermaid_id(node_id: str) -> str:
    return node_id.replace(".", "__")


def to_mermaid(lineage: Lineage, layers: list[str]) -> str:
    fed = lineage.feeds_critical()
    lines = ["flowchart LR"]
    for layer in layers:
        lines.append(f"    subgraph {layer}")
        for node in sorted((n for n in lineage.nodes.values() if n.layer == layer), key=lambda n: n.name):
            label = node.name if node.published else f"{node.name} (temp)"
            lines.append(f'        {_mermaid_id(node.id)}["{label}"]')
        lines.append("    end")
    for upstream, downstream in sorted(lineage.edges):
        lines.append(f"    {_mermaid_id(upstream)} --> {_mermaid_id(downstream)}")
    lines += [
        "    classDef critical fill:#f8d7da,stroke:#b02a37,stroke-width:2px,color:#000",
        "    classDef feeds fill:#fff3cd,stroke:#997404,color:#000",
        "    classDef temp stroke-dasharray:4 3",
    ]
    for css, members in (
        ("critical", lineage.critical()),
        ("feeds", sorted(fed)),
        ("temp", sorted(n for n, node in lineage.nodes.items() if not node.published)),
    ):
        if members:
            lines.append(f"    class {','.join(_mermaid_id(m) for m in members)} {css}")
    return "\n".join(lines)
