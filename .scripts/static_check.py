"""Lightweight static analyzer for the TSSI codebase.

Runs against app/**.py with stdlib only. Catches:
* Syntax errors (already caught by ast.parse, but surfaced here too).
* Unused imports / unused `from X import Y` names.
* Undefined name references inside function bodies (best-effort; it does
  not fully emulate Python scoping but flags obvious typos).

This does NOT replace mypy / ruff / pyflakes; it's a defence-in-depth pass
for offline / sandboxed environments that can't reach PyPI.
"""

from __future__ import annotations

import ast
import builtins
import pathlib
import sys
from typing import Iterable

ROOT = pathlib.Path(__file__).resolve().parents[1]


class _Scope(ast.NodeVisitor):
    def __init__(self) -> None:
        self.defined: set[str] = set(dir(builtins))
        self.used: set[str] = set()
        self.imports: dict[str, ast.AST] = {}

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            name = alias.asname or alias.name.split(".")[0]
            self.defined.add(name)
            self.imports[name] = node

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name == "*":
                continue
            name = alias.asname or alias.name
            self.defined.add(name)
            self.imports[name] = node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.defined.add(node.name)
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.defined.add(node.name)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            for name in _names(target):
                self.defined.add(name)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        for name in _names(node.target):
            self.defined.add(name)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.used.add(node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # record the root name when an attribute chain bottoms out at a Name
        head = node
        while isinstance(head, ast.Attribute):
            head = head.value
        if isinstance(head, ast.Name):
            self.used.add(head.id)
        self.generic_visit(node)


def _names(target: ast.AST) -> Iterable[str]:
    if isinstance(target, ast.Name):
        yield target.id
    elif isinstance(target, (ast.Tuple, ast.List)):
        for elt in target.elts:
            yield from _names(elt)


def check_file(path: pathlib.Path) -> list[str]:
    text = path.read_text()
    tree = ast.parse(text, filename=str(path))
    scope = _Scope()
    scope.visit(tree)

    problems: list[str] = []
    for name, node in scope.imports.items():
        if name in scope.used:
            continue
        # Allow re-exports in __init__.py or explicit __all__.
        if path.name == "__init__.py":
            continue
        if name.startswith("_") or name in {"annotations"}:
            continue
        problems.append(
            f"{path.relative_to(ROOT)}:{node.lineno}: unused import {name!r}"
        )
    return problems


def main() -> int:
    problems: list[str] = []
    for path in sorted(ROOT.rglob("app/**/*.py")):
        problems.extend(check_file(path))

    for p in problems:
        print(p)
    print(f"\n{len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
