#!/usr/bin/env python3
"""Self-tests for the semantic invariant inventory and mutation harness."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "test" / "semantic_regression.py"


def load_module():
    spec = importlib.util.spec_from_file_location("semantic_regression", SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def expect_error(callback, fragment: str) -> None:
    try:
        callback()
    except Exception as exc:  # noqa: BLE001 - dependency-free harness self-test
        if fragment not in str(exc):
            raise AssertionError(f"expected {fragment!r} in {exc!r}") from exc
        return
    raise AssertionError(f"expected an error containing {fragment!r}")


def test_duplicate_json_keys_fail_closed(module) -> None:
    with tempfile.TemporaryDirectory(prefix="semantic-json-") as temporary:
        path = Path(temporary) / "duplicate.json"
        path.write_text('{"schemaVersion": 1, "schemaVersion": 2}\n')
        expect_error(lambda: module.load_json(path), "duplicate JSON key")


def test_ast_inventory_has_stable_assertion_clauses_and_reachability(module) -> None:
    with tempfile.TemporaryDirectory(prefix="semantic-ast-") as temporary:
        root = Path(temporary)
        path = root / "tests" / "host" / "unit" / "sample_source_test.py"
        path.parent.mkdir(parents=True)
        path.write_text(
            "def fail(message):\n"
            "    raise AssertionError(message)\n\n"
            "def test_contract():\n"
            "    assert 1 + 1 == 2, 'arithmetic invariant'\n"
            "    if False:\n"
            "        fail('guarded invariant')\n\n"
            "if __name__ == '__main__':\n"
            "    test_contract()\n"
        )
        claims, unreachable = module.parse_source_claims(root, path)
        if unreachable:
            raise AssertionError(f"reachable test was rejected: {unreachable}")
        selectors = [claim.selector for claim in claims]
        if len(selectors) < 3 or len(selectors) != len(set(selectors)):
            raise AssertionError(f"assertion inventory is incomplete or unstable: {selectors}")
        if not any("arithmetic invariant" in claim.claim for claim in claims):
            raise AssertionError("literal invariant description was not retained")


def test_uninvoked_standalone_test_is_a_silent_skip(module) -> None:
    with tempfile.TemporaryDirectory(prefix="semantic-skip-") as temporary:
        root = Path(temporary)
        path = root / "orphan_source_test.py"
        path.write_text("def test_never_called():\n    assert True\n")
        _, unreachable = module.parse_source_claims(root, path)
        if unreachable != ["test_never_called"]:
            raise AssertionError(f"silent skip was not detected: {unreachable}")


def test_exact_mutation_rejects_stale_operator_and_restores_by_value(module) -> None:
    with tempfile.TemporaryDirectory(prefix="semantic-mutate-") as temporary:
        path = Path(temporary) / "model.cpp"
        original = "bool allowed = generation == expected;\n"
        path.write_text(original)
        saved = module.apply_exact_mutation(path, "generation == expected", "true")
        if saved != original or path.read_text() == original:
            raise AssertionError("exact mutation did not apply in the disposable file")
        path.write_text(saved)
        if path.read_text() != original:
            raise AssertionError("disposable mutation did not restore exactly")
        expect_error(lambda: module.apply_exact_mutation(path, "missing token", "false"), "expected 1 matches")


def test_mutation_source_copy_preserves_dirty_input(module) -> None:
    with tempfile.TemporaryDirectory(prefix="semantic-copy-") as temporary:
        root = Path(temporary) / "root"
        destination = Path(temporary) / "copy"
        for name in ("tests", "modules", "shared"):
            (root / name).mkdir(parents=True)
        dirty = root / "modules" / "dirty.cpp"
        dirty.write_text("uncommitted current source\n")
        module.copy_mutation_sources(root, destination)
        copied = destination / "modules" / "dirty.cpp"
        copied.write_text("mutated copy\n")
        if dirty.read_text() != "uncommitted current source\n":
            raise AssertionError("mutation source copy modified the dirty input tree")


def test_repository_inventory_is_complete_and_mutants_are_fresh(module) -> None:
    inventory = module.audit(ROOT, module.POLICY_PATH, module.MUTATIONS_PATH, None)
    summary = inventory["summary"]
    if summary["sourceFiles"] < 100 or summary["assertionClauses"] < summary["sourceFiles"]:
        raise AssertionError(f"source assertion inventory is unexpectedly small: {summary}")
    if set(summary["mutationDomains"]) != {"scheduler", "mm", "vfs", "network", "wki", "block", "device-lifetime"}:
        raise AssertionError(f"representative mutation domains are incomplete: {summary}")
    critical = [claim for claim in inventory["claims"] if claim["risk"] == "critical"]
    if not critical or any(not claim["replacementTests"] or not claim["mutationIds"] for claim in critical):
        raise AssertionError("critical source-only claim escaped the inventory gate")


def main() -> None:
    module = load_module()
    tests = [
        test_duplicate_json_keys_fail_closed,
        test_ast_inventory_has_stable_assertion_clauses_and_reachability,
        test_uninvoked_standalone_test_is_a_silent_skip,
        test_exact_mutation_rejects_stale_operator_and_restores_by_value,
        test_mutation_source_copy_preserves_dirty_input,
        test_repository_inventory_is_complete_and_mutants_are_fresh,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} semantic regression harness tests passed")


if __name__ == "__main__":
    main()
