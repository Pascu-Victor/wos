#!/usr/bin/env python3
"""Inventory source-shape claims and run bounded semantic mutation checks.

The mutation runner never edits the checkout.  It copies the current source
files (including uncommitted changes) into a temporary workspace, applies one
exact-count mutation there, builds its focused host oracle, and requires the
mutant to build successfully and fail semantically.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "configs" / "testing" / "semantic-invariants.v1.json"
MUTATIONS_PATH = ROOT / "configs" / "testing" / "semantic-mutations.v1.json"
HOST_CMAKE = ROOT / "tests" / "host" / "CMakeLists.txt"
KTEST_DIR = ROOT / "modules" / "kern" / "src" / "test"

KTEST_RE = re.compile(
    r"\b(?:KTEST|KTEST_OFF)\(\s*(?P<suite>[A-Za-z_][A-Za-z0-9_]*)\s*,\s*"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\)"
)
HOST_TARGET_RE = re.compile(
    r"(?:\bNAME\s+|\bwos_add_unit_test\(|\bwos_add_fuzz_target\()"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)
CLASSIFICATIONS = {"implementation-shape-debt", "abi", "layout", "schema", "policy"}
RISKS = {"critical", "high", "medium", "low"}
MIGRATION_STATES = {"planned", "replacement-added", "mutant-killed", "retained-static", "retired"}
SEMANTIC_TIERS = {"host-unit", "host-model", "host-shim", "fuzz", "ktest", "testd", "multi-node", "hardware-user"}


class SemanticRegressionError(RuntimeError):
    pass


def reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SemanticRegressionError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(), object_pairs_hook=reject_duplicate_pairs)
    except (OSError, json.JSONDecodeError) as exc:
        raise SemanticRegressionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SemanticRegressionError(f"{path} must contain a JSON object")
    return value


def require_keys(value: dict[str, Any], required: set[str], allowed: set[str], where: str) -> None:
    missing = sorted(required - value.keys())
    unknown = sorted(value.keys() - allowed)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if unknown:
            details.append(f"unknown {', '.join(unknown)}")
        raise SemanticRegressionError(f"{where}: {'; '.join(details)}")


def string_list(value: Any, where: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise SemanticRegressionError(f"{where} must be a list of non-empty strings")
    if not value and not allow_empty:
        raise SemanticRegressionError(f"{where} must not be empty")
    return value


def normalized_fingerprint(node: ast.AST) -> str:
    normalized = ast.dump(node, annotate_fields=True, include_attributes=False)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def callable_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def literal_claim(node: ast.AST, fallback: str) -> str:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str) and child.value.strip():
            return " ".join(child.value.split())[:240]
    return fallback.replace("_", " ")


@dataclass(frozen=True)
class SourceClaim:
    path: str
    symbol: str
    kind: str
    fingerprint: str
    ordinal: int
    line: int
    claim: str

    @property
    def selector(self) -> str:
        suffix = f".{self.ordinal}" if self.ordinal else ""
        return f"{self.path}::{self.symbol}::{self.kind}:{self.fingerprint}{suffix}"


class ClaimVisitor(ast.NodeVisitor):
    def __init__(self, relative_path: str) -> None:
        self.relative_path = relative_path
        self.symbols = ["<module>"]
        self.raw: list[tuple[str, str, ast.AST, str]] = []

    @property
    def symbol(self) -> str:
        return self.symbols[-1]

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.symbols.append(node.name)
        self.generic_visit(node)
        self.symbols.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assert(self, node: ast.Assert) -> None:
        self.raw.append((self.symbol, "assert", node.test, literal_claim(node, self.symbol)))
        self.generic_visit(node)

    def visit_Raise(self, node: ast.Raise) -> None:
        if node.exc is not None:
            name = callable_name(node.exc.func) if isinstance(node.exc, ast.Call) else callable_name(node.exc)
            if name in {"AssertionError", "SystemExit"}:
                self.raw.append((self.symbol, "raise", node, literal_claim(node, self.symbol)))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        name = callable_name(node.func)
        if name is not None and (
            name == "fail"
            or name.startswith("require")
            or name.startswith("expect")
            or name.startswith("assert")
            or name.startswith("check_")
        ):
            self.raw.append((self.symbol, f"call-{name}", node, literal_claim(node, self.symbol)))
        self.generic_visit(node)

    def claims(self) -> list[SourceClaim]:
        counts: dict[tuple[str, str, str], int] = {}
        result = []
        for symbol, kind, node, claim in self.raw:
            fingerprint = normalized_fingerprint(node)
            key = (symbol, kind, fingerprint)
            ordinal = counts.get(key, 0)
            counts[key] = ordinal + 1
            result.append(
                SourceClaim(
                    path=self.relative_path,
                    symbol=symbol,
                    kind=kind,
                    fingerprint=fingerprint,
                    ordinal=ordinal,
                    line=getattr(node, "lineno", 0),
                    claim=claim,
                )
            )
        return result


def source_test_paths(root: Path, policy: dict[str, Any]) -> list[Path]:
    discovery = policy.get("discovery")
    if not isinstance(discovery, dict):
        raise SemanticRegressionError("policy.discovery must be an object")
    globs = string_list(discovery.get("globs"), "policy.discovery.globs")
    paths = sorted({path for pattern in globs for path in root.glob(pattern) if path.is_file()})
    if not paths:
        raise SemanticRegressionError("source-test discovery matched no files")
    return paths


def parse_source_claims(root: Path, path: Path) -> tuple[list[SourceClaim], list[str]]:
    relative = path.relative_to(root).as_posix()
    try:
        tree = ast.parse(path.read_text(), filename=relative)
    except (OSError, SyntaxError) as exc:
        raise SemanticRegressionError(f"cannot parse {relative}: {exc}") from exc
    visitor = ClaimVisitor(relative)
    visitor.visit(tree)

    tests = [node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")]
    loaded = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    unreachable = sorted(name for name in tests if name not in loaded)
    return visitor.claims(), unreachable


def validate_rule(rule: dict[str, Any], index: int, tier_names: set[str]) -> None:
    where = f"policy.rules[{index}]"
    allowed = {
        "id",
        "paths",
        "symbols",
        "classification",
        "risk",
        "targetTier",
        "migrationStatus",
        "owner",
        "rationale",
        "replacementTests",
        "mutationIds",
    }
    require_keys(rule, {"id", "paths", "classification", "risk", "targetTier", "migrationStatus", "owner"}, allowed, where)
    string_list(rule["paths"], f"{where}.paths")
    if "symbols" in rule:
        string_list(rule["symbols"], f"{where}.symbols")
    if rule["classification"] not in CLASSIFICATIONS:
        raise SemanticRegressionError(f"{where}.classification is invalid")
    if rule["risk"] not in RISKS:
        raise SemanticRegressionError(f"{where}.risk is invalid")
    if rule["targetTier"] not in tier_names:
        raise SemanticRegressionError(f"{where}.targetTier is unknown")
    if rule["migrationStatus"] not in MIGRATION_STATES:
        raise SemanticRegressionError(f"{where}.migrationStatus is invalid")
    replacements = string_list(rule.get("replacementTests", []), f"{where}.replacementTests", allow_empty=True)
    mutants = string_list(rule.get("mutationIds", []), f"{where}.mutationIds", allow_empty=True)
    if rule["classification"] != "implementation-shape-debt":
        if rule["migrationStatus"] != "retained-static" or not rule.get("rationale"):
            raise SemanticRegressionError(f"{where}: retained static claims require retained-static status and rationale")
    if rule["risk"] == "critical" and rule["classification"] == "implementation-shape-debt":
        if rule["targetTier"] not in SEMANTIC_TIERS or not replacements or not mutants:
            raise SemanticRegressionError(f"{where}: critical implementation debt needs semantic replacements and mutants")


def validate_policy(policy: dict[str, Any]) -> None:
    allowed = {"schemaVersion", "inventoryVersion", "discovery", "defaults", "rules", "tiers", "fixtures", "requiredDomains"}
    require_keys(
        policy,
        {"schemaVersion", "inventoryVersion", "discovery", "defaults", "rules", "tiers", "fixtures", "requiredDomains"},
        allowed,
        "policy",
    )
    if policy["schemaVersion"] != 1 or not isinstance(policy["inventoryVersion"], str):
        raise SemanticRegressionError("unsupported semantic inventory version")
    tiers = policy["tiers"]
    if not isinstance(tiers, dict) or not tiers:
        raise SemanticRegressionError("policy.tiers must be a non-empty object")
    for name, tier in tiers.items():
        if not isinstance(tier, dict):
            raise SemanticRegressionError(f"policy.tiers.{name} must be an object")
        require_keys(tier, {"budgetSeconds", "owner", "evidence"}, {"budgetSeconds", "owner", "evidence"}, f"policy.tiers.{name}")
        if not isinstance(tier["budgetSeconds"], int) or tier["budgetSeconds"] <= 0:
            raise SemanticRegressionError(f"policy.tiers.{name}.budgetSeconds must be positive")
    defaults = policy["defaults"]
    if not isinstance(defaults, dict):
        raise SemanticRegressionError("policy.defaults must be an object")
    validate_rule({"id": "defaults", "paths": ["*"], **defaults}, -1, set(tiers))
    rules = policy["rules"]
    if not isinstance(rules, list) or not rules:
        raise SemanticRegressionError("policy.rules must be a non-empty list")
    identifiers: set[str] = set()
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise SemanticRegressionError(f"policy.rules[{index}] must be an object")
        validate_rule(rule, index, set(tiers))
        if rule["id"] in identifiers:
            raise SemanticRegressionError(f"duplicate policy rule id {rule['id']}")
        identifiers.add(rule["id"])
    required_fixtures = {"allocation-failure", "cancellation", "timeout", "disconnect", "stale-generation", "reorder", "duplicate-completion", "concurrent-teardown"}
    fixtures = policy["fixtures"]
    if not isinstance(fixtures, dict) or required_fixtures - fixtures.keys():
        raise SemanticRegressionError("policy.fixtures does not cover every required reusable fault shape")
    string_list(policy["requiredDomains"], "policy.requiredDomains")


def rule_matches(rule: dict[str, Any], claim: SourceClaim) -> bool:
    if not any(fnmatch.fnmatch(claim.path, pattern) for pattern in rule["paths"]):
        return False
    symbols = rule.get("symbols")
    return symbols is None or any(fnmatch.fnmatch(claim.symbol, pattern) for pattern in symbols)


def resolve_claim(claim: SourceClaim, policy: dict[str, Any]) -> dict[str, Any]:
    merged = dict(policy["defaults"])
    matched = [rule["id"] for rule in policy["rules"] if rule_matches(rule, claim)]
    for rule in policy["rules"]:
        if rule_matches(rule, claim):
            merged.update({key: value for key, value in rule.items() if key not in {"paths", "symbols"}})
    merged.pop("id", None)
    return {
        "selector": claim.selector,
        "path": claim.path,
        "symbol": claim.symbol,
        "kind": claim.kind,
        "fingerprint": claim.fingerprint,
        "line": claim.line,
        "claim": claim.claim,
        "matchedRules": matched,
        **merged,
    }


def registered_host_targets(root: Path) -> set[str]:
    source = (root / "tests" / "host" / "CMakeLists.txt").read_text()
    return {match.group("name") for match in HOST_TARGET_RE.finditer(source)}


def registered_ktests(root: Path) -> set[str]:
    result = set()
    for path in sorted((root / "modules" / "kern" / "src" / "test").glob("*_ktest.cpp")):
        for match in KTEST_RE.finditer(path.read_text()):
            result.add(f"{match.group('suite')}/{match.group('name')}")
    return result


def validate_oracle(root: Path, oracle: str, host_targets: set[str], ktests: set[str]) -> None:
    kind, separator, value = oracle.partition(":")
    if not separator or not value:
        raise SemanticRegressionError(f"invalid replacement oracle {oracle!r}")
    if kind in {"host-unit", "host-model", "host-shim", "fuzz"}:
        target = value.split("/", 1)[0]
        if target not in host_targets:
            raise SemanticRegressionError(f"replacement oracle {oracle!r} names an unregistered host target")
    elif kind == "ktest":
        if value not in ktests:
            raise SemanticRegressionError(f"replacement oracle {oracle!r} names an unknown KTEST")
    elif kind in {"testd", "multi-node", "hardware-user"}:
        if value.startswith("path=") and not (root / value.removeprefix("path=")).exists():
            raise SemanticRegressionError(f"replacement oracle {oracle!r} names a missing path")
    else:
        raise SemanticRegressionError(f"replacement oracle {oracle!r} has an unknown tier")


def load_mutations(root: Path, path: Path) -> dict[str, Any]:
    manifest = load_json(path)
    require_keys(manifest, {"schemaVersion", "manifestVersion", "mutants"}, {"schemaVersion", "manifestVersion", "mutants"}, "mutations")
    if manifest["schemaVersion"] != 1 or not isinstance(manifest["manifestVersion"], str):
        raise SemanticRegressionError("unsupported mutation manifest version")
    mutants = manifest["mutants"]
    if not isinstance(mutants, list) or not mutants:
        raise SemanticRegressionError("mutations.mutants must be a non-empty list")
    allowed = {"id", "domain", "target", "search", "replacement", "expectedCount", "testTarget", "testFilter", "timeoutSeconds", "invariantRules"}
    ids: set[str] = set()
    for index, mutant in enumerate(mutants):
        where = f"mutations.mutants[{index}]"
        if not isinstance(mutant, dict):
            raise SemanticRegressionError(f"{where} must be an object")
        require_keys(mutant, allowed, allowed, where)
        for field in {"id", "domain", "target", "search", "replacement", "testTarget", "testFilter"}:
            if not isinstance(mutant[field], str) or not mutant[field]:
                raise SemanticRegressionError(f"{where}.{field} must be a non-empty string")
        if mutant["id"] in ids:
            raise SemanticRegressionError(f"duplicate mutant id {mutant['id']}")
        ids.add(mutant["id"])
        if mutant["expectedCount"] != 1:
            raise SemanticRegressionError(f"{where}.expectedCount must be exactly 1")
        if not isinstance(mutant["timeoutSeconds"], int) or mutant["timeoutSeconds"] <= 0:
            raise SemanticRegressionError(f"{where}.timeoutSeconds must be positive")
        string_list(mutant["invariantRules"], f"{where}.invariantRules")
        target = root / mutant["target"]
        if not target.is_file():
            raise SemanticRegressionError(f"{where}.target does not exist: {mutant['target']}")
        count = target.read_text().count(mutant["search"])
        if count != mutant["expectedCount"]:
            raise SemanticRegressionError(f"{where} is stale: expected one exact search match, found {count}")
    return manifest


def build_inventory(root: Path, policy: dict[str, Any], mutations: dict[str, Any]) -> dict[str, Any]:
    validate_policy(policy)
    rule_ids = {rule["id"] for rule in policy["rules"]}
    mutation_ids = {entry["id"] for entry in mutations["mutants"]}
    mutation_domains = {entry["domain"] for entry in mutations["mutants"]}
    for entry in mutations["mutants"]:
        stale_rules = sorted(set(entry["invariantRules"]) - rule_ids)
        if stale_rules:
            raise SemanticRegressionError(f"mutant {entry['id']} references stale invariant rules: {', '.join(stale_rules)}")
    missing_domains = sorted(set(policy["requiredDomains"]) - mutation_domains)
    if missing_domains:
        raise SemanticRegressionError(f"mutation manifest misses required domains: {', '.join(missing_domains)}")
    for fixture, paths in policy["fixtures"].items():
        for relative in string_list(paths, f"policy.fixtures.{fixture}"):
            if not (root / relative).is_file():
                raise SemanticRegressionError(f"policy fixture {fixture} names a missing file: {relative}")
    host_targets = registered_host_targets(root)
    ktests = registered_ktests(root)
    claims: list[dict[str, Any]] = []
    unreachable: list[str] = []
    for path in source_test_paths(root, policy):
        discovered, missing_calls = parse_source_claims(root, path)
        unreachable.extend(f"{path.relative_to(root).as_posix()}::{name}" for name in missing_calls)
        claims.extend(resolve_claim(claim, policy) for claim in discovered)
    if unreachable:
        raise SemanticRegressionError("standalone source tests have unreachable test functions: " + ", ".join(unreachable))
    selectors = [claim["selector"] for claim in claims]
    if len(selectors) != len(set(selectors)):
        raise SemanticRegressionError("source-claim selectors are not unique")
    for claim in claims:
        for oracle in claim.get("replacementTests", []):
            validate_oracle(root, oracle, host_targets, ktests)
        stale = sorted(set(claim.get("mutationIds", [])) - mutation_ids)
        if stale:
            raise SemanticRegressionError(f"{claim['selector']} references stale mutants: {', '.join(stale)}")
        if claim["risk"] == "critical" and claim["classification"] == "implementation-shape-debt":
            if not claim.get("replacementTests") or not claim.get("mutationIds"):
                raise SemanticRegressionError(f"critical source-only claim remains: {claim['selector']}")
    referenced_mutations = {mutation for claim in claims for mutation in claim.get("mutationIds", [])}
    orphan_mutations = sorted(mutation_ids - referenced_mutations)
    if orphan_mutations:
        raise SemanticRegressionError(f"mutation manifest has orphan mutants: {', '.join(orphan_mutations)}")
    if not claims:
        raise SemanticRegressionError("source assertion inventory is empty")
    return {
        "schemaVersion": 1,
        "inventoryVersion": policy["inventoryVersion"],
        "generatedFrom": {
            "policy": POLICY_PATH.relative_to(ROOT).as_posix(),
            "mutations": MUTATIONS_PATH.relative_to(ROOT).as_posix(),
        },
        "summary": {
            "sourceFiles": len(source_test_paths(root, policy)),
            "assertionClauses": len(claims),
            "criticalClauses": sum(claim["risk"] == "critical" for claim in claims),
            "retainedStaticClauses": sum(claim["classification"] != "implementation-shape-debt" for claim in claims),
            "mutationDomains": sorted(mutation_domains),
        },
        "tiers": policy["tiers"],
        "fixtures": policy["fixtures"],
        "claims": claims,
    }


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def copy_mutation_sources(root: Path, destination: Path) -> None:
    ignored = shutil.ignore_patterns("__pycache__", "*.pyc", "extern", "build", "build-*", "test-results")
    for name in ("tests", "modules", "shared"):
        source = root / name
        if not source.is_dir():
            raise SemanticRegressionError(f"mutation source tree is missing {name}/")
        shutil.copytree(source, destination / name, symlinks=True, ignore=ignored)


def apply_exact_mutation(path: Path, search: str, replacement: str, expected_count: int = 1) -> str:
    original = path.read_text()
    count = original.count(search)
    if count != expected_count:
        raise SemanticRegressionError(f"stale mutant for {path}: expected {expected_count} matches, found {count}")
    path.write_text(original.replace(search, replacement))
    return original


def run_command(command: list[str], *, timeout: int, cwd: Path, env: dict[str, str], log: Path) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    try:
        result = subprocess.run(command, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        log.write_text(f"$ {' '.join(command)}\n{output}\nTIMEOUT after {timeout}s\n")
        raise SemanticRegressionError(f"command timed out after {timeout}s: {' '.join(command)}") from exc
    elapsed = time.monotonic() - started
    log.write_text(f"$ {' '.join(command)}\n{result.stdout}\nELAPSED_SECONDS={elapsed:.3f}\nRETURN_CODE={result.returncode}\n")
    return result


def find_gtest_source(root: Path, explicit: Path | None) -> Path | None:
    candidates = [explicit] if explicit is not None else []
    environment = os.environ.get("WOS_GTEST_SOURCE")
    if environment:
        candidates.append(Path(environment))
    candidates.extend(
        [
            Path(os.environ.get("WOS_TEST_BUILD_DIR", "/tmp/wos-tests")) / "_deps" / "googletest-src",
            root / "build-tests" / "_deps" / "googletest-src",
        ]
    )
    return next((candidate.resolve() for candidate in candidates if candidate is not None and candidate.is_dir()), None)


def test_binary(build: Path, target: str) -> Path:
    for candidate in (build / "host" / target, build / target):
        if candidate.is_file():
            return candidate
    raise SemanticRegressionError(f"built test binary not found for {target}")


def host_compilers() -> tuple[str, str]:
    system_c = Path("/usr/bin/clang")
    system_cxx = Path("/usr/bin/clang++")
    if system_c.is_file() and os.access(system_c, os.X_OK) and system_cxx.is_file() and os.access(system_cxx, os.X_OK):
        return str(system_c), str(system_cxx)
    compiler = shutil.which("clang")
    compiler_cxx = shutil.which("clang++")
    if compiler is None or compiler_cxx is None:
        raise SemanticRegressionError("no native host clang/clang++ pair is available")
    return compiler, compiler_cxx


def run_mutations(
    root: Path,
    manifest: dict[str, Any],
    selected: set[str],
    report_path: Path,
    keep_workspace: bool,
    gtest_source: Path | None,
) -> dict[str, Any]:
    mutants = [entry for entry in manifest["mutants"] if not selected or entry["id"] in selected or entry["domain"] in selected]
    unknown = selected - {entry["id"] for entry in manifest["mutants"]} - {entry["domain"] for entry in manifest["mutants"]}
    if unknown:
        raise SemanticRegressionError(f"unknown mutant/domain selection: {', '.join(sorted(unknown))}")
    if not mutants:
        raise SemanticRegressionError("no mutants selected")

    temporary = Path(tempfile.mkdtemp(prefix="wos-semantic-mutants-"))
    workspace = temporary / "workspace"
    build = temporary / "build"
    logs = temporary / "logs"
    logs.mkdir(parents=True)
    result: dict[str, Any] = {
        "schemaVersion": 1,
        "manifestVersion": manifest["manifestVersion"],
        "workspaceIsolation": "copy-current-sources",
        "workspace": str(temporary) if keep_workspace else "temporary-redacted",
        "mutants": [],
    }
    env = os.environ.copy()
    compiler, compiler_cxx = host_compilers()
    env.update(
        {
            "ASAN_OPTIONS": "detect_leaks=0",
            "LSAN_OPTIONS": "detect_leaks=0",
            "CC": compiler,
            "CXX": compiler_cxx,
            "CFLAGS": "",
            "CXXFLAGS": "",
            "LDFLAGS": "",
        }
    )
    try:
        copy_mutation_sources(root, workspace)
        configure = [
            "cmake",
            "-S",
            str(workspace / "tests"),
            "-B",
            str(build),
            f"-DCMAKE_C_COMPILER={compiler}",
            f"-DCMAKE_CXX_COMPILER={compiler_cxx}",
            f"-DWOS_ROOT={workspace}",
        ]
        if gtest_source is not None:
            configure.append(f"-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST={gtest_source}")
        configured = run_command(configure, timeout=300, cwd=temporary, env=env, log=logs / "configure.log")
        if configured.returncode != 0:
            tail = configured.stdout[-2000:].strip()
            raise SemanticRegressionError(f"mutation workspace configure failed:\n{tail}")

        for mutant in mutants:
            mutant_id = mutant["id"]
            timeout = mutant["timeoutSeconds"]
            target_path = workspace / mutant["target"]
            build_command = ["cmake", "--build", str(build), "--target", mutant["testTarget"], "-j", "2"]
            baseline_build = run_command(build_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-baseline-build.log")
            if baseline_build.returncode != 0:
                raise SemanticRegressionError(f"baseline build failed for {mutant_id}")
            binary = test_binary(build, mutant["testTarget"])
            test_command = [str(binary), f"--gtest_filter={mutant['testFilter']}"]
            baseline = run_command(test_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-baseline-test.log")
            if baseline.returncode != 0:
                raise SemanticRegressionError(f"baseline oracle failed for {mutant_id}")

            original = apply_exact_mutation(target_path, mutant["search"], mutant["replacement"], mutant["expectedCount"])
            try:
                mutant_build = run_command(build_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-mutant-build.log")
                if mutant_build.returncode != 0:
                    raise SemanticRegressionError(f"mutant {mutant_id} did not compile; compile failures are invalid evidence")
                mutated = run_command(test_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-mutant-test.log")
                if mutated.returncode == 0:
                    raise SemanticRegressionError(f"mutant survived semantic oracle: {mutant_id}")
            finally:
                target_path.write_text(original)

            restored_build = run_command(build_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-restored-build.log")
            if restored_build.returncode != 0:
                raise SemanticRegressionError(f"restored build failed for {mutant_id}")
            restored = run_command(test_command, timeout=timeout, cwd=temporary, env=env, log=logs / f"{mutant_id}-restored-test.log")
            if restored.returncode != 0 or target_path.read_text() != original:
                raise SemanticRegressionError(f"mutation workspace did not restore cleanly for {mutant_id}")
            result["mutants"].append(
                {
                    "id": mutant_id,
                    "domain": mutant["domain"],
                    "target": mutant["target"],
                    "oracle": mutant["testTarget"] + "/" + mutant["testFilter"],
                    "status": "killed",
                    "mutantExitCode": mutated.returncode,
                    "baselineExitCode": baseline.returncode,
                    "restoredExitCode": restored.returncode,
                }
            )
        result["summary"] = {"selected": len(mutants), "killed": len(result["mutants"]), "survived": 0, "invalid": 0}
        write_json_atomic(report_path, result)
        return result
    finally:
        if not keep_workspace:
            shutil.rmtree(temporary, ignore_errors=True)


def audit(root: Path, policy_path: Path, mutations_path: Path, inventory_output: Path | None) -> dict[str, Any]:
    policy = load_json(policy_path)
    manifest = load_mutations(root, mutations_path)
    inventory = build_inventory(root, policy, manifest)
    if inventory_output is not None:
        write_json_atomic(inventory_output, inventory)
    return inventory


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--root", type=Path, default=ROOT)
    result.add_argument("--policy", type=Path, default=POLICY_PATH)
    result.add_argument("--mutations", type=Path, default=MUTATIONS_PATH)
    subparsers = result.add_subparsers(dest="command", required=True)
    audit_parser = subparsers.add_parser("audit", help="validate and optionally materialize the invariant inventory")
    audit_parser.add_argument("--inventory-output", type=Path)
    mutate_parser = subparsers.add_parser("mutate", help="run isolated buildable semantic mutants")
    mutate_parser.add_argument("selectors", nargs="*", help="mutant ids or domains (default: all)")
    mutate_parser.add_argument("--report", type=Path)
    mutate_parser.add_argument("--keep-workspace", action="store_true")
    mutate_parser.add_argument("--gtest-source", type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    root = args.root.resolve()
    try:
        inventory_output = getattr(args, "inventory_output", None)
        inventory = audit(root, args.policy.resolve(), args.mutations.resolve(), inventory_output)
        if args.command == "audit":
            print(json.dumps(inventory["summary"], sort_keys=True))
            return 0
        manifest = load_mutations(root, args.mutations.resolve())
        report = args.report or root / "test-results" / "semantic-regression" / "mutation-report.json"
        result = run_mutations(
            root,
            manifest,
            set(args.selectors),
            report.resolve(),
            args.keep_workspace,
            find_gtest_source(root, args.gtest_source),
        )
        print(json.dumps(result["summary"], sort_keys=True))
        return 0
    except SemanticRegressionError as exc:
        print(f"semantic-regression: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
