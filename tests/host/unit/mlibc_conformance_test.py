#!/usr/bin/env python3
"""Host tests for the WOS mlibc conformance contract and runner."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from argparse import Namespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts" / "test" / "mlibc_conformance.py"
SPEC = importlib.util.spec_from_file_location("mlibc_conformance", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
CONFORMANCE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONFORMANCE
SPEC.loader.exec_module(CONFORMANCE)

MANIFEST_PATH = ROOT / "tests" / "conformance" / "mlibc" / "baseline-v1.json"
MLIBC_ROOT = ROOT / "toolchain" / "src" / "mlibc"


def expect_manifest_error(action, needle: str) -> None:
    try:
        action()
    except CONFORMANCE.ManifestError as exc:
        assert needle in str(exc), (needle, str(exc))
    else:
        raise AssertionError(f"expected ManifestError containing {needle!r}")


def write_manifest(path: Path, raw: dict) -> None:
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")


def test_baseline_and_coverage() -> None:
    manifest = CONFORMANCE.load_manifest(MANIFEST_PATH)
    CONFORMANCE.validate_corpus_coverage(manifest, MLIBC_ROOT)
    CONFORMANCE.validate_selection_policy(manifest, MLIBC_ROOT)
    assert manifest.schema_version == 1
    assert manifest.baseline_id == "wos-mlibc-v1"
    assert len(manifest.cases) == 187
    assert tuple(port.identifier for port in manifest.port_smokes) == CONFORMANCE.PORT_IDS
    assert {case.expected for case in manifest.cases} == {"pass", "unsupported"}


def test_ambiguous_and_unsafe_manifests() -> None:
    raw = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="wos-mlibc-manifest-") as directory:
        temp = Path(directory)

        duplicate = temp / "duplicate.json"
        duplicate.write_text('{"schema_version": 1, "schema_version": 1}\n', encoding="utf-8")
        expect_manifest_error(lambda: CONFORMANCE.load_manifest(duplicate), "duplicate JSON key")

        no_rationale = json.loads(json.dumps(raw))
        unsupported = next(case for case in no_rationale["cases"] if case["expected"] == "unsupported")
        unsupported.pop("rationale")
        no_rationale_path = temp / "no-rationale.json"
        write_manifest(no_rationale_path, no_rationale)
        expect_manifest_error(lambda: CONFORMANCE.load_manifest(no_rationale_path), "require a rationale")

        traversal = json.loads(json.dumps(raw))
        supported = next(case for case in traversal["cases"] if case["expected"] == "pass")
        supported["artifact"] = "../escape"
        traversal_path = temp / "traversal.json"
        write_manifest(traversal_path, traversal)
        expect_manifest_error(lambda: CONFORMANCE.load_manifest(traversal_path), "normalized relative path")

        missing = json.loads(json.dumps(raw))
        missing["cases"].pop(0)
        missing_path = temp / "missing.json"
        write_manifest(missing_path, missing)
        parsed = CONFORMANCE.load_manifest(missing_path)
        expect_manifest_error(
            lambda: CONFORMANCE.validate_corpus_coverage(parsed, MLIBC_ROOT),
            "manifest corpus coverage mismatch",
        )

        unexplained = json.loads(json.dumps(raw))
        active_unsupported = next(case for case in unexplained["cases"] if case["id"] == "posix/getparam")
        active_unsupported["prerequisites"] = ["mlibc-option:posix", "runtime:wos"]
        unexplained_path = temp / "unexplained.json"
        write_manifest(unexplained_path, unexplained)
        parsed = CONFORMANCE.load_manifest(unexplained_path)
        expect_manifest_error(
            lambda: CONFORMANCE.validate_selection_policy(parsed, MLIBC_ROOT),
            "lacks an objective missing prerequisite",
        )

        reserved_status = json.loads(json.dumps(raw))
        supported = next(case for case in reserved_status["cases"] if case["expected"] == "pass")
        supported["invocations"] = [{"name": "bad-status", "accepted_returncodes": [124]}]
        reserved_status_path = temp / "reserved-status.json"
        write_manifest(reserved_status_path, reserved_status)
        expect_manifest_error(
            lambda: CONFORMANCE.load_manifest(reserved_status_path),
            "reserved for timeout or infrastructure failures",
        )


def test_result_classification() -> None:
    RawRun = CONFORMANCE.RawRun
    assert CONFORMANCE.classify_run(RawRun(0, b"ok\n", b"", 1)) == ("pass", None)
    assert CONFORMANCE.classify_run(RawRun(0, b"bad\n", b"", 1), "ok\n")[0] == "assertion_failure"
    assert CONFORMANCE.classify_run(RawRun(134, b"", b"Assertion failed", 1))[0] == "assertion_failure"
    assert CONFORMANCE.classify_run(RawRun(134, b"", b"", 1), accepted_returncodes=(134,))[0] == "pass"
    assert CONFORMANCE.classify_run(RawRun(139, b"", b"", 1))[0] == "crash"
    assert CONFORMANCE.classify_run(RawRun(124, b"", b"", 1))[0] == "timeout"
    assert CONFORMANCE.classify_run(RawRun(255, b"", b"", 1))[0] == "infrastructure_failure"
    assert CONFORMANCE.classify_run(RawRun(None, b"", b"", 0, spawn_error="missing"))[0] == "infrastructure_failure"
    assert CONFORMANCE.classify_run(RawRun(-9, b"", b"", 1))[0] == "crash"


def test_bounded_process_contract() -> None:
    passed = CONFORMANCE.run_bounded([sys.executable, "-c", "print('bounded')"], 5, 1024)
    assert passed.returncode == 0
    assert passed.stdout == b"bounded\n"

    timed_out = CONFORMANCE.run_bounded([sys.executable, "-c", "import time; time.sleep(5)"], 1, 1024)
    assert timed_out.timed_out
    assert CONFORMANCE.classify_run(timed_out)[0] == "timeout"

    limited = CONFORMANCE.run_bounded(
        [sys.executable, "-c", "import sys; sys.stdout.write('x' * 4096)"],
        5,
        1024,
    )
    assert limited.output_limited
    assert len(limited.stdout) <= 1024
    assert CONFORMANCE.classify_run(limited)[0] == "infrastructure_failure"


def test_json_and_junit_run() -> None:
    manifest = CONFORMANCE.load_manifest(MANIFEST_PATH)
    with tempfile.TemporaryDirectory(prefix="wos-mlibc-runner-") as directory:
        temp = Path(directory)
        fake_ssh = temp / "fake-ssh"
        fake_ssh.write_text("#!/bin/sh\nprintf 'target-pass\\n'\n", encoding="utf-8")
        fake_ssh.chmod(0o755)
        output = temp / "results"
        args = Namespace(
            suite=[],
            case=["ansi/abs"],
            ssh_command=fake_ssh,
            target="fixture",
            remote_runner="/fixture/run-case",
            transport_grace_seconds=0,
            output_dir=output,
            keep_going=True,
        )
        assert CONFORMANCE.run_cases(args, manifest) == 0
        document = json.loads((output / "results.json").read_text(encoding="utf-8"))
        assert document["summary"]["pass"] == 1
        assert document["summary"]["unsupported"] == 0
        assert document["cases"][0]["status"] == "pass"
        assert isinstance(document["suite_identity"]["repository_dirty"], bool)
        assert isinstance(document["suite_identity"]["mlibc_dirty"], bool)

        suite = ET.parse(output / "results.xml").getroot()
        assert suite.attrib["tests"] == "1"
        assert suite.attrib["failures"] == "0"
        assert suite.find("testcase") is not None
        properties = {
            node.attrib["name"]: node.attrib["value"]
            for node in suite.findall("./properties/property")
        }
        assert properties["manifest_sha256"] == manifest.digest
        assert properties["target"] == "fixture"
        assert properties["repository_dirty"] in {"true", "false"}
        assert properties["mlibc_dirty"] in {"true", "false"}


def test_staging_and_fixed_dispatch() -> None:
    manifest = CONFORMANCE.load_manifest(MANIFEST_PATH)
    with tempfile.TemporaryDirectory(prefix="wos-mlibc-stage-") as directory:
        temp = Path(directory)
        build = temp / "mlibc-build"
        build.mkdir()
        (build / "build.ninja").write_text("# fixture\n", encoding="utf-8")
        for case in manifest.cases:
            if case.expected != "pass":
                continue
            source = CONFORMANCE._build_artifact_path(build, case)
            source.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2("/bin/true", source)
        output = temp / "stage"
        fake_localedef = temp / "fake-localedef"
        fake_localedef.write_text(
            "#!/bin/sh\n"
            "prefix=\n"
            "output=\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  case $1 in\n"
            "    --prefix) prefix=$2; shift 2 ;;\n"
            "    -i|-f) shift 2 ;;\n"
            "    --no-archive) shift ;;\n"
            "    *) output=$1; shift ;;\n"
            "  esac\n"
            "done\n"
            "mkdir -p \"$prefix/usr/lib/locale/$output\"\n"
            "printf '%s\\n' \"$output\" > \"$prefix/usr/lib/locale/$output/LC_CTYPE\"\n",
            encoding="utf-8",
        )
        fake_localedef.chmod(0o755)
        args = Namespace(
            build_dir=build,
            output_dir=output,
            remote_root=CONFORMANCE.DEFAULT_REMOTE_ROOT,
            localedef=str(fake_localedef),
        )
        assert CONFORMANCE.stage_cases(args, manifest) == 0
        dispatcher = output / "run-case"
        assert dispatcher.stat().st_mode & 0o111
        text = dispatcher.read_text(encoding="utf-8")
        assert "eval " not in text
        assert "ansi/abs:0)" in text
        assert "port/git:0)" in text
        assert "/usr/bin/timeout 10" in text
        assert "LANG=en_US.UTF-8" in text
        assert "LOCPATH=/usr/libexec/wos-mlibc-conformance/locales" in text
        locale_names = {path.parent.name for path in (output / "locales").glob("*/LC_CTYPE")}
        assert {"C.UTF-8", "en_US.UTF-8", "en_US", "de_DE.UTF-8", "de_DE", "ru_RU.UTF-8", "ru_RU"} <= locale_names
        assert subprocess.run(["sh", "-n", str(dispatcher)], check=False).returncode == 0
        build_results = json.loads((output / "build-results.json").read_text(encoding="utf-8"))
        statuses = [item["status"] for item in build_results["cases"]]
        assert statuses.count("pass") == 159
        assert statuses.count("unsupported") == 28


def test_cli_validation() -> None:
    result = subprocess.run(
        [sys.executable, str(MODULE_PATH), "validate"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "187 corpus cases" in result.stdout


def main() -> None:
    test_baseline_and_coverage()
    test_ambiguous_and_unsafe_manifests()
    test_result_classification()
    test_bounded_process_contract()
    test_json_and_junit_run()
    test_staging_and_fixed_dispatch()
    test_cli_validation()
    print("mlibc conformance host tests passed")


if __name__ == "__main__":
    main()
