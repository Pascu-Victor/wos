#!/usr/bin/env python3

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WOSINCIDENT = ROOT / "scripts" / "debug" / "wosincident.py"
WOSINCIDENT_BIN = ROOT / "bin" / "wos-incident"
WOS_SFTP_GET = ROOT / "scripts" / "remote" / "wos_sftp_get.sh"


def load_module():
    cluster_dir = ROOT / "scripts" / "cluster"
    if str(cluster_dir) not in sys.path:
        sys.path.insert(0, str(cluster_dir))
    spec = importlib.util.spec_from_file_location("wosincident", WOSINCIDENT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {WOSINCIDENT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def fixture(tmp: Path) -> tuple[dict, Path, list[dict], Path]:
    config_path = tmp / "configs" / "node_ktest.json"
    serial = tmp / "ktest-data" / "serial-vm0.log"
    qemu = tmp / "ktest-data" / "qemu-vm0.log"
    kernel = tmp / "build-ktest" / "modules" / "kern" / "wos"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    serial.parent.mkdir(parents=True, exist_ok=True)
    kernel.parent.mkdir(parents=True, exist_ok=True)
    serial.write_text(
        "[0.100] booting WOS\n"
        "authorization: Bearer should-not-survive\n"
        "[0.200] selftests complete\n"
    )
    qemu.write_text("qemu guest_errors: none\n")
    kernel.write_bytes(b"ELF-fixture-kernel")
    config = {
        "build": {"dir": "build-ktest"},
        "node": {
            "id": 0,
            "hostname": "wos-ktest",
            "vm": {
                "disk0": "ktest-data/disk.qcow2",
                "disk1": "ktest-data/mountfs.qcow2",
                "overlay_dir": "ktest-data/overlays",
                "serial_log": str(serial),
                "qemu_log": str(qemu),
            },
            "nics": [
                {
                    "name": "lan",
                    "zone_id": 0,
                    "mac": "52:54:00:00:00:00",
                    "model": "virtio-net-pci",
                    "queues": 2,
                    "driver": "unmanaged",
                }
            ],
        },
    }
    config_path.write_text(json.dumps(config))
    return config, config_path, [config["node"]], kernel.parents[2]


def capture(module, tmp: Path, output: Path, *, archive: bool = False, **kwargs):
    config, config_path, nodes, build_dir = fixture(tmp)
    return module.capture_incident(
        output=output,
        archive=archive,
        kind="ktest",
        config=config,
        config_path=config_path,
        node_specs=nodes,
        build_dir=build_dir,
        profile="Debug",
        repo_root=tmp,
        created_utc="2026-08-09T00:00:00Z",
        **kwargs,
    )


def test_public_entrypoint_and_help(module) -> None:
    if not WOSINCIDENT_BIN.is_symlink():
        raise AssertionError("bin/wos-incident must be a symlink")
    assert_equal(
        WOSINCIDENT_BIN.resolve(), WOSINCIDENT.resolve(), "public entrypoint target"
    )
    result = subprocess.run(
        [str(WOSINCIDENT_BIN), "capture", "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert_equal(result.returncode, 0, "wos-incident capture --help")
    for flag in ("--archive", "--coverage-manifest", "--live-coredump"):
        if flag not in result.stdout:
            raise AssertionError(f"public help is missing {flag}")


def test_directory_manifest_is_canonical_bounded_and_redacted(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        output = tmp / "incident.wosincident"
        manifest = capture(module, tmp, output)

        on_disk = json.loads((output / "manifest.json").read_text())
        assert_equal(on_disk, manifest, "returned and persisted manifest")
        assert_equal(manifest["format"], "wosincident", "format")
        assert_equal(manifest["version"], 1, "version")
        assert_equal(
            manifest["limits"],
            {
                "maxMembers": module.DEFAULT_MAX_MEMBERS,
                "maxFileBytes": module.DEFAULT_MAX_FILE_BYTES,
                "maxTotalBytes": module.DEFAULT_MAX_TOTAL_BYTES,
                "maxPathBytes": module.DEFAULT_MAX_PATH_BYTES,
                "maxManifestBytes": module.DEFAULT_MAX_MANIFEST_BYTES,
            },
            "effective capture limits",
        )
        if not manifest["incidentId"].startswith("sha256:"):
            raise AssertionError("incidentId is not content-derived")
        identity_payload = dict(manifest)
        identity_payload.pop("incidentId")
        identity_payload.pop("createdUtc")
        assert_equal(
            manifest["incidentId"],
            "sha256:"
            + hashlib.sha256(module._canonical_json(identity_payload)).hexdigest(),
            "canonical incident identity",
        )
        assert_equal(
            list(manifest["source"]),
            ["kind", "revision", "dirty", "profile", "configMember"],
            "source schema",
        )
        assert_equal(manifest["capture"]["complete"], True, "complete capture")
        if manifest["capture"]["redactions"] < 1:
            raise AssertionError("credential-bearing log line was not redacted")
        paths = [member["path"] for member in manifest["members"]]
        assert_equal(paths, sorted(paths), "member ordering")
        if any(Path(path).is_absolute() or ".." in Path(path).parts for path in paths):
            raise AssertionError(f"unsafe member path: {paths!r}")

        serial_member = next(
            member for member in manifest["members"] if member["kind"] == "serial-log"
        )
        serial_data = (output / serial_member["path"]).read_bytes()
        if b"should-not-survive" in serial_data:
            raise AssertionError("secret text survived redaction")
        config_data = (output / manifest["source"]["configMember"]).read_text()
        if "disk.qcow2" in config_data or "mountfs.qcow2" in config_data:
            raise AssertionError("disk paths survived config sanitization")

        for member in manifest["members"]:
            data = (output / member["path"]).read_bytes()
            assert_equal(len(data), member["size"], f"size for {member['path']}")
            assert_equal(
                hashlib.sha256(data).hexdigest(),
                member["sha256"],
                f"sha256 for {member['path']}",
            )
            if "/" in member["sourceName"]:
                raise AssertionError(f"sourceName leaked a path: {member!r}")


def test_directory_and_ustar_share_manifest_and_archive_is_deterministic(
    module,
) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        directory = tmp / "directory.wosincident"
        archive_a = tmp / "archive-a.wosincident"
        archive_b = tmp / "archive-b.wosincident"
        directory_manifest = capture(module, tmp, directory)
        archive_manifest_a = capture(module, tmp, archive_a, archive=True)
        archive_manifest_b = capture(module, tmp, archive_b, archive=True)

        assert_equal(
            directory_manifest["incidentId"],
            archive_manifest_a["incidentId"],
            "directory/archive incident identity",
        )
        assert_equal(
            archive_manifest_a["incidentId"],
            archive_manifest_b["incidentId"],
            "repeat archive incident identity",
        )
        assert_equal(
            archive_a.read_bytes(), archive_b.read_bytes(), "deterministic USTAR bytes"
        )
        assert_equal(
            stat.S_IMODE(archive_a.stat().st_mode),
            0o600,
            "published USTAR mode",
        )
        with tarfile.open(archive_a, "r:") as archive:
            names = archive.getnames()
            assert_equal(names, sorted(names), "USTAR member ordering")
            if "manifest.json" not in names:
                raise AssertionError("USTAR is missing manifest.json")
            for info in archive.getmembers():
                if not info.isfile():
                    raise AssertionError(f"non-regular archive member: {info.name}")
                assert_equal(info.mtime, 0, f"USTAR mtime for {info.name}")
                assert_equal(info.uid, 0, f"USTAR uid for {info.name}")
                assert_equal(info.gid, 0, f"USTAR gid for {info.name}")
            archive_manifest = json.load(archive.extractfile("manifest.json"))
        assert_equal(archive_manifest, archive_manifest_a, "USTAR manifest")


def test_atomic_publish_refuses_existing_output(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        output = tmp / "existing.wosincident"
        output.write_text("keep me")
        try:
            capture(module, tmp, output)
        except FileExistsError:
            pass
        else:
            raise AssertionError("existing output was overwritten")
        assert_equal(output.read_text(), "keep me", "existing output content")
        leftovers = list(tmp.glob(".existing.wosincident.*"))
        assert_equal(leftovers, [], "failed capture staging cleanup")

        config, config_path, nodes, build_dir = fixture(tmp)
        diagnostics = io.StringIO()
        with contextlib.redirect_stderr(diagnostics):
            result = module.capture_safely(
                output=output,
                archive=False,
                kind="ktest",
                config=config,
                config_path=config_path,
                node_specs=nodes,
                build_dir=build_dir,
                repo_root=tmp,
            )
        assert_equal(result, None, "non-masking safe capture result")
        if "WARNING: incident capture failed" not in diagnostics.getvalue():
            raise AssertionError("safe capture did not report its warning")
        assert_equal(output.read_text(), "keep me", "safe capture output preservation")


def test_snapshot_uses_new_inode_and_appended_range(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        config, config_path, nodes, build_dir = fixture(tmp)
        serial = Path(config["node"]["vm"]["serial_log"])
        snapshots = module.snapshot_node_logs(nodes, tcg_level=None, repo_root=tmp)
        serial.unlink()
        serial.write_text("new-log-same-size-is-not-stale\n")
        replaced_output = tmp / "replaced.wosincident"
        replaced = module.capture_incident(
            output=replaced_output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            snapshots=snapshots,
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        serial_member = next(
            member for member in replaced["members"] if member["kind"] == "serial-log"
        )
        assert_equal(
            (replaced_output / serial_member["path"]).read_text(),
            "new-log-same-size-is-not-stale\n",
            "recreated log capture",
        )

        snapshots = module.snapshot_node_logs(nodes, tcg_level=None, repo_root=tmp)
        with serial.open("a") as output:
            output.write("appended-only\n")
        appended_output = tmp / "appended.wosincident"
        appended = module.capture_incident(
            output=appended_output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            snapshots=snapshots,
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        serial_member = next(
            member for member in appended["members"] if member["kind"] == "serial-log"
        )
        assert_equal(
            (appended_output / serial_member["path"]).read_text(),
            "appended-only\n",
            "same-inode appended range",
        )

        serial.write_bytes(b"A" * 32)
        snapshots = module.snapshot_node_logs(nodes, tcg_level=None, repo_root=tmp)
        serial_snapshot = snapshots[str(serial.resolve())]
        serial.write_bytes(b"B" * 32)
        rewritten_mtime = serial_snapshot.mtime_ns + 1_000_000
        os.utime(serial, ns=(rewritten_mtime, rewritten_mtime))
        rewritten_output = tmp / "rewritten.wosincident"
        rewritten = module.capture_incident(
            output=rewritten_output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            snapshots=snapshots,
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        serial_member = next(
            member for member in rewritten["members"] if member["kind"] == "serial-log"
        )
        assert_equal(
            (rewritten_output / serial_member["path"]).read_bytes(),
            b"B" * 32,
            "same-inode same-size rewrite",
        )


def test_coverage_manifest_materializes_ranges_without_host_paths(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        config, config_path, nodes, build_dir = fixture(tmp)
        coverage_dir = tmp / "test-results" / "coverage"
        coverage_dir.mkdir(parents=True)
        info = coverage_dir / "kcov.info"
        info.write_text("TN:\nSF:kernel.cpp\nDA:1,1\nend_of_record\n")
        disk = coverage_dir / "disk.qcow2"
        disk.write_bytes(b"must never be collected")
        host_key = tmp / "cluster-data" / "ssh-keys" / "host.log"
        host_key.parent.mkdir(parents=True)
        host_key.write_text("must never be collected")
        serial = Path(config["node"]["vm"]["serial_log"])
        coverage_manifest = coverage_dir / "run-all-iteration.json"
        coverage_manifest.write_text(
            json.dumps(
                {
                    "status": "pass",
                    "artifacts": {
                        "disk": str(disk),
                        "host_key": str(host_key),
                        "kcov_lcov": str(info),
                    },
                    "external_log_ranges": [
                        {"path": str(serial), "start_offset": 0, "end_offset": 8}
                    ],
                }
            )
        )
        output = tmp / "coverage.wosincident"
        manifest = module.capture_incident(
            output=output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            coverage_manifests=[coverage_manifest],
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        kinds = {member["kind"] for member in manifest["members"]}
        for expected in ("coverage-manifest", "coverage-artifact", "coverage-log"):
            if expected not in kinds:
                raise AssertionError(f"missing imported {expected}: {kinds!r}")
        imported_manifest = next(
            member
            for member in manifest["members"]
            if member["kind"] == "coverage-manifest"
        )
        imported_text = (output / imported_manifest["path"]).read_text()
        if str(tmp) in imported_text:
            raise AssertionError("coverage manifest leaked its host path")
        source_names = {member["sourceName"] for member in manifest["members"]}
        if disk.name in source_names or host_key.name in source_names:
            raise AssertionError("disk or SSH material entered the member inventory")
        if not any(
            error["code"] == "forbidden" for error in manifest["capture"]["errors"]
        ):
            raise AssertionError("forbidden coverage evidence was not recorded")


def test_coverage_manifest_is_stable_strict_and_has_no_repo_fallback(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        config, config_path, nodes, build_dir = fixture(tmp)
        fallback = tmp / "fallback.log"
        fallback.write_text("wrong same-named repo artifact\n")
        coverage_dir = tmp / "test-results" / "coverage"
        coverage_dir.mkdir(parents=True)
        coverage_manifest = coverage_dir / "run-all-iteration.json"
        coverage_manifest.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "bad_type": 7,
                        "missing_relative": "fallback.log",
                    },
                    "external_log_ranges": {"not": "a list"},
                }
            )
        )
        output = tmp / "strict-coverage.wosincident"
        manifest = module.capture_incident(
            output=output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            coverage_manifests=[coverage_manifest],
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        assert_equal(manifest["capture"]["complete"], False, "strict coverage status")
        codes = {error["code"] for error in manifest["capture"]["errors"]}
        for expected in (
            "invalid-coverage-artifact",
            "invalid-coverage-manifest",
            "missing",
        ):
            if expected not in codes:
                raise AssertionError(f"strict coverage error is missing {expected}: {codes!r}")
        if any(
            member["kind"] in {"coverage-artifact", "coverage-log"}
            for member in manifest["members"]
        ):
            raise AssertionError("relative coverage lookup fell back to the repository")

        stable_staging = tmp / "stable-staging"
        stable_staging.mkdir()
        stable_builder = module.IncidentBuilder(
            stable_staging,
            allowed_roots=[tmp],
            limits=module.CaptureLimits(),
        )
        source = coverage_dir / "stable.json"
        source.write_text('{"artifacts":{},"external_log_ranges":[]}')
        original_read = module.os.read
        grew = False

        def read_then_grow(descriptor, count):
            nonlocal grew
            data = original_read(descriptor, count)
            if data and not grew:
                with source.open("ab") as stream:
                    stream.write(b" ")
                grew = True
            return data

        module.os.read = read_then_grow
        try:
            stable = module._read_stable_regular_bytes(
                stable_builder,
                source,
                required=True,
                max_bytes=module.DEFAULT_MAX_METADATA_BYTES,
            )
        finally:
            module.os.read = original_read
        assert_equal(stable, None, "mutating coverage metadata read")
        if not any(error["code"] == "changed" for error in stable_builder.errors):
            raise AssertionError("coverage metadata mutation was not reported")


def test_limits_truncate_logs_and_reject_symlinks(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        config, config_path, nodes, build_dir = fixture(tmp)
        serial = Path(config["node"]["vm"]["serial_log"])
        serial.write_text("0123456789abcdef" * 128)
        output = tmp / "truncated.wosincident"
        manifest = module.capture_incident(
            output=output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            repo_root=tmp,
            limits=module.CaptureLimits(
                max_members=32,
                max_file_bytes=1024,
                max_total_bytes=4096,
            ),
            created_utc="2026-08-09T00:00:00Z",
        )
        assert_equal(
            manifest["capture"]["complete"], True, "bounded log capture status"
        )
        assert_equal(
            manifest["limits"],
            {
                "maxMembers": 32,
                "maxFileBytes": 1024,
                "maxTotalBytes": 4096,
                "maxPathBytes": module.DEFAULT_MAX_PATH_BYTES,
                "maxManifestBytes": module.DEFAULT_MAX_MANIFEST_BYTES,
            },
            "lower requested capture limits",
        )
        serial_member = next(
            member for member in manifest["members"] if member["kind"] == "serial-log"
        )
        assert_equal(serial_member["truncated"], True, "oversized log truncation flag")
        if serial_member["path"] not in manifest["capture"]["truncatedMembers"]:
            raise AssertionError("truncated member is absent from capture summary")

        real_serial = tmp / "real-serial.log"
        real_serial.write_text("must not traverse a symlink")
        serial.unlink()
        serial.symlink_to(real_serial)
        symlink_output = tmp / "symlink.wosincident"
        symlink_manifest = module.capture_incident(
            output=symlink_output,
            archive=False,
            kind="ktest",
            config=config,
            config_path=config_path,
            node_specs=nodes,
            build_dir=build_dir,
            repo_root=tmp,
            created_utc="2026-08-09T00:00:00Z",
        )
        assert_equal(
            symlink_manifest["capture"]["complete"], False, "symlink capture status"
        )
        if not any(
            error["code"] == "unsafe-type"
            for error in symlink_manifest["capture"]["errors"]
        ):
            raise AssertionError("symlink rejection was not recorded")


def test_live_target_is_exact_tmpfs_coredump(module) -> None:
    assert_equal(
        module._live_target("wos-0:/tmp/testprog_123_coredump.bin"),
        ("wos-0", "/tmp/testprog_123_coredump.bin", "testprog_123_coredump.bin"),
        "valid live target",
    )
    for value in (
        "wos-0:/var/log/coredump.bin",
        "wos-0:/tmp/not-a-dump.bin",
        "wos-0:/tmp/../root/secret_coredump.bin",
        "bad host:/tmp/a_coredump.bin",
        "wos-0:/tmp/a coredump_coredump.bin",
        "wos-0:/tmp/a\\name_coredump.bin",
        'wos-0:/tmp/a"name_coredump.bin',
        "wos-0:/tmp/a\tname_coredump.bin",
        "wos-0:/tmp/a\rname_coredump.bin",
        "wos-0:/tmp/a\n!id\n#_coredump.bin",
        "wos-0:/tmp/é_coredump.bin",
        "wos-0:/tmp/" + ("a" * 243) + "_coredump.bin",
    ):
        try:
            module._live_target(value)
        except module.IncidentError:
            pass
        else:
            raise AssertionError(f"unsafe live target accepted: {value}")


def test_sftp_batch_paths_are_quoted_and_controls_are_rejected(_module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-sftp-test-") as temporary:
        tmp = Path(temporary)
        fake_bin = tmp / "bin"
        fake_bin.mkdir()
        fake_sftp = fake_bin / "sftp"
        fake_sftp.write_text(
            "#!/bin/bash\n"
            "set -euo pipefail\n"
            'printf called > "$FAKE_SFTP_CALLED"\n'
            "batch=\n"
            "while (($#)); do\n"
            '  if [[ "$1" == "-b" ]]; then batch="$2"; shift 2; else shift; fi\n'
            "done\n"
            'cat "$batch"\n'
            'rm -f "$batch"\n'
        )
        fake_sftp.chmod(0o755)
        marker = tmp / "sftp-called"
        env = os.environ.copy()
        env["WOS_WORKSPACE_ROOT"] = str(ROOT)
        env["FAKE_SFTP_CALLED"] = str(marker)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        local_path = tmp / 'local "quoted"\\name.bin'
        quoted = subprocess.run(
            [
                str(WOS_SFTP_GET),
                "wos-0",
                '/tmp/remote "quoted"\\name.bin',
                str(local_path),
            ],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert_equal(quoted.returncode, 0, "quoted SFTP helper")
        assert_equal(marker.exists(), True, "valid SFTP helper invocation")
        batch_lines = quoted.stdout.splitlines()
        assert_equal(len(batch_lines), 1, "single SFTP batch command")
        if '\\"quoted\\"' not in batch_lines[0] or "\\\\name" not in batch_lines[0]:
            raise AssertionError(f"SFTP batch arguments were not escaped: {batch_lines!r}")

        marker.unlink()
        rejected = subprocess.run(
            [
                str(WOS_SFTP_GET),
                "wos-0",
                '/tmp/x"\n!id\n#_coredump.bin',
                str(tmp / "rejected.bin"),
            ],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if rejected.returncode == 0:
            raise AssertionError("control-bearing SFTP batch path was accepted")
        assert_equal(marker.exists(), False, "rejected SFTP helper invocation")


def test_builder_enforces_member_total_path_and_special_file_limits(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        staging = tmp / "staging"
        staging.mkdir()
        builder = module.IncidentBuilder(
            staging,
            allowed_roots=[tmp],
            limits=module.CaptureLimits(
                max_members=1,
                max_file_bytes=8,
                max_total_bytes=4,
                max_path_bytes=32,
            ),
        )
        first = builder.add_bytes(
            b"abcdefgh",
            "logs/first.log",
            kind="serial-log",
            source_name="first.log",
            required=True,
        )
        if first is None:
            raise AssertionError("bounded first member was rejected")
        assert_equal(first["truncated"], True, "total-byte truncation")
        assert_equal(
            (staging / first["path"]).read_bytes(), b"efgh", "bounded log tail"
        )
        second = builder.add_bytes(
            b"x",
            "logs/second.log",
            kind="qemu-log",
            source_name="second.log",
            required=False,
        )
        assert_equal(second, None, "member-count rejection")
        if not any(error["code"] == "member-limit" for error in builder.errors):
            raise AssertionError("member-count limit was not recorded")

        fifo = tmp / "serial.fifo"
        os.mkfifo(fifo)
        special_builder = module.IncidentBuilder(
            tmp / "special-staging",
            allowed_roots=[tmp],
            limits=module.CaptureLimits(),
        )
        (tmp / "special-staging").mkdir()
        special_builder.add_file(
            fifo,
            "nodes/node-0/serial.log",
            kind="serial-log",
            required=True,
        )
        if not any(error["code"] == "unsafe-type" for error in special_builder.errors):
            raise AssertionError("FIFO rejection was not recorded")

        for unsafe in ("../escape", "/absolute", "x" * 33):
            try:
                module._member_path(unsafe, 32)
            except module.IncidentError:
                pass
            else:
                raise AssertionError(f"unsafe member path accepted: {unsafe!r}")


def test_append_growth_is_truncated_and_limits_are_loader_compatible(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wosincident-test-") as temporary:
        tmp = Path(temporary)
        staging = tmp / "staging"
        staging.mkdir()
        source = tmp / "growing.log"
        source.write_bytes(b"before")
        builder = module.IncidentBuilder(
            staging,
            allowed_roots=[tmp],
            limits=module.CaptureLimits(),
        )
        original_read = module.os.read
        grew = False

        def read_then_append(descriptor, count):
            nonlocal grew
            data = original_read(descriptor, count)
            if data and not grew:
                with source.open("ab") as stream:
                    stream.write(b"after")
                grew = True
            return data

        module.os.read = read_then_append
        try:
            member = builder.add_file(
                source,
                "nodes/node-0/serial.log",
                kind="serial-log",
                required=True,
            )
        finally:
            module.os.read = original_read
        if member is None:
            raise AssertionError("append-only growth was rejected instead of bounded")
        assert_equal(member["truncated"], True, "append-only growth truncation")
        assert_equal(
            (staging / member["path"]).read_bytes(), b"before", "append snapshot boundary"
        )

        output = tmp / "clamped.wosincident"
        manifest = capture(
            module,
            tmp,
            output,
            limits=module.CaptureLimits(
                max_members=module.DEFAULT_MAX_MEMBERS * 2,
                max_file_bytes=module.DEFAULT_MAX_FILE_BYTES * 2,
                max_total_bytes=module.DEFAULT_MAX_TOTAL_BYTES * 2,
                max_path_bytes=module.DEFAULT_MAX_PATH_BYTES * 2,
            ),
        )
        assert_equal(
            manifest["limits"],
            {
                "maxMembers": module.DEFAULT_MAX_MEMBERS,
                "maxFileBytes": module.DEFAULT_MAX_FILE_BYTES,
                "maxTotalBytes": module.DEFAULT_MAX_TOTAL_BYTES,
                "maxPathBytes": module.DEFAULT_MAX_PATH_BYTES,
                "maxManifestBytes": module.DEFAULT_MAX_MANIFEST_BYTES,
            },
            "clamped loader-compatible limits",
        )

        config, config_path, nodes, build_dir = fixture(tmp)
        config["node"]["hostname"] = "x" * (module.DEFAULT_MAX_MANIFEST_BYTES + 4096)
        config_path.write_text(json.dumps(config))
        oversized_output = tmp / "oversized-manifest.wosincident"
        try:
            module.capture_incident(
                output=oversized_output,
                archive=False,
                kind="ktest",
                config=config,
                config_path=config_path,
                node_specs=nodes,
                build_dir=build_dir,
                repo_root=tmp,
                created_utc="2026-08-09T00:00:00Z",
            )
        except module.IncidentError as exc:
            if "manifest exceeds" not in str(exc):
                raise
        else:
            raise AssertionError("loader-incompatible manifest was published")
        assert_equal(oversized_output.exists(), False, "oversized manifest output")


def test_launcher_help_exposes_capture_hooks(_module) -> None:
    for command in (
        [str(ROOT / "bin" / "wos-ktest"), "--help"],
        [str(ROOT / "bin" / "wos-cluster"), "--help"],
    ):
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert_equal(result.returncode, 0, f"help command {' '.join(command)}")
        for flag in ("--incident-output", "--incident-archive"):
            if flag not in result.stdout:
                raise AssertionError(f"{' '.join(command)} is missing {flag}")


def main() -> None:
    module = load_module()
    tests = [
        test_public_entrypoint_and_help,
        test_directory_manifest_is_canonical_bounded_and_redacted,
        test_directory_and_ustar_share_manifest_and_archive_is_deterministic,
        test_atomic_publish_refuses_existing_output,
        test_snapshot_uses_new_inode_and_appended_range,
        test_coverage_manifest_materializes_ranges_without_host_paths,
        test_coverage_manifest_is_stable_strict_and_has_no_repo_fallback,
        test_limits_truncate_logs_and_reject_symlinks,
        test_live_target_is_exact_tmpfs_coredump,
        test_sftp_batch_paths_are_quoted_and_controls_are_rejected,
        test_builder_enforces_member_total_path_and_special_file_limits,
        test_append_growth_is_truncated_and_limits_are_loader_compatible,
        test_launcher_help_exposes_capture_hooks,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} wosincident tests passed")


if __name__ == "__main__":
    main()
