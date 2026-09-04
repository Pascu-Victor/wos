#!/usr/bin/env python3
"""Deterministic end-to-end tests for WOSDBG's read-only live CLI tools.

The peers in this file implement only the bounded protocol surface WOSDBG is
allowed to use: QMP capability/status/stop/cont and read-only GDB RSP packets.
Run against any build with ``--wosdbg PATH``.  Older binaries produce a clear
SKIP unless ``--require-live-tools`` is selected.

CLI invocations intentionally have the fixed owner ``local`` and sessions are
process-local, so a distinct-owner failure cannot be induced through one
``--batch`` workflow.  The bad-token path is covered here; owner isolation is
an MCP/GUI transport-lifetime test boundary.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WOSDBG = REPO_ROOT / "tools" / "build" / "bin" / "wosdbg"
MAX_CLI_BYTES = 2 * 1024 * 1024
LIVE_TOOLS = {
    "wosdbg.discover_live_targets",
    "wosdbg.list_live_targets",
    "wosdbg.open_live_session",
    "wosdbg.list_live_sessions",
    "wosdbg.get_live_session",
    "wosdbg.renew_live_session",
    "wosdbg.close_live_session",
    "wosdbg.read_live_registers",
    "wosdbg.read_live_memory",
    "wosdbg.backtrace_live",
    "wosdbg.resolve_live_address",
    "wosdbg.get_live_source",
    "wosdbg.inspect_live_pte",
    "wosdbg.get_live_transcript",
    "wosdbg.verify_live_transcript",
    "wosdbg.capture_live_incident",
}
MUTATING_RSP_PREFIXES = ("G", "P", "M", "X", "Z", "z", "c", "s", "vCont", "k", "D")


class TestFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def process_start_ticks() -> str:
    stat = Path(f"/proc/{os.getpid()}/stat").read_text(encoding="ascii")
    closing = stat.rfind(")")
    require(closing >= 0, "could not parse /proc process identity")
    fields = stat[closing + 1 :].split()
    require(len(fields) > 19, "short /proc process identity")
    return fields[19]


def elf_build_id(path: Path) -> str:
    try:
        completed = subprocess.run(
            ["readelf", "-n", str(path)],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise TestFailure(f"cannot inspect WOSDBG build ID: {error}") from error
    match = re.search(r"Build ID:\s*([0-9a-fA-F]+)", completed.stdout)
    require(completed.returncode == 0 and match is not None, "WOSDBG binary has no readable ELF build ID")
    return match.group(1).lower()


def load_json_process(
    command: list[str], *, cwd: Path, timeout: float, input_text: str | None = None
) -> tuple[int, dict[str, Any]]:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise TestFailure(f"CLI exceeded outer {timeout:g}s deadline: {' '.join(command)}") from error
    require(
        len(completed.stdout.encode("utf-8")) <= MAX_CLI_BYTES,
        f"CLI exceeded {MAX_CLI_BYTES} output bytes",
    )
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise TestFailure(
            f"CLI returned non-JSON output (status {completed.returncode}); stderr={completed.stderr.strip()!r}"
        ) from error
    require(isinstance(result, dict), "CLI returned a non-object JSON value")
    return completed.returncode, result


def tool_catalog(binary: Path, *, cwd: Path, timeout: float) -> dict[str, dict[str, Any]]:
    status, output = load_json_process([str(binary), "--list-tools"], cwd=cwd, timeout=timeout)
    require(status == 0, f"--list-tools failed with status {status}")
    entries = output.get("tools")
    require(isinstance(entries, list), "tool catalog has no tools array")
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        require(isinstance(entry, dict), "tool catalog contains a non-object")
        name = entry.get("name")
        require(isinstance(name, str) and name, "tool catalog entry has no name")
        require(name not in result, f"duplicate tool catalog name {name}")
        result[name] = entry
    return result


def validate_live_catalog(catalog: dict[str, dict[str, Any]]) -> list[str]:
    missing = sorted(LIVE_TOOLS - catalog.keys())
    if missing:
        return missing
    for name in LIVE_TOOLS:
        schema = catalog[name].get("inputSchema")
        require(isinstance(schema, dict) and schema.get("type") == "object", f"{name}: invalid input schema")
        properties = schema.get("properties")
        require(isinstance(properties, dict), f"{name}: schema properties are absent")
        for required in schema.get("required", []):
            require(required in properties, f"{name}: required field {required!r} has no schema")
    live_names = {name for name in catalog if "live" in name}
    forbidden_words = ("write", "set_register", "continue", "step", "breakpoint", "kill", "detach")
    require(
        not any(any(word in name.lower() for word in forbidden_words) for name in live_names),
        "catalog exposes a mutating live tool",
    )
    return []


def invoke_batch(
    binary: Path, *, cwd: Path, calls: list[dict[str, Any]], timeout: float
) -> tuple[int, dict[str, dict[str, Any]]]:
    payload = json.dumps({"calls": calls, "continueOnError": True}, sort_keys=True)
    status, output = load_json_process(
        [str(binary), "--batch", "-"], cwd=cwd, timeout=timeout, input_text=payload
    )
    require(status in {0, 1}, f"batch returned unexpected status {status}")
    entries = output.get("calls")
    require(isinstance(entries, list), "batch output has no calls array")
    results: dict[str, dict[str, Any]] = {}
    for entry in entries:
        require(isinstance(entry, dict), "batch output contains a non-object call")
        call_id = entry.get("id")
        result = entry.get("result")
        require(isinstance(call_id, str) and call_id, "batch call has no id")
        require(isinstance(result, dict), f"batch call {call_id} has a non-object result")
        require(call_id not in results, f"batch returned duplicate id {call_id}")
        results[call_id] = result
    require(len(results) == len(calls), f"batch returned {len(results)} of {len(calls)} results")
    return status, results


def call(call_id: str, tool: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"id": call_id, "tool": tool, "arguments": arguments or {}}


def open_arguments(**arguments: Any) -> dict[str, Any]:
    return {
        "authority": "pause-read",
        "confirmation": "PAUSE_ALLOWLISTED_TARGETS",
        "auditId": "semantic-test",
        **arguments,
    }


def success(result: dict[str, Any], label: str) -> None:
    require(result.get("ok", True) is not False, f"{label} failed: {result}")


def rsp_escape(payload: bytes) -> bytes:
    encoded = bytearray()
    for value in payload:
        if value in b"#$}*":
            encoded.extend((ord("}"), value ^ 0x20))
        else:
            encoded.append(value)
    return bytes(encoded)


def rsp_frame(payload: bytes, *, bad_checksum: bool = False) -> bytes:
    wire = rsp_escape(payload)
    checksum = sum(wire) & 0xFF
    if bad_checksum:
        checksum ^= 0xFF
    return b"$" + wire + b"#" + f"{checksum:02x}".encode("ascii")


def recv_rsp_packet(connection: socket.socket, pending: bytearray) -> bytes | None:
    while True:
        while pending and pending[0] in b"+-":
            del pending[0]
        marker = pending.find(b"$")
        if marker >= 0:
            if marker:
                del pending[:marker]
            checksum_at = pending.find(b"#", 1)
            if checksum_at >= 0 and len(pending) >= checksum_at + 3:
                wire = bytes(pending[1:checksum_at])
                claimed = int(bytes(pending[checksum_at + 1 : checksum_at + 3]), 16)
                del pending[: checksum_at + 3]
                require((sum(wire) & 0xFF) == claimed, "fake RSP peer received a bad client checksum")
                decoded = bytearray()
                index = 0
                while index < len(wire):
                    if wire[index] == ord("}"):
                        require(index + 1 < len(wire), "client sent a dangling RSP escape")
                        index += 1
                        decoded.append(wire[index] ^ 0x20)
                    else:
                        decoded.append(wire[index])
                    index += 1
                return bytes(decoded)
        data = connection.recv(65536)
        if not data:
            return None
        pending.extend(data)


class FakeRsp:
    """Small read-only loopback RSP target with deterministic evidence."""

    TARGET_XML = (
        # QEMU emits this established GDB target-description form without an
        # xmlns:xi declaration; the live adapter must interoperate with it.
        b'<?xml version="1.0"?><target>'
        b'<architecture>i386:x86-64</architecture><xi:include href="dynamic-core.xml"/></target>'
    )
    CORE_XML = (
        b'<?xml version="1.0"?><feature name="wos.dynamic">'
        b'<reg name="wos32" bitsize="32" regnum="0" type="uint32" group="general"/>'
        b'<reg name="rbp" bitsize="64" regnum="1" type="data_ptr" group="general"/>'
        b'<reg name="rip" bitsize="64" regnum="2" type="code_ptr" group="general"/>'
        b'<reg name="flags16" bitsize="16" regnum="3" type="uint16" group="general"/>'
        b'</feature>'
    )
    REGISTER_BYTES = bytes.fromhex("44332211" "0020000000000000" "3412000000000000" "0200")

    def __init__(self, build_id: str, *, fault: str | None = None) -> None:
        self.build_id = build_id
        self.fault = fault
        self.commands: list[str] = []
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(4)
        self._listener.settimeout(0.1)
        self.port = int(self._listener.getsockname()[1])
        self._thread = threading.Thread(target=self._serve, name=f"fake-rsp-{self.port}", daemon=True)
        self._thread.start()

    def image_catalog(self) -> bytes:
        entry = {
            "pathHex": b"/wos".hex(),
            "loadBase": "0x0",
            "imageStart": "0x1000",
            "imageEnd": "0x9000",
            "textAddress": "0x1000",
            "textSize": "0x8000",
            "entry": "0x1234",
            "dynamicAddress": "0x0",
            "flags": "0x1",
            "buildIdSize": len(self.build_id) // 2,
            "buildId": self.build_id,
        }
        return json.dumps(
            {
                "format": "wos-image-catalog",
                "version": 1,
                "source": "semantic-fake",
                "recordSize": 128,
                "snapshotStatus": "complete",
                "snapshotStatusCode": 0,
                "count": 1,
                "images": [entry],
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")

    @staticmethod
    def xfer(command: str, payload: bytes) -> bytes:
        match = re.fullmatch(r"qXfer:[^:]+:read:[^:]*:([0-9a-fA-F]+),([0-9a-fA-F]+)", command)
        require(match is not None, f"malformed qXfer request {command!r}")
        offset, length = (int(value, 16) for value in match.groups())
        chunk = payload[offset : offset + length]
        return (b"l" if offset + len(chunk) >= len(payload) else b"m") + chunk

    def response(self, command: str) -> bytes:
        if command.startswith("qSupported:"):
            return b"PacketSize=2000;qXfer:features:read+;qXfer:wos-images:read+;QStartNoAckMode+"
        if command == "QStartNoAckMode":
            return b"OK"
        if command.startswith("qXfer:features:read:target.xml:"):
            return self.xfer(command, self.TARGET_XML)
        if command.startswith("qXfer:features:read:dynamic-core.xml:"):
            return self.xfer(command, self.CORE_XML)
        if command.startswith("qXfer:wos-images:read::"):
            return self.xfer(command, self.image_catalog())
        if command == "g":
            return self.REGISTER_BYTES.hex().encode("ascii")
        if command.startswith("p"):
            regnum = int(command[1:], 16)
            widths = (4, 8, 8, 2)
            require(0 <= regnum < len(widths), f"unexpected register number {regnum}")
            start = sum(widths[:regnum])
            return self.REGISTER_BYTES[start : start + widths[regnum]].hex().encode("ascii")
        memory = re.fullmatch(r"m([0-9a-fA-F]+),([0-9a-fA-F]+)", command)
        if memory:
            address, length = (int(value, 16) for value in memory.groups())
            return bytes((address + index) & 0xFF for index in range(length)).hex().encode("ascii")
        if command == "?":
            return b"T05"
        raise TestFailure(f"unexpected RSP command {command!r}")

    def _serve_connection(self, connection: socket.socket) -> None:
        connection.settimeout(0.2)
        pending = bytearray()
        no_ack = False
        bad_replies = 0
        while not self._stop.is_set():
            try:
                command_bytes = recv_rsp_packet(connection, pending)
            except socket.timeout:
                continue
            if command_bytes is None:
                return
            command = command_bytes.decode("ascii")
            self.commands.append(command)
            if self.fault and len(self.commands) == 1:
                if self.fault == "disconnect":
                    return
                if self.fault == "timeout":
                    while not self._stop.wait(0.05):
                        pass
                    return
                if not no_ack:
                    connection.sendall(b"+")
                if self.fault == "oversize":
                    connection.sendall(rsp_frame(b"x" * (64 * 1024 + 1)))
                    return
                if self.fault == "malformed":
                    frame = rsp_frame(b"malformed", bad_checksum=True)
                    connection.sendall(frame)
                    while bad_replies < 3:
                        try:
                            marker = connection.recv(1)
                        except socket.timeout:
                            continue
                        if marker == b"-":
                            bad_replies += 1
                            connection.sendall(frame)
                        elif not marker:
                            return
                    return
                raise TestFailure(f"unknown RSP fault {self.fault}")
            if not no_ack:
                connection.sendall(b"+")
            response = self.response(command)
            connection.sendall(rsp_frame(response))
            if command == "QStartNoAckMode":
                no_ack = True

    def _serve(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    connection, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                with connection:
                    self._serve_connection(connection)
        except BaseException as error:  # propagated by close()
            self.error = error

    def close(self) -> None:
        self._stop.set()
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.1).close()
        except OSError:
            pass
        self._listener.close()
        self._thread.join(timeout=2)
        require(not self._thread.is_alive(), "fake RSP peer did not terminate")
        if self.error:
            raise TestFailure(f"fake RSP peer failed: {self.error}") from self.error
        mutating = [command for command in self.commands if command.startswith(MUTATING_RSP_PREFIXES)]
        require(not mutating, f"WOSDBG sent mutating RSP packets: {mutating}")


class FakeQmp:
    """Bounded AF_UNIX QMP server that records pause ownership transitions."""

    def __init__(self, path: Path, *, running: bool = True, fault: str | None = None) -> None:
        self.path = path
        self.running = running
        self.fault = fault
        self.commands: list[str] = []
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(path))
        self._listener.listen(4)
        self._listener.settimeout(0.1)
        self._thread = threading.Thread(target=self._serve, name=f"fake-qmp-{path.name}", daemon=True)
        self._thread.start()

    def reply(self, request: dict[str, Any]) -> bytes:
        command = request.get("execute")
        request_id = request.get("id")
        require(isinstance(command, str), "fake QMP received a command without execute")
        self.commands.append(command)
        if self.fault and command == "query-status":
            if self.fault == "disconnect":
                return b""
            if self.fault == "timeout":
                while not self._stop.wait(0.05):
                    pass
                return b""
            if self.fault == "malformed":
                return b"{not-json}\r\n"
            if self.fault == "duplicate":
                return (
                    b'{"return":{"status":"running","running":true,"singlestep":false},"id":'
                    + str(request_id).encode("ascii")
                    + b',"id":'
                    + str(request_id).encode("ascii")
                    + b"}\r\n"
                )
            if self.fault == "oversize":
                return b'{"return":"' + (b"x" * (1024 * 1024 + 1)) + b'","id":1}\r\n'
            raise TestFailure(f"unknown QMP fault {self.fault}")
        if command == "qmp_capabilities":
            body: Any = {}
        elif command == "query-status":
            body = {"status": "running" if self.running else "paused", "running": self.running, "singlestep": False}
        elif command == "stop":
            require(self.running, "WOSDBG sent stop to an already paused target")
            self.running = False
            body = {}
        elif command == "cont":
            require(not self.running, "WOSDBG sent cont to a running target")
            self.running = True
            body = {}
        else:
            raise TestFailure(f"unexpected QMP command {command!r}")
        return json.dumps({"return": body, "id": request_id}, separators=(",", ":")).encode("ascii") + b"\r\n"

    def _serve_connection(self, connection: socket.socket) -> None:
        greeting = {
            "QMP": {
                "version": {"qemu": {"major": 8, "minor": 2, "micro": 0}, "package": "semantic-fake"},
                "capabilities": [],
            }
        }
        connection.sendall(json.dumps(greeting, separators=(",", ":")).encode("ascii") + b"\r\n")
        connection.settimeout(0.2)
        pending = bytearray()
        while not self._stop.is_set():
            while b"\n" not in pending:
                try:
                    chunk = connection.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                pending.extend(chunk)
            line, _, remainder = pending.partition(b"\n")
            pending = bytearray(remainder)
            request = json.loads(line)
            require(isinstance(request, dict), "fake QMP received a non-object")
            response = self.reply(request)
            if not response:
                return
            connection.sendall(response)

    def _serve(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    connection, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                with connection:
                    self._serve_connection(connection)
        except BaseException as error:  # propagated by close()
            self.error = error

    def close(self) -> None:
        self._stop.set()
        try:
            wake = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            wake.settimeout(0.1)
            wake.connect(str(self.path))
            wake.close()
        except OSError:
            pass
        self._listener.close()
        self._thread.join(timeout=2)
        require(not self._thread.is_alive(), "fake QMP peer did not terminate")
        if self.error:
            raise TestFailure(f"fake QMP peer failed: {self.error}") from self.error


def runtime_target(target_id: str, node_id: str, qmp: FakeQmp, rsp: FakeRsp, binary: Path, build_id: str) -> dict[str, Any]:
    return {
        "id": target_id,
        "nodeId": node_id,
        "debug": True,
        "transport": "qemu",
        "host": "127.0.0.1",
        "port": rsp.port,
        "qmpSocket": str(qmp.path),
        "symbolPath": str(binary),
        "expectedBuildId": build_id,
        "logPaths": [],
        "pid": os.getpid(),
        "processStartTicks": process_start_ticks(),
    }


def write_config(root: Path, targets: list[dict[str, Any]], binary: Path, *, operation_ms: int = 500) -> None:
    descriptor = root / "live-runtime.json"
    descriptor.write_text(
        json.dumps(
            {
                "format": "wosdbg-live",
                "version": 1,
                "state": "running",
                "bootId": Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip(),
                "launchNonce": "live-cli-semantic-test",
                "topology": {
                    "nodes": [{"nodeId": target["nodeId"]} for target in targets],
                    "zones": [],
                },
                "targets": targets,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    config = {
        "mcp": {"allowedRoots": [str(root), str(binary.parent), str(REPO_ROOT)]},
        "live": {
            "enabled": True,
            "allowMutations": False,
            "allowedHosts": ["127.0.0.1"],
            "maxTargets": 16,
            "maxSessions": 4,
            "operationTimeoutMs": operation_ms,
            "leaseMs": 5000,
            "maxMemoryBytes": 4096,
            "maxTranscriptBytes": 1024 * 1024,
            "runtimeDescriptors": [str(descriptor)],
            "targets": [],
        },
    }
    (root / "wosdbg.json").write_text(json.dumps(config, sort_keys=True), encoding="utf-8")


def assert_qmp_lease(qmp: FakeQmp, *, owned: bool) -> None:
    require(qmp.commands.count("qmp_capabilities") == 1, f"unexpected QMP negotiation: {qmp.commands}")
    require(qmp.commands.count("stop") == (1 if owned else 0), f"unexpected stop count: {qmp.commands}")
    require(qmp.commands.count("cont") == (1 if owned else 0), f"unexpected cont count: {qmp.commands}")
    require(qmp.running is True if owned else qmp.running is False, f"wrong final QMP state: {qmp.commands}")


def run_happy(binary: Path, build_id: str, timeout: float) -> None:
    with tempfile.TemporaryDirectory(prefix="wosdbg-live-happy-") as raw_root:
        root = Path(raw_root)
        qmp = FakeQmp(root / "qmp.sock", running=True)
        rsp = FakeRsp(build_id)
        try:
            write_config(root, [runtime_target("vm0", "0", qmp, rsp, binary, build_id)], binary)
            calls = [
                call("discover", "discover_live_targets"),
                call("listed", "list_live_targets"),
                call("open", "open_live_session", open_arguments(targetIds=["vm0"], leaseMs=4000)),
                call(
                    "registers",
                    "read_live_registers",
                    {"sessionId": "$open.sessionId", "leaseToken": "$open.leaseToken", "targetId": "vm0"},
                ),
                call(
                    "memory",
                    "read_live_memory",
                    {
                        "sessionId": "$open.sessionId",
                        "leaseToken": "$open.leaseToken",
                        "targetId": "vm0",
                        "address": "0x1000",
                        "length": 8,
                        "confirmation": "READ_SENSITIVE_LIVE_MEMORY",
                    },
                ),
                call(
                    "redacted1",
                    "get_live_transcript",
                    {"sessionId": "$open.sessionId", "leaseToken": "$open.leaseToken"},
                ),
                call(
                    "redacted2",
                    "get_live_transcript",
                    {"sessionId": "$open.sessionId", "leaseToken": "$open.leaseToken"},
                ),
                call(
                    "replay",
                    "verify_live_transcript",
                    {"transcript": "$redacted1.transcript", "digest": "$redacted1.digest"},
                ),
                call(
                    "sensitive_denied",
                    "get_live_transcript",
                    {
                        "sessionId": "$open.sessionId",
                        "leaseToken": "$open.leaseToken",
                        "includeSensitivePayloads": True,
                    },
                ),
                call(
                    "bad_token",
                    "get_live_session",
                    {"sessionId": "$open.sessionId", "leaseToken": "00" * 32},
                ),
                call("sessions", "list_live_sessions"),
                call(
                    "capture",
                    "capture_live_incident",
                    {
                        "sessionId": "$open.sessionId",
                        "leaseToken": "$open.leaseToken",
                        "output": str(root / "live.wosincident"),
                        "confirmation": "CAPTURE_LIVE_INCIDENT",
                    },
                ),
                call(
                    "close",
                    "close_live_session",
                    {"sessionId": "$open.sessionId", "leaseToken": "$open.leaseToken"},
                ),
                call("after", "list_live_sessions"),
            ]
            status, results = invoke_batch(binary, cwd=root, calls=calls, timeout=timeout)
            require(status == 1, "happy batch should be nonzero because it includes expected negative cases")
            for label in (
                "discover",
                "listed",
                "open",
                "registers",
                "memory",
                "redacted1",
                "redacted2",
                "replay",
                "sessions",
                "capture",
                "close",
                "after",
            ):
                success(results[label], label)

            discovered = results["discover"]
            require(discovered.get("targetCount") == 1, f"bad discovery result: {discovered}")
            target = discovered.get("targets", [{}])[0]
            require(target.get("id") == "vm0" and target.get("source") == "live-runtime.json", "runtime target not discovered")
            require(target.get("localBuildId") == build_id, f"local build ID mismatch: {target}")

            opened_target = results["open"].get("targets", [{}])[0]
            require(results["open"].get("readOnly") is True, "live session is not marked read-only")
            require(opened_target.get("ownsPause") is True, f"pause ownership absent: {opened_target}")
            require(opened_target.get("remoteBuildId") == build_id, f"image catalog build ID mismatch: {opened_target}")
            require(opened_target.get("symbolsQuarantined") is False, f"matching symbols were quarantined: {opened_target}")

            registers = results["registers"].get("registers")
            require(isinstance(registers, list), "register result has no array")
            require(
                [(entry.get("name"), entry.get("bitsize")) for entry in registers]
                == [("wos32", 32), ("rbp", 64), ("rip", 64), ("flags16", 16)],
                f"target XML dynamic layout was not honored: {registers}",
            )
            require(registers[0].get("value") == "0x0000000011223344", f"bad register decoding: {registers[0]}")

            memory = bytes(range(8))
            require(results["memory"].get("data") == base64.b64encode(memory).decode("ascii"), "bad memory payload")
            require(results["memory"].get("sha256") == hashlib.sha256(memory).hexdigest(), "bad memory digest")

            first = results["redacted1"]
            second = results["redacted2"]
            require(first == second, "redacted transcript changed without a protocol operation")
            transcript = first.get("transcript", {})
            require(transcript.get("format") == "wosdbg-live-transcript" and transcript.get("version") == 1, "bad transcript envelope")
            records = transcript.get("records")
            require(isinstance(records, list) and records, "transcript has no records")
            require(
                [record.get("sequence") for record in records] == list(range(1, len(records) + 1)),
                "transcript sequence is not contiguous and deterministic",
            )
            require(all(record.get("payloadRedacted") is True and "payload" not in record for record in records), "transcript leaked payload")
            require(all(re.fullmatch(r"[0-9a-f]{64}", str(record.get("payloadSha256"))) for record in records), "bad transcript payload digest")
            require(re.fullmatch(r"sha256:[0-9a-f]{64}", str(first.get("digest"))) is not None, "bad transcript digest")
            require(results["replay"].get("deterministic") is True, "transcript replay was not deterministic")
            require(results["replay"].get("recordCount") == len(records), "transcript replay record count changed")
            require(results["capture"].get("reopened", {}).get("ok") is True, "live incident was not reopened")
            require((root / "live.wosincident" / "manifest.json").is_file(), "live incident was not atomically published")

            require(results["sensitive_denied"].get("errorCode") == "sensitive_confirmation_required", "sensitive transcript was not denied")
            require(results["bad_token"].get("errorCode") == "lease_token_mismatch", "bad token was not rejected")
            require(results["sessions"].get("sessionCount") == 1, "active session not listed")
            require(results["after"].get("sessionCount") == 0, "closed session remained listed")
        finally:
            rsp.close()
            qmp.close()
        assert_qmp_lease(qmp, owned=True)


def run_initially_paused(binary: Path, build_id: str, timeout: float) -> None:
    with tempfile.TemporaryDirectory(prefix="wosdbg-live-paused-") as raw_root:
        root = Path(raw_root)
        qmp = FakeQmp(root / "qmp.sock", running=False)
        rsp = FakeRsp(build_id)
        try:
            write_config(root, [runtime_target("paused", "1", qmp, rsp, binary, build_id)], binary)
            _, results = invoke_batch(
                binary,
                cwd=root,
                timeout=timeout,
                calls=[
                    call("discover", "discover_live_targets"),
                    call("open", "open_live_session", open_arguments(targetId="paused")),
                    call(
                        "close",
                        "close_live_session",
                        {"sessionId": "$open.sessionId", "leaseToken": "$open.leaseToken"},
                    ),
                ],
            )
            for label in ("discover", "open", "close"):
                success(results[label], label)
            require(results["open"].get("targets", [{}])[0].get("ownsPause") is False, "initial pause was claimed")
        finally:
            rsp.close()
            qmp.close()
        assert_qmp_lease(qmp, owned=False)


def run_rollback(binary: Path, build_id: str, timeout: float) -> None:
    with tempfile.TemporaryDirectory(prefix="wosdbg-live-rollback-") as raw_root:
        root = Path(raw_root)
        good_qmp = FakeQmp(root / "good.sock", running=True)
        bad_qmp = FakeQmp(root / "bad.sock", running=True)
        good_rsp = FakeRsp(build_id)
        bad_rsp = FakeRsp(build_id, fault="disconnect")
        try:
            targets = [
                runtime_target("a-good", "2", good_qmp, good_rsp, binary, build_id),
                runtime_target("z-bad", "3", bad_qmp, bad_rsp, binary, build_id),
            ]
            write_config(root, targets, binary)
            status, results = invoke_batch(
                binary,
                cwd=root,
                timeout=timeout,
                calls=[
                    call("discover", "discover_live_targets"),
                    call("open", "open_live_session", open_arguments(targetIds=["z-bad", "a-good"])),
                    call("sessions", "list_live_sessions"),
                ],
            )
            require(status == 1, "partial acquisition failure should fail its batch")
            success(results["discover"], "discover")
            require(results["open"].get("errorCode") == "protocol_failure", f"rollback fault misclassified: {results['open']}")
            success(results["sessions"], "sessions")
            require(results["sessions"].get("sessionCount") == 0, "failed multi-target session was retained")
        finally:
            bad_rsp.close()
            good_rsp.close()
            bad_qmp.close()
            good_qmp.close()
        assert_qmp_lease(good_qmp, owned=True)
        assert_qmp_lease(bad_qmp, owned=True)


def run_fault_case(binary: Path, build_id: str, timeout: float, protocol: str, fault: str) -> None:
    with tempfile.TemporaryDirectory(prefix=f"wosdbg-live-{protocol}-{fault}-") as raw_root:
        root = Path(raw_root)
        qmp = FakeQmp(root / "qmp.sock", running=True, fault=fault if protocol == "qmp" else None)
        rsp = FakeRsp(build_id, fault=fault if protocol == "rsp" else None)
        try:
            write_config(
                root,
                [runtime_target(f"{protocol}-{fault}", "4", qmp, rsp, binary, build_id)],
                binary,
                operation_ms=200,
            )
            started = time.monotonic()
            status, results = invoke_batch(
                binary,
                cwd=root,
                timeout=timeout,
                calls=[
                    call("discover", "discover_live_targets"),
                    call("open", "open_live_session", open_arguments(targetId=f"{protocol}-{fault}")),
                    call("sessions", "list_live_sessions"),
                ],
            )
            elapsed = time.monotonic() - started
            require(elapsed < min(timeout, 4.0), f"{protocol}/{fault} ignored bounded deadline ({elapsed:.2f}s)")
            require(status == 1, f"{protocol}/{fault} unexpectedly succeeded")
            success(results["discover"], f"{protocol}/{fault} discovery")
            require(results["open"].get("errorCode") == "protocol_failure", f"{protocol}/{fault} misclassified: {results['open']}")
            success(results["sessions"], f"{protocol}/{fault} sessions")
            require(results["sessions"].get("sessionCount") == 0, f"{protocol}/{fault} retained a session")
        finally:
            rsp.close()
            qmp.close()
        if protocol == "rsp":
            assert_qmp_lease(qmp, owned=True)
        else:
            require(qmp.commands.count("stop") == 0 and qmp.commands.count("cont") == 0, f"QMP fault mutated run state: {qmp.commands}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wosdbg", type=Path, default=DEFAULT_WOSDBG, help="WOSDBG executable to exercise")
    parser.add_argument(
        "--mode",
        choices=("all", "happy", "paused", "rollback", "faults"),
        default="all",
        help="bounded scenario group to run",
    )
    parser.add_argument("--timeout", type=float, default=12.0, help="outer timeout for each CLI process")
    parser.add_argument("--require-live-tools", action="store_true", help="fail instead of skipping an older WOSDBG")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    binary = args.wosdbg.resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), f"WOSDBG is not executable: {binary}")
    require(args.timeout >= 2.0, "--timeout must be at least 2 seconds")
    with tempfile.TemporaryDirectory(prefix="wosdbg-live-catalog-") as raw_root:
        catalog = tool_catalog(binary, cwd=Path(raw_root), timeout=args.timeout)
    missing = validate_live_catalog(catalog)
    if missing:
        message = "WOSDBG predates live tool catalog: " + ", ".join(missing)
        if args.require_live_tools:
            raise TestFailure(message)
        print("SKIP:", message)
        return 0

    build_id = elf_build_id(binary)
    if args.mode in {"all", "happy"}:
        run_happy(binary, build_id, args.timeout)
    if args.mode in {"all", "paused"}:
        run_initially_paused(binary, build_id, args.timeout)
    if args.mode in {"all", "rollback"}:
        run_rollback(binary, build_id, args.timeout)
    if args.mode in {"all", "faults"}:
        for protocol, fault in (
            ("qmp", "timeout"),
            ("qmp", "disconnect"),
            ("qmp", "malformed"),
            ("qmp", "duplicate"),
            ("qmp", "oversize"),
            ("rsp", "timeout"),
            ("rsp", "disconnect"),
            ("rsp", "malformed"),
            ("rsp", "oversize"),
        ):
            run_fault_case(binary, build_id, args.timeout, protocol, fault)
    print(f"PASS: WOSDBG live CLI semantic scenarios ({args.mode})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TestFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1)
