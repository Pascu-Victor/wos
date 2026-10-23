#!/usr/bin/env python3
"""Exercise the real Linux command supervisor with private disposable processes."""

from __future__ import annotations

import ctypes
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[3]
SUPERVISOR = ROOT / "scripts/test/owned_command.py"
SELF = Path(__file__).resolve()
PAYLOAD = bytes(range(256)) * 2048
STDERR_BYTES = b"stderr:\x00\xff\x80\n"


def require(value: bool, message: str) -> None:
    if not value:
        raise AssertionError(message)


def identity(pid: int) -> tuple[int, int, str, int, int] | None:
    try:
        tail = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        return pid, int(tail[19]), tail[0], int(tail[1]), int(tail[3])
    except (FileNotFoundError, ProcessLookupError):
        return None


def same_birth(birth: tuple[int, int, str, int, int]) -> bool:
    current = identity(birth[0])
    return current is not None and current[1] == birth[1]


def live_birth(birth: tuple[int, int, str, int, int]) -> bool:
    current = identity(birth[0])
    return current is not None and current[1] == birth[1] and current[2] not in ("Z", "X")


def signal_birth(birth: tuple[int, int, str, int, int], signum: int) -> None:
    if not live_birth(birth):
        return
    try:
        fd = os.pidfd_open(birth[0], 0)
    except ProcessLookupError:
        return
    try:
        if same_birth(birth):
            signal.pidfd_send_signal(fd, signum)
    except ProcessLookupError:
        pass
    finally:
        os.close(fd)


def process_rows() -> dict[int, tuple[int, int, str, int, int]]:
    rows = {}
    for path in Path("/proc").iterdir():
        if path.name.isdecimal():
            row = identity(int(path.name))
            if row is not None:
                rows[row[0]] = row
    return rows


def save_birth(folder: Path, name: str, **extra: object) -> None:
    row = identity(os.getpid())
    require(row is not None, "native fixture can observe its own PID birth")
    body = (json.dumps(dict(pid=row[0], start_ticks=row[1], state=row[2], ppid=row[3], sid=row[4], **extra)) + "\n").encode()
    fd = os.open(folder / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def wait_file(path: Path, seconds: float = 10) -> None:
    deadline = time.monotonic() + seconds
    while not path.exists():
        require(time.monotonic() < deadline, "native fixture coordination deadline")
        time.sleep(0.005)


def native_fixture(mode: str, folder: Path) -> int:
    save_birth(folder, "native.json")
    if mode == "self-signal":
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError("native SIGKILL unexpectedly returned")
    if mode == "stream":
        chunks = []
        while True:
            chunk = os.read(0, 65536)
            if not chunk:
                break
            chunks.append(chunk)
        pending = memoryview(b"".join(chunks))
        while pending:
            pending = pending[os.write(1, pending):]
        os.write(2, STDERR_BYTES)
        return 23
    if mode == "zombie":
        child = os.fork()
        if child == 0:
            save_birth(folder, "child.json")
            os._exit(0)
        observed = os.waitid(os.P_PID, child, os.WEXITED | os.WNOWAIT)
        row = identity(child)
        require(observed.si_pid == child and row is not None and row[2] == "Z", "real WNOWAIT retains the terminal child")
        (folder / "zombie-confirmed.json").write_text(json.dumps(dict(pid=row[0], start_ticks=row[1], state=row[2])) + "\n")
        os.write(1, b"confirmed zombie\n")
        return 0
    require(mode in ("orphan", "blocked"), "known native fixture mode")
    child = os.fork()
    if child == 0:
        os.setsid()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        for fd in (0, 1, 2):
            os.close(fd)
        save_birth(folder, "child.json", term_ignored=True, stdio_closed=True)
        while True:
            time.sleep(0.05)
    wait_file(folder / "child.json")
    if mode == "orphan":
        wait_file(folder / "release")
        os.write(1, b"native leader completed\n")
        return 0
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(0.05)


class Scenario:
    def __init__(self, mode: str, seconds: float = 8):
        self.mode = mode
        self.seconds = seconds
        self.folder = Path(tempfile.mkdtemp(prefix="wos-owned-command-test-"))
        self.receipt = self.folder / "receipt.json"
        self.process: subprocess.Popen[bytes] | None = None
        self.worker = None
        self.sentinel: subprocess.Popen[bytes] | None = None
        self.sentinel_birth = None
        self.births = []
        self.baseline = {pid for pid, row in process_rows().items() if row[3] == os.getpid()}

    def __enter__(self) -> Scenario:
        return self

    def __exit__(self, *_: object) -> None:
        self.cleanup()
        shutil.rmtree(self.folder)

    def start(self) -> None:
        require(self.process is None, "one native command per dedicated supervisor")
        self.deadline = time.monotonic() + self.seconds
        self.process = subprocess.Popen(
            [sys.executable, "-I", "-B", str(SUPERVISOR), "--deadline-monotonic", str(self.deadline),
             "--receipt", str(self.receipt), "--", sys.executable, "-I", "-B", str(SELF),
             "--native-fixture", self.mode, str(self.folder)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
        )
        self.worker = identity(self.process.pid)
        require(self.worker is not None, "supervisor PID birth is retained before communication")
        self.births.append(self.worker)

    def record(self, name: str) -> dict:
        limit = min(self.deadline - 5, time.monotonic() + 2)
        path = self.folder / name
        while True:
            try:
                value = json.loads(path.read_bytes())
                require(type(value) is dict, "private fixture record is an object")
                return value
            except (FileNotFoundError, json.JSONDecodeError):
                require(time.monotonic() < limit, "fixture is ready before the command execution cutoff")
                time.sleep(0.005)

    def release(self) -> None:
        (self.folder / "release").write_bytes(b"go\n")

    def finish(self, payload: bytes = b"") -> tuple[bytes, dict]:
        require(self.process is not None, "supervisor was started")
        output, _ = self.process.communicate(payload, timeout=max(0.01, self.deadline - time.monotonic()) + 1)
        require(time.monotonic() < self.deadline + 1, "supervisor did not renew its absolute deadline")
        require(self.receipt.stat().st_size <= 65536, "bounded supervisor receipt")
        value = json.loads(self.receipt.read_bytes())
        require(value["format"] == "WOS_OWNED_COMMAND_V1", "the ownership receipt has the public versioned schema")
        require(value["deadline"] == self.deadline, "the original absolute deadline is preserved")
        require(value["leader_reaped"] is True and value["owned_session_absent"] is True and
                value["adopted_children_absent"] is True, "native leader, session and adopted children are settled")
        self.collect_births()
        for birth in self.births:
            require(not same_birth(birth), "each recorded native/supervisor PID birth is absent after completion")
        return output, value

    def collect_births(self) -> None:
        for name in ("native.json", "child.json", "zombie-confirmed.json"):
            try:
                value = json.loads((self.folder / name).read_bytes())
            except (FileNotFoundError, json.JSONDecodeError):
                continue
            pid, started = value["pid"], value["start_ticks"]
            require(type(pid) is int and pid > 1 and type(started) is int and started > 0, "actual native birth record")
            if not any(row[:2] == (pid, started) for row in self.births):
                self.births.append((pid, started, value.get("state", "?"), value.get("ppid", -1), value.get("sid", -1)))

    def spawn_sentinel(self) -> tuple[int, int, str, int, int]:
        self.sentinel = subprocess.Popen(
            [sys.executable, "-I", "-B", str(SELF), "--sentinel", str(self.folder)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
        )
        self.sentinel_birth = identity(self.sentinel.pid)
        require(self.sentinel_birth is not None, "unrelated sibling has a recorded PID birth")
        wait_file(self.folder / "sentinel.json", 2)
        return self.sentinel_birth

    def cleanup(self) -> None:
        # This test process is itself a subreaper. Its only new children are
        # the private worker/sentinel and descendants adopted from that worker.
        # Preserve any children that existed before this scenario.
        self.collect_births()
        if self.sentinel_birth is not None:
            self.births.append(self.sentinel_birth)
        deadline = time.monotonic() + 3
        while True:
            rows = process_rows()
            owned = {pid for pid, row in rows.items() if row[3] == os.getpid() and pid not in self.baseline}
            while True:
                descendants = {pid for pid, row in rows.items() if row[3] in owned}
                if descendants <= owned:
                    break
                owned |= descendants
            for pid in owned:
                signal_birth(rows[pid], signal.SIGKILL)
            for pid in owned:
                try:
                    os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    pass
            pending = [row for row in process_rows().values() if row[3] == os.getpid() and row[0] not in self.baseline]
            if not pending and not any(same_birth(row) for row in self.births):
                break
            require(time.monotonic() < deadline, "test-owned processes fully settle even after a failed assertion")
            time.sleep(0.01)
        for process in (self.process, self.sentinel):
            if process is not None:
                if process.returncode is None:
                    process.wait(timeout=0.01)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()


def test_binary_stdin_and_nonzero_output() -> None:
    with Scenario("stream") as case:
        case.start()
        output, receipt = case.finish(PAYLOAD)
        require(output == PAYLOAD + STDERR_BYTES, "stdin larger than a pipe and binary stdout/stderr pass through exactly")
        require(case.process.returncode == receipt["native_exit"] == 23 and receipt["errors"] == [], "settled native nonzero exit is preserved")


def test_confirmed_zombie_is_reaped() -> None:
    with Scenario("zombie") as case:
        case.start()
        output, receipt = case.finish()
        confirmed = json.loads((case.folder / "zombie-confirmed.json").read_bytes())
        require(confirmed["state"] == "Z", "native WNOWAIT confirmed a real unreaped zombie before orphaning")
        require(output == b"confirmed zombie\n" and case.process.returncode == receipt["native_exit"] == 0 and
                receipt["errors"] == [], "successful leader waits for orphan zombie reaping")


def test_native_signal_status_is_preserved() -> None:
    with Scenario("self-signal") as case:
        case.start()
        output, receipt = case.finish()
        require(output == b"" and case.process.returncode == receipt["native_exit"] == -signal.SIGKILL and
                receipt["errors"] == [], "settled native SIGKILL is mirrored without a signal-handler EINVAL")


def test_live_escaped_orphan_ignores_term() -> None:
    with Scenario("orphan") as case:
        case.start()
        child = case.record("child.json")
        birth = (child["pid"], child["start_ticks"], child["state"], child["ppid"], child["sid"])
        require(child["term_ignored"] is True and child["stdio_closed"] is True and child["sid"] == child["pid"],
                "real orphan escaped the native session without keeping output pipes alive")
        signal_birth(birth, signal.SIGTERM)
        time.sleep(0.03)
        require(live_birth(birth), "the actual escaped child survives SIGTERM")
        case.release()
        output, receipt = case.finish()
        require(output == b"native leader completed\n" and case.process.returncode == receipt["native_exit"] == 0 and
                receipt["errors"] == [], "successful native leader also settles a live escaped descendant")


def test_timeout_and_signal_cleanup() -> None:
    for cancel in (False, True):
        with Scenario("blocked", seconds=7) as case:
            case.start()
            case.record("child.json")
            if cancel:
                signal_birth(case.worker, signal.SIGTERM)
            _, receipt = case.finish()
            require(case.process.returncode != 0 and receipt["errors"], "timeout/cancellation remains a failure after cleanup")
            require(receipt["cancelled"] is cancel and receipt["timed_out"] is not cancel,
                    "actual timeout and actual cancellation remain distinct")


def test_unrelated_sibling_survives() -> None:
    with Scenario("orphan") as case:
        sentinel = case.spawn_sentinel()
        case.start()
        case.record("child.json")
        case.release()
        _, receipt = case.finish()
        require(case.process.returncode == 0 and receipt["errors"] == [], "owned command settles")
        require(live_birth(sentinel), "cleanup preserves an unrelated live sibling with its original PID birth")
        require(os.waitid(os.P_PID, sentinel[0], os.WEXITED | os.WNOHANG | os.WNOWAIT) is None,
                "the unrelated sibling remains live and waitable by its original parent")


def test_actual_pid_start_mismatch_is_refused() -> None:
    spec = importlib.util.spec_from_file_location("owned_command_under_test", SUPERVISOR)
    helper = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = helper
    spec.loader.exec_module(helper)
    with Scenario("stream") as case:
        sentinel = case.spawn_sentinel()
        observed = helper.process_identity(sentinel[0])
        require(observed[:2] == sentinel[:2], "the refusal test targets an actual live process")
        forged = (observed[0], observed[1] + 1, *observed[2:])
        try:
            helper.kill_identity(forged)
        except ValueError:
            pass
        else:
            raise AssertionError("mismatched PID birth was not explicitly refused")
        require(live_birth(sentinel), "PID-start refusal leaves the real process alive")
        require(os.waitid(os.P_PID, sentinel[0], os.WEXITED | os.WNOHANG | os.WNOWAIT) is None,
                "PID-start refusal preserves the original parent's wait authority")


def main() -> int:
    require(sys.platform.startswith("linux"), "actual supervisor regression requires Linux")
    require(all(hasattr(os, name) for name in ("fork", "waitid", "WNOWAIT", "pidfd_open")) and
            hasattr(signal, "pidfd_send_signal"), "actual Linux ownership primitives are available")
    libc = ctypes.CDLL(None, use_errno=True)
    require(libc.prctl(36, 1, 0, 0, 0) == 0, "test cleanup enables its own child subreaper")
    sys.dont_write_bytecode = True
    tests = (test_binary_stdin_and_nonzero_output, test_confirmed_zombie_is_reaped, test_native_signal_status_is_preserved,
             test_live_escaped_orphan_ignores_term, test_timeout_and_signal_cleanup,
             test_unrelated_sibling_survives, test_actual_pid_start_mismatch_is_refused)
    started = time.monotonic()
    for test in tests:
        test()
        print("PASS " + test.__name__, flush=True)
    require(time.monotonic() - started < 60, "all actual host supervision checks complete inside60s")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--native-fixture":
        raise SystemExit(native_fixture(sys.argv[2], Path(sys.argv[3])))
    if len(sys.argv) == 3 and sys.argv[1] == "--sentinel":
        save_birth(Path(sys.argv[2]), "sentinel.json")
        while True:
            time.sleep(0.05)
    require(len(sys.argv) == 1, "no unrecognized test options")
    raise SystemExit(main())
