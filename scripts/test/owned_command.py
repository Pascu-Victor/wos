#!/usr/bin/env python3
"""Run one stdin-capable host command and settle its descendants before exit.

The caller owns the worker's private session and the absolute deadline. Native
stdin/stdout/stderr are inherited unchanged; this worker never pumps or prints
success data. A retained native leader reserves its SID until descendant cleanup
is complete. Linux pidfds and a dedicated child-subreaper are required.
"""
from __future__ import annotations

import argparse
import ctypes
import errno
import json
import math
import os
from pathlib import Path
import signal
import stat
import sys
import time


Identity = tuple[int, int, str, int, int]
CLEANUP_SECONDS = 5.0
CANCEL_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
RECEIPT_LIMIT = 32 * 1024


class CommandCancelled(Exception):
    def __init__(self, signum: int):
        self.signum = signum
        super().__init__(f"owned command cancelled by signal {signum}")


def process_identity(pid: int) -> Identity:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def kill_identity(identity: Identity) -> None:
    """Signal only the captured birth through a pidfd; never accept PID reuse."""
    pid, started, _, _, _ = identity
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        try:
            current = process_identity(pid)
        except (FileNotFoundError, ProcessLookupError):
            return
        if current[1] != started:
            raise ValueError("owned process birth changed before pidfd signal")
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        except ProcessLookupError:
            pass
    finally:
        os.close(fd)


def owned_processes(leader: int | None) -> list[Identity]:
    """The worker has no other children; after leader reap use direct children only."""
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        if identity[3] == os.getpid() or (leader is not None and identity[4] == leader):
            result.append(identity)
    return result


def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("owned command requires Linux child-subreaper and pidfds")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    # Refuse unsupported pidfds before creating a native process.
    fd = os.pidfd_open(os.getpid(), 0)
    try:
        signal.pidfd_send_signal(fd, 0)
    finally:
        os.close(fd)


def settle(leader: int, birth: Identity, deadline: float) -> int:
    """Keep the native leader unreaped until every other owned process is gone."""
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("owned descendants not settled by original deadline")
        current = process_identity(leader)
        if current[1] != birth[1]:
            raise ValueError("native leader birth changed while reserved")
        identities = owned_processes(leader)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == leader:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                # A live descendant still has its own parent; it remains in the
                # session census until death/reparenting permits the worker reap.
                pass
        remaining = owned_processes(leader)
        if len(remaining) == 1 and remaining[0][0] == leader and remaining[0][2] == "Z":
            if remaining[0][1] != birth[1]:
                raise ValueError("terminal native leader birth changed")
            # WNOWAIT observes the exact child but never consumes it early.
            terminal = os.waitid(os.P_PID, leader, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if terminal is None:
                raise ValueError("native leader was not waitable while reserved")
            pid, status = os.waitpid(leader, 0)  # Sole native-leader reap.
            if pid != leader:
                raise ValueError("wrong native leader reaped")
            native_exit = os.waitstatus_to_exitcode(status)
            # The released SID is never scanned or signalled again.
            if owned_processes(None):
                raise ValueError("adopted children remain after native leader reap")
            try:
                os.waitid(os.P_ALL, 0, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            except ChildProcessError:
                return native_exit
            raise ValueError("native child ownership not empty after sole leader reap")
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))


def spawn(command: list[str], original_mask: set[signal.Signals]) -> int:
    """Use a PID-only child handle: no destructor can consume its wait status."""
    if hasattr(os, "POSIX_SPAWN_CLOSEFROM"):
        actions = [(os.POSIX_SPAWN_CLOSEFROM, 3)]
    else:
        actions = []
        for name in os.listdir("/proc/self/fd"):
            if not name.isdecimal() or int(name) <= 2:
                continue
            fd = int(name)
            try:
                os.fstat(fd)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise
                continue  # The enumeration's own directory FD is already closed.
            actions.append((os.POSIX_SPAWN_CLOSE, fd))
    defaults = set(CANCEL_SIGNALS)
    for name in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
        if hasattr(signal, name):
            defaults.add(getattr(signal, name))
    return os.posix_spawnp(command[0], command, dict(os.environ), file_actions=actions,
                           setsid=True, setsigmask=original_mask, setsigdef=defaults)


def receipt_path(value: str) -> Path:
    path = Path(value)
    if (not path.is_absolute() or ".." in path.parts or path.exists() or path.is_symlink()
            or path.parent.resolve(strict=True) != path.parent):
        raise ValueError("receipt must be fresh in a canonical private directory")
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 0o077:
        raise ValueError("receipt parent must be private and owned by the worker UID")
    return path


def publish_receipt(path: Path | None, result: dict, deadline: float) -> None:
    if path is None:
        return
    if time.monotonic() >= deadline:
        raise TimeoutError("original deadline before ownership receipt")
    raw = json.dumps(result, separators=(",", ":")).encode() + b"\n"
    if len(raw) > RECEIPT_LIMIT:
        raise ValueError("ownership receipt exceeds32KiB")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
    if time.monotonic() >= deadline:
        raise TimeoutError("original deadline after ownership receipt")


def run(command: list[str], deadline: float, receipt: Path | None = None) -> tuple[dict, int]:
    if not command:
        raise ValueError("native command is required")
    started = time.monotonic()
    if not math.isfinite(deadline) or started >= deadline - CLEANUP_SECONDS:
        raise TimeoutError("original deadline leaves no native execution and cleanup reserve")
    if owned_processes(None):
        raise ValueError("dedicated worker must have no pre-existing children")
    subreaper()
    native_pid = None
    birth = None
    leader_fd = None
    cancelled = None
    timed_out = False
    errors = []
    native_exit = None
    settled = False
    handlers = {}

    def interrupted(signum, _frame):
        raise CommandCancelled(signum)

    for sig in CANCEL_SIGNALS:
        handlers[sig] = signal.signal(sig, interrupted)
    try:
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, CANCEL_SIGNALS)
        try:
            # No Python post-fork callback, child PIPE or reaping object exists.
            native_pid = spawn(command, blocked)
            leader_fd = os.pidfd_open(native_pid, 0)
            birth = process_identity(native_pid)
            if birth[3] != os.getpid() or birth[4] != native_pid:
                raise ValueError("native process is not the registered child/session leader")
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        while True:
            if time.monotonic() >= deadline - CLEANUP_SECONDS:
                timed_out = True
                raise TimeoutError("native execution reached original cleanup reserve")
            terminal = os.waitid(os.P_PID, native_pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if terminal is not None:
                break
            time.sleep(min(0.01, max(0.0, deadline - CLEANUP_SECONDS - time.monotonic())))
    except CommandCancelled as error:
        cancelled = error.signum
        errors.append(str(error))
    except BaseException as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        for sig in CANCEL_SIGNALS:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if native_pid is not None:
                if birth is None:
                    # An unreaped direct child still reserves this PID; recover
                    # only this worker's child identity if registration failed.
                    birth = process_identity(native_pid)
                    if birth[3] != os.getpid() or birth[4] != native_pid:
                        raise ValueError("failed native registration has foreign identity")
                native_exit = settle(native_pid, birth, deadline)
                settled = True
        except BaseException as error:
            errors.append(f"settlement {type(error).__name__}: {error}")
        finally:
            if leader_fd is not None:
                os.close(leader_fd)
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    worker_exit = (128 + cancelled) if cancelled is not None else (124 if timed_out else (125 if errors or not settled else native_exit))
    result = dict(format="WOS_OWNED_COMMAND_V1", native_argv=command,
        native_pid=native_pid,
        start_ticks=birth[1] if birth is not None else None, native_exit=native_exit,
        leader_reaped=settled, owned_session_absent=settled, adopted_children_absent=settled,
        errors=errors, cancelled=cancelled is not None, timed_out=timed_out,
        deadline=deadline, started=started, seconds=time.monotonic() - started,
        cleanup_reserve_seconds=CLEANUP_SECONDS, native_stdio_inherited=True,
        planned_worker_exit=worker_exit)
    try:
        publish_receipt(receipt, result, deadline)
    except BaseException as error:
        errors.append(f"receipt {type(error).__name__}: {error}")
        worker_exit = 125
    return result, worker_exit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadline-monotonic", type=float, required=True)
    parser.add_argument("--receipt", type=receipt_path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    result, code = run(command, args.deadline_monotonic, args.receipt)
    if result["errors"]:
        print("owned command: " + "; ".join(result["errors"]), file=sys.stderr)
    if code < 0:
        # Mirror a native signal only after its whole process forest was settled.
        sig = -code
        if sig != signal.SIGKILL:
            signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)
        return 128 + sig
    return code


if __name__ == "__main__":
    raise SystemExit(main())
