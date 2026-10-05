#!/usr/bin/env python3
"""Qualify the event-driven WOS SSH broker using an authorized agent key."""

import argparse
import json
import secrets
import shlex
import socket
import statistics
import time

import paramiko

from service_ssh_session_io import signal_request


def collect(channel, timeout):
    output, error = bytearray(), bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        while channel.recv_ready():
            output.extend(channel.recv(32768))
        while channel.recv_stderr_ready():
            error.extend(channel.recv_stderr(32768))
        if channel.exit_status_ready() and (channel.eof_received or channel.closed):
            if not channel.recv_ready() and not channel.recv_stderr_ready():
                return channel.recv_exit_status(), bytes(output), bytes(error)
        time.sleep(0.002)
    raise TimeoutError("broker command did not deliver EOF and exit status")


def execute(transport, command, timeout, expected):
    with transport.open_session(timeout=timeout) as channel:
        channel.settimeout(timeout)
        channel.exec_command(command)
        result = collect(channel, timeout)
        if result != expected:
            raise RuntimeError(f"unexpected command result: status={result[0]}, stdout={len(result[1])}, stderr={len(result[2])}")


def measure_latency(transport, samples, timeout):
    code = """import sys,termios
attributes=termios.tcgetattr(0)
attributes[3]&=~termios.ECHO
attributes[1]&=~termios.OPOST
termios.tcsetattr(0,termios.TCSANOW,attributes)
print('latency_ready',flush=True)
for line in sys.stdin.buffer:
 if line==b'quit\\n':break
 sys.stdout.buffer.write(line)
 sys.stdout.buffer.flush()
"""
    with transport.open_session(timeout=timeout) as channel:
        channel.settimeout(timeout)
        channel.get_pty()
        channel.exec_command("exec python3 -u -c " + shlex.quote(code))
        pending = bytearray()

        def receive_line():
            while b"\n" not in pending:
                data = channel.recv(4096)
                if not data:
                    raise RuntimeError("interactive latency channel closed")
                pending.extend(data)
            line, _, rest = pending.partition(b"\n")
            pending[:] = rest
            return bytes(line) + b"\n"

        if receive_line() != b"latency_ready\n":
            raise RuntimeError("interactive latency helper did not become ready")
        elapsed = []
        for index in range(samples + 3):
            marker = f"latency_{index}\n".encode()
            started = time.perf_counter_ns()
            channel.sendall(marker)
            if receive_line() != marker:
                raise RuntimeError("interactive latency echo mismatch")
            if index >= 3:
                elapsed.append((time.perf_counter_ns() - started) / 1e6)
        channel.sendall(b"quit\n")
        result = collect(channel, timeout)
        if result != (0, b"", b""):
            raise RuntimeError(f"interactive latency helper failed to exit: {result!r}")
    elapsed.sort()
    result = {"samples": samples, "median_ms": statistics.median(elapsed),
              "p99_ms": elapsed[max(0, (99 * samples + 99) // 100 - 1)], "max_ms": elapsed[-1]}
    print("PTY latency: " + json.dumps(result, sort_keys=True), flush=True)


def exercise(args, key):
    with socket.create_connection((args.host, args.port), args.timeout) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = args.timeout
            transport.start_client(timeout=args.timeout)
            transport.auth_publickey(args.username, key)
            if not transport.is_authenticated():
                raise RuntimeError("authorized agent login required")
            execute(transport, "printf broker_ready", args.timeout, (0, b"broker_ready", b""))
            execute(transport, "exit 23", args.timeout, (23, b"", b""))
            print("PASS output and silent child exit status", flush=True)

            with transport.open_session(timeout=args.timeout) as channel:
                channel.settimeout(args.timeout)
                channel.exec_command("sleep 1; cat")
                data = b"input-backpressure\n" * 16384
                channel.sendall(data)
                channel.shutdown_write()
                result = collect(channel, args.timeout)
                if result != (0, data, b""):
                    raise RuntimeError(f"stdin/EOF echo mismatch: status={result[0]}, stdout={len(result[1])}/{len(data)}, stderr={result[2]!r}")
            print("PASS blocked child stdin, sustained input and EOF", flush=True)

            with transport.open_session(window_size=65536, timeout=args.timeout) as channel:
                channel.settimeout(args.timeout)
                channel.exec_command("python3 -c 'import sys; sys.stdout.buffer.write(b\"x\"*524288); sys.stdout.flush(); sys.stderr.buffer.write(b\"e\"*131072); sys.stderr.flush()'")
                time.sleep(0.3)  # Intentionally exercise a slow SSH receiver.
                if collect(channel, args.timeout) != (0, b"x" * 524288, b"e" * 131072):
                    raise RuntimeError("slow-reader stdout/stderr mismatch")
            print("PASS bounded-window slow reader and stdout/stderr transfer", flush=True)

            channels = [transport.open_session(timeout=args.timeout) for _ in range(3)]
            try:
                for index, channel in enumerate(channels):
                    channel.settimeout(args.timeout)
                    channel.exec_command(f"sleep 1; printf multi{index}; exit {index}")
                session_id = transport.session_id
                transport.renegotiate_keys()
                if transport.session_id != session_id:
                    raise RuntimeError("session identity changed across rekey")
                for index, channel in enumerate(channels):
                    if collect(channel, args.timeout) != (index, f"multi{index}".encode(), b""):
                        raise RuntimeError("multiplexed channel/rekey mismatch")
            finally:
                for channel in channels:
                    channel.close()
            print("PASS multiplexed channels and rekey during active commands", flush=True)

            with transport.open_session(timeout=args.timeout) as channel:
                channel.settimeout(args.timeout)
                channel.exec_command("exec python3 -c 'import signal,sys,time; signal.signal(signal.SIGTERM,lambda *_:sys.exit(17)); print(\"signal_ready\",flush=True); time.sleep(30)'")
                ready = bytearray()
                while b"signal_ready\n" not in ready:
                    ready.extend(channel.recv(4096))
                signal_request(transport, channel, "TERM")
                if collect(channel, args.timeout) != (17, b"", b""):
                    raise RuntimeError("broker signal/exit delivery mismatch")
            start = time.monotonic()
            execute(transport, "sleep 5 & printf background_tail", args.timeout, (0, b"background_tail", b""))
            if time.monotonic() - start >= 3:
                raise RuntimeError("exited shell waited for its background descendant")
            print("PASS broker signal and background descendant exit", flush=True)

            with transport.open_session(timeout=args.timeout) as channel:
                channel.settimeout(args.timeout)
                channel.get_pty(width=80, height=24)
                size_command = "python3 -c 'import os; s=os.get_terminal_size(0); print(s.lines,s.columns,flush=True)'"
                channel.exec_command(f"{size_command}; read marker; {size_command}; printf pty_done; exit 9")
                prefix = bytearray()
                deadline = time.monotonic() + args.timeout
                while b"24 80" not in prefix:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("PTY initial output missing")
                    try:
                        prefix.extend(channel.recv(4096))
                    except TimeoutError as exc:
                        raise TimeoutError(f"PTY initial output missing: {bytes(prefix)!r}") from exc
                channel.resize_pty(width=120, height=40)
                channel.sendall(b"resize\n")
                status, output, error = collect(channel, args.timeout)
                if status != 9 or b"40 120" not in output or b"pty_done" not in output or error:
                    raise RuntimeError("PTY resize/output/exit mismatch")
            print("PASS PTY input, resize, output and exit status", flush=True)

            with paramiko.SFTPClient.from_transport(transport) as sftp:
                path = f"/tmp/wos-broker-readiness-{secrets.token_hex(8)}.bin"
                data = bytes(range(256)) * 2048
                try:
                    with sftp.open(path, "wx") as file:
                        file.write(data)
                    with sftp.open(path, "rb") as file:
                        if file.read() != data:
                            raise RuntimeError("SFTP binary round-trip mismatch")
                finally:
                    sftp.remove(path)
            print("PASS SFTP binary round-trip", flush=True)

            if args.latency_samples:
                measure_latency(transport, args.latency_samples, args.timeout)

            if args.idle_seconds:
                with transport.open_session(timeout=args.timeout) as channel:
                    channel.settimeout(args.timeout)
                    channel.get_pty()
                    channel.exec_command("printf idle_ready; read marker; printf idle_done")
                    output = bytearray()
                    while b"idle_ready" not in output:
                        output.extend(channel.recv(4096))
                    print(f"IDLE PTY ready for {args.idle_seconds}s CPU sample at monotonic={time.monotonic()}", flush=True)
                    time.sleep(args.idle_seconds)
                    channel.sendall(b"wake\n")
                    status, output, _ = collect(channel, args.timeout)
                    if status != 0 or b"idle_done" not in output:
                        raise RuntimeError("idle PTY did not wake")
                print(f"PASS idle PTY wake and disconnect at monotonic={time.monotonic()}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--username", default="root")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--idle-seconds", type=float, default=0)
    parser.add_argument("--latency-samples", type=int, default=0)
    args = parser.parse_args()
    if not 0 <= args.latency_samples <= 10000:
        parser.error("latency samples must be 0..10000")
    agent = paramiko.Agent()
    try:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        exercise(args, keys[0])
    finally:
        agent.close()


if __name__ == "__main__":
    main()
