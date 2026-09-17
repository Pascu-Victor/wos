#!/usr/bin/env python3
"""Verify broker PTY admission, ownership, relay, resize and cleanup in WOS."""
import argparse
import concurrent.futures
import contextlib
import struct
import time

import paramiko
from paramiko.common import cMSG_CHANNEL_REQUEST
from paramiko.message import Message

from service_ssh_broker_lifetime import connect, inspect, run
from service_ssh_session_io import drain, signal_request

HELPER = "/tmp/goal19-session-pty"
# Disable input transforms, software flow control, canonical/echo/signal modes
# and output postprocessing for exact binary exchanges. Keep VMIN=1 defaults.
RAW_MODES = b"".join(bytes([code]) + struct.pack(">I", 0)
                     for code in (30, 31, 32, 33, 34, 35, 36, 38, 39, 40, 41, 50, 51, 53, 54, 55, 56, 59, 70)) + b"\0"


def body(term=b"goal19-raw", modes=RAW_MODES):
    message = Message()
    message.add_string(term)
    for value in (80, 24, 640, 480):
        message.add_int(value)
    message.add_string(modes)
    return message.asbytes()


def request(transport, channel, kind, payload, want_reply=True):
    message = Message()
    message.add_byte(cMSG_CHANNEL_REQUEST)
    message.add_int(channel.remote_chanid)
    message.add_string(kind)
    message.add_boolean(want_reply)
    message.add_bytes(payload)
    if want_reply:
        channel._event_pending()
    transport._send_user_message(message)
    if want_reply:
        channel._wait_for_event()


def line(channel):
    result = bytearray()
    while not result.endswith(b"\n"):
        byte = channel.recv(1)
        if not byte or len(result) >= 4096:
            raise AssertionError(("PTY line truncated", bytes(result), channel.recv_exit_status()))
        result.extend(byte)
    return bytes(result)


def snapshot(channel):
    fields = dict(item.split("=", 1) for item in line(channel).decode().strip().split())
    assert fields["mode"] == "600" and fields["fds"] == "0", fields
    assert fields["pid"] == fields["sid"] == fields["pgrp"] == fields["foreground"], fields
    assert fields["term"] == "goal19-raw", fields
    assert [fields[name] for name in ("cols", "rows", "x", "y")] == ["80", "24", "640", "480"], fields
    assert all(fields[name] == "0" for name in ("echo", "canonical", "opost")), fields
    return fields


def wait_absent(sftp, tty):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            sftp.stat(tty)
        except FileNotFoundError:
            return
        time.sleep(0.02)
    raise AssertionError(("released PTY remains", tty))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--cycles", type=int, default=32)
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent:
        key = agent.get_keys()[0]
        # No earlier SFTP/non-PTY spawn may enlarge the broker's FD close bound.
        with connect(args, key) as first:
            with first.open_session(timeout=10) as channel:
                channel.settimeout(15)
                request(first, channel, "pty-req", body())
                channel.exec_command(HELPER + " inspect")
                snapshot(channel)
                assert drain(channel, 0, 4096) == (b"", b"")
        print("PASS first-channel PTY inherits no broker descriptors", flush=True)
        with connect(args, key) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            worker, _, broker, _ = inspect(transport)
            with transport.open_session(timeout=10) as channel:
                channel.settimeout(15)
                request(transport, channel, "pty-req", body())
                channel.exec_command("/tmp/goal19-broker-lifetime inspect")
                output, error = drain(channel, 0, 4096)
                fields = list(map(int, output.split()))
                assert fields[0] == worker and fields[2] == broker and not error, (output, error)
            print("PASS PTY child belongs to the same broker as non-PTY commands", flush=True)

            with transport.open_session(timeout=10) as channel:
                channel.settimeout(15)
                request(transport, channel, "pty-req", body())
                # A duplicate request must preserve the already-owned terminal.
                request(transport, channel, "pty-req", body(b"wrong-terminal"), False)
                channel.exec_command(HELPER + " resize")
                fields = snapshot(channel)
                tty = fields["tty"]
                assert fields["uid"] == fields["gid"] == "0", fields
                request(transport, channel, "pty-req", body(b"post-start"), False)
                # Trailing and truncated resize bodies must not mutate the PTY.
                request(transport, channel, "window-change", struct.pack(">4I", 1, 2, 3, 4) + b"x", False)
                request(transport, channel, "window-change", b"\0" * 15, False)
                request(transport, channel, b"window-change\0suffix", struct.pack(">4I", 1, 2, 3, 4), False)
                channel.sendall(b"r")
                assert line(channel) == b"resized=80,24,640,480\n"
                request(transport, channel, "window-change", struct.pack(">4I", 123, 45, 678, 901))
                channel.sendall(b"r")
                assert drain(channel, 17, 4096) == (b"resized=123,45,678,901\n", b"")
            wait_absent(sftp, tty)
            print("PASS private slave ownership, controlling tty, modes, metadata, FD closure and resize", flush=True)

            malformed = [
                ("pty-req", body(b"bad\0suffix")),
                ("pty-req", body(b"T" * 1025)),
                ("pty-req", body(modes=b"\x35\0\0")),
                ("pty-req", body(modes=b"\x35\0\0\0\0")),
                ("pty-req", body() + b"trailing"),
                ("pty-req", body()[:-1]),
                ("pty-req", b"\0\0\0\x01T"),
                (b"pty-req\0suffix", body()),
            ]
            for kind, payload in malformed:
                before = set(sftp.listdir("/dev/pts"))
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(10)
                    request(transport, channel, kind, payload, False)
                    channel.exec_command("printf unchanged")
                    assert drain(channel, 0, 4096) == (b"unchanged", b"")
                assert set(sftp.listdir("/dev/pts")) == before
            print(f"PASS {len(malformed)} malformed PTY bodies/names allocate no terminal and preserve command admission", flush=True)

            for modes in (b"", b"\x00ignored", b"\xa0reserved", RAW_MODES):
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(10)
                    request(transport, channel, "pty-req", body(modes=modes))
                    channel.exec_command("printf modes_accepted")
                    assert drain(channel, 0, 4096) == (b"modes_accepted", b"")
            print("PASS empty modes, END and future-opcode termination remain accepted", flush=True)

            with transport.open_session(timeout=10) as channel:
                channel.settimeout(15)
                channel.get_pty()
                channel.invoke_shell()
                channel.sendall(b"printf 'PTY_%s_READY\\n' SHELL; exit 0\n")
                output, error = drain(channel, 0, 32768)
                assert b"PTY_SHELL_READY" in output and not error, (output, error)
            print("PASS login shell executes through the shared controlling-terminal setup", flush=True)

            # Abandon PTYs both within a connection and at connection EOF.
            before = set(sftp.listdir("/dev/pts"))
            for _ in range(args.cycles):
                with transport.open_session(timeout=10) as channel:
                    request(transport, channel, "pty-req", body())
                assert run(transport, "printf released") == (0, "released")
                assert set(sftp.listdir("/dev/pts")) == before
            with connect(args, key) as target:
                abandoned = target.open_session(timeout=10)
                request(target, abandoned, "pty-req", body())
                added = set(sftp.listdir("/dev/pts")) - before
                assert len(added) == 1, added
            wait_absent(sftp, "/dev/pts/" + added.pop())
            with connect(args, key) as target:
                active = target.open_session(timeout=10)
                active.settimeout(15)
                request(target, active, "pty-req", body())
                active.exec_command(HELPER + " wait")
                tty = snapshot(active)["tty"]
            wait_absent(sftp, tty)
            print(f"PASS {args.cycles} abandoned PTY/channel cycles plus allocated and running connection EOF cleanup", flush=True)

            payload = bytes(range(256)) * 1024
            with transport.open_session(window_size=32768, timeout=10) as channel:
                channel.settimeout(30)
                request(transport, channel, "pty-req", body())
                channel.exec_command(HELPER + " duplex")
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    sender = pool.submit(channel.sendall, payload)
                    output, error = drain(channel, 37, len(payload) + 13)
                    sender.result(timeout=5)
                assert output == payload + b"merged-stderr" and not error, (len(output), len(error))
            print("PASS 256 KiB raw duplex and merged stderr", flush=True)
            with transport.open_session(window_size=32768, timeout=10) as channel:
                channel.settimeout(30)
                request(transport, channel, "pty-req", body())
                channel.exec_command(HELPER + " output")
                time.sleep(0.25)
                assert run(transport, "printf independent") == (0, "independent")
                transport.renegotiate_keys()
                assert drain(channel, 29, 2 * 1024 * 1024) == (b"P" * (2 * 1024 * 1024), b"")
            print("PASS backpressured PTY preserves complete output, independent commands and rekey", flush=True)
            # Broker EOF closes the shared master. Verify the hangup itself,
            # clean EOF when ignored, and independent explicit TERM handling.
            for mode, sig, status in (("wait", False, -1), ("wait-hup", False, 41),
                                      ("wait-ignore-hup", False, 0), ("wait", True, -1)):
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(15)
                    request(transport, channel, "pty-req", body())
                    channel.exec_command(HELPER + " " + mode)
                    tty = snapshot(channel)["tty"]
                    if sig:
                        signal_request(transport, channel, "TERM")
                    else:
                        channel.shutdown_write()
                    assert drain(channel, status, 4096) == (b"", b"")
                wait_absent(sftp, tty)
            print("PASS PTY EOF, broker-owned signal/exit status and final descriptor cleanup", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
