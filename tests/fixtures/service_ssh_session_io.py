#!/usr/bin/env python3
"""Exercise broker-owned child identity, bounded relay, EOF and signal status."""
import argparse
import concurrent.futures
import contextlib
import time

import paramiko
from paramiko.common import cMSG_CHANNEL_REQUEST
from paramiko.message import Message

from service_ssh_broker_lifetime import connect, inspect, run

HELPER = "/tmp/goal19-session-io"


def drain(channel, expected_status, limit):
    output, error = bytearray(), bytearray()
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if channel.recv_ready():
            output.extend(channel.recv(32768))
        if channel.recv_stderr_ready():
            error.extend(channel.recv_stderr(32768))
        if len(output) > limit or len(error) > limit:
            raise AssertionError("unexpected unbounded output")
        if (channel.eof_received and channel.exit_status_ready()
                and not channel.recv_ready() and not channel.recv_stderr_ready()):
            break
        time.sleep(0.001)
    else:
        raise AssertionError("relay output timed out")
    status = channel.recv_exit_status()
    if status != expected_status:
        raise AssertionError(("exit status changed", status, bytes(output[:256]), bytes(error[:256])))
    return bytes(output), bytes(error)


def signal_request(transport, channel, name, want_reply=True):
    message = Message()
    message.add_byte(cMSG_CHANNEL_REQUEST)
    message.add_int(channel.remote_chanid)
    message.add_string("signal")
    message.add_boolean(want_reply)
    message.add_string(name)
    if want_reply:
        channel._event_pending()
    transport._send_user_message(message)
    if want_reply:
        channel._wait_for_event()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--cycles", type=int, default=32)
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent:
        key = agent.get_keys()[0]
        with connect(args, key) as transport:
            worker, _, broker, _ = inspect(transport)
            assert worker != broker
            print("PASS command ancestry is broker -> worker -> listener", flush=True)
            status, result = run(transport, HELPER + " fds")
            assert status == 0 and result == "unexpected_fds=0 uid=0 gid=0", result
            print("PASS no private descriptors inherited by exec", flush=True)
            payload = bytes(range(256)) * 4096
            with transport.open_session(timeout=10) as channel:
                channel.settimeout(30)
                channel.exec_command(HELPER + " duplex")
                def send():
                    channel.sendall(payload)
                    channel.shutdown_write()
                start = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    sender = pool.submit(send)
                    output, error = drain(channel, 37, len(payload))
                    sender.result(timeout=5)
                assert output == payload and error == payload
                print(f"PASS binary duplex stdout/stderr and stdin EOF ({time.monotonic()-start:.2f}s)", flush=True)
            with transport.open_session(window_size=32768, timeout=10) as slow:
                slow.settimeout(30)
                slow.exec_command(HELPER + " output")
                time.sleep(0.25)
                assert run(transport, "printf independent_channel") == (0, "independent_channel")
                transport.renegotiate_keys()
                output, error = drain(slow, 29, 2 * 1024 * 1024)
                assert output == b"O" * (2 * 1024 * 1024) and error == b"E" * (2 * 1024 * 1024)
            print("PASS backpressured channel permits another command and rekey; tail output preserved", flush=True)
            with transport.open_session(timeout=10) as channel:
                channel.settimeout(10)
                channel.exec_command(HELPER + " wait")
                assert channel.recv(6) == b"ready\n"
                signal_request(transport, channel, b"TERM\0suffix", want_reply=False)
                assert run(transport, "printf after_bad_signal") == (0, "after_bad_signal")
                assert not channel.exit_status_ready()
                signal_request(transport, channel, "TERM")
                assert drain(channel, -1, 1024) == (b"", b"")
            print("PASS broker-owned signal delivery and exit-signal", flush=True)
            start = time.monotonic()
            assert run(transport, "sleep 5 & printf background_tail") == (0, "background_tail")
            assert time.monotonic() - start < 3
            print("PASS exited shell drains available output without waiting for background child", flush=True)
            for cycle in range(args.cycles):
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(10)
                    channel.exec_command(HELPER + " wait")
                    ready = channel.recv(6)
                    if ready != b"ready\n":
                        error = channel.makefile_stderr("rb").read(4096)
                        raise AssertionError(("child startup", cycle, ready, error, channel.recv_exit_status()))
                    signal_request(transport, channel, "TERM")
                    result = drain(channel, -1, 1024)
                    assert result == (b"", b""), ("signal drain", cycle, result)
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(10)
                    channel.exec_command("sleep 1 & printf background_tail")
                    result = drain(channel, 0, 4096)
                    assert result == (b"background_tail", b""), (cycle, result)
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(10)
                    channel.exec_command("printf after_sessions")
                    result = drain(channel, 0, 4096)
                    assert result == (b"after_sessions", b""), (cycle, result)
            print(f"PASS {args.cycles} signal/background/channel-reuse cycles", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
