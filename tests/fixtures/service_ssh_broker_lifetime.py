#!/usr/bin/env python3
"""Run with the native lifecycle fixture installed in the disposable WOS VM."""

import argparse
import contextlib
import socket
import time

import paramiko

HELPER = "/tmp/goal19-broker-lifetime"


@contextlib.contextmanager
def connect(args, key):
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = 10
            transport.start_client(timeout=10)
            transport.auth_publickey(args.username, key)
            if not transport.is_authenticated():
                raise RuntimeError("authorized agent login required")
            yield transport


def run(transport, command):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(10)
        channel.exec_command(command)
        output = bytearray()
        while chunk := channel.recv(4096):
            output.extend(chunk)
            if len(output) > 4096:
                raise RuntimeError("unexpected fixture output")
        return channel.recv_exit_status(), output.decode().strip()


def inspect(transport):
    status, output = run(transport, HELPER + " inspect")
    fields = output.split()
    if status != 0 or len(fields) != 4 or not all(field.isdecimal() for field in fields):
        raise RuntimeError(f"broker identity inspection failed: {status}, {output!r}")
    return list(map(int, fields))


def wait_gone(control, pid, start):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if run(control, f"{HELPER} gone {pid} {start}")[0] == 0:
            return
        time.sleep(0.1)
    raise RuntimeError("broker remained after connection teardown")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    args = parser.parse_args()
    agent = paramiko.Agent()
    try:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as control:
            with connect(args, keys[0]) as target:
                worker, worker_start, broker, broker_start = inspect(target)
                invalid = f"{HELPER} terminate {worker} {worker_start} {broker} {broker_start + 1}"
                if run(control, invalid)[0] != 1 or run(target, "printf still_active") != (0, "still_active"):
                    raise RuntimeError("mismatched identity did not preserve the connection")
                print("PASS stale broker identity rejected; connection retained", flush=True)
                # Keep a prepared command alive while its broker disappears.
                # Whole-session cleanup must not issue a release RPC to it.
                active = target.open_session(timeout=10)
                active.settimeout(10)
                active.exec_command("read broker_fixture_input")
                valid = f"{HELPER} terminate {worker} {worker_start} {broker} {broker_start}"
                if run(control, valid)[0] != 0:
                    raise RuntimeError("matched broker termination failed")
                deadline = time.monotonic() + 10
                while target.is_active() and time.monotonic() < deadline:
                    time.sleep(0.05)
                if target.is_active():
                    raise RuntimeError("connection survived loss of its broker")
                wait_gone(control, broker, broker_start)
                active.close()
                print("PASS broker termination closes an active command connection and reaps broker", flush=True)
            with connect(args, keys[0]) as target:
                _, _, broker, broker_start = inspect(target)
            wait_gone(control, broker, broker_start)
            print("PASS normal connection EOF releases broker", flush=True)
        return 0
    finally:
        agent.close()


if __name__ == "__main__":
    raise SystemExit(main())
