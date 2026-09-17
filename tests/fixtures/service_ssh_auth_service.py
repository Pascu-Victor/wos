#!/usr/bin/env python3
"""Probe an explicitly selected test VM's SSH auth-service boundary.

Requires host Paramiko. Sends only unauthenticated 'none' requests, never keys
or passwords. Every case uses a fresh connection and a bounded timeout.
"""

import argparse
import socket
import threading
import time

import paramiko
from paramiko.auth_handler import AuthHandler
from paramiko.common import cMSG_USERAUTH_REQUEST
from paramiko.message import Message


class ProbeTransport(paramiko.Transport):
    def __init__(self, sock):
        super().__init__(sock)
        self.disconnect = None

    def _parse_disconnect(self, message):
        self.disconnect = (message.get_int(), message.get_text())


class ProbeAuth(AuthHandler):
    def __init__(self, transport, service):
        super().__init__(transport)
        self.service = service
        self.reply = None

    def _parse_service_accept(self, message):
        if message.get_text() != "ssh-userauth":
            raise paramiko.SSHException("unexpected service acceptance")
        request = Message()
        request.add_byte(cMSG_USERAUTH_REQUEST)
        request.add_string(self.username)
        request.add_string(self.service)
        request.add_string("none")
        self.transport._send_message(request)

    def _parse_userauth_failure(self, message):
        self.reply = "failure"
        super()._parse_userauth_failure(message)

    def _parse_userauth_success(self, message):
        self.reply = "success"
        super()._parse_userauth_success(message)


def probe(args, service):
    with socket.create_connection((args.host, args.port), args.timeout) as sock:
        with ProbeTransport(sock) as transport:
            transport.start_client(timeout=args.timeout)
            if not transport.is_active() or not transport.initial_kex_done:
                raise RuntimeError("key exchange did not complete")
            auth = ProbeAuth(transport, service)
            transport.auth_handler = auth
            event = threading.Event()
            auth.auth_none(args.username, event)
            if not event.wait(args.timeout):
                raise TimeoutError("authentication response timed out")
            if auth.reply is not None:
                return auth.reply
            if transport.disconnect is not None:
                code, description = transport.disconnect
                if "unknown service in auth" in description:
                    return "service-disconnect"
                return f"other-disconnect({code}, {description!r})"
            error = transport.get_exception()
            if isinstance(error, EOFError):
                return "closed"
            raise RuntimeError(f"connection closed unexpectedly: {error!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", help="address of the disposable WOS test VM")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--username", default="root")
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    cases = [
        ("valid", b"ssh-connection", "failure"),
        ("same-length-wrong", b"bad-connection", "closed"),
        ("same-length-nul", b"ssh-connectio\0", "closed"),
        ("prefix-suffix", b"ssh-connection-extra", "closed"),
        ("prefix-nul", b"ssh-connection\0", "closed"),
        ("empty", b"", "closed"),
        ("short", b"ssh-connectio", "closed"),
        ("long-wrong", b"invalid-connection", "closed"),
        ("valid-after-denials", b"ssh-connection", "failure"),
    ]
    failed = 0
    for name, service, expected in cases:
        try:
            actual = probe(args, service)
        except Exception as error:
            actual = f"ERROR {type(error).__name__}: {error}"
        passed = actual == expected
        failed += not passed
        print(f"{'PASS' if passed else 'FAIL'} {name}: {actual}; expected {expected}", flush=True)
        # Allow the listener to reclaim each child's pre-auth slot. Avoid
        # turning this into a connection-limit test.
        time.sleep(2)
    print(f"SSH auth service: {len(cases)} cases, {failed} failed", flush=True)
    return int(failed != 0)


if __name__ == "__main__":
    raise SystemExit(main())
