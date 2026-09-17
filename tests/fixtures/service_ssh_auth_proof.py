#!/usr/bin/env python3
"""Exercise login proof framing on an explicitly selected disposable WOS VM.

Requires Paramiko and an SSH agent with an authorized Ed25519 key. The agent
signs requests; this fixture never reads or exports a private key. No command
is executed after authentication. Replay bytes remain in memory for this run.
"""

import argparse
import socket
import struct
import threading
import time

import paramiko
from paramiko.common import cMSG_USERAUTH_REQUEST
from paramiko.message import Message

from service_ssh_auth_service import ProbeAuth, ProbeTransport


class ProofAuth(ProbeAuth):
    def __init__(self, transport, key, case, replay):
        super().__init__(transport, b"ssh-connection")
        self.key = key
        self.case = case
        self.replay = replay

    def _parse_service_accept(self, message):
        if message.get_text() != "ssh-userauth":
            raise paramiko.SSHException("unexpected service acceptance")
        request = Message()
        request.add_byte(cMSG_USERAUTH_REQUEST)
        request.add_string(self.username)
        request.add_string(self.service)
        request.add_string("publickey")
        request.add_boolean(True)
        request.add_string(self.key.get_name())
        request.add_string(self.key.asbytes())
        transcript = Message()
        session_id = self.transport.session_id
        if self.case == "wrong-session":
            session_id = bytes(len(session_id))
        transcript.add_string(session_id)
        transcript.add_bytes(request.asbytes())
        signature = self.key.sign_ssh_data(transcript.asbytes())
        if self.case == "valid":
            self.replay.append(signature)
        elif self.case == "replay":
            signature = self.replay[0]
        elif self.case == "bad-signature":
            signature = signature[:-1] + bytes([signature[-1] ^ 1])
        elif self.case == "inner-trailing":
            signature += b"unsigned"
        size = len(signature)
        if self.case == "outer-zero":
            size = 0
        elif self.case == "outer-short":
            size -= 1
        elif self.case == "outer-long":
            size += 1
        elif self.case == "outer-overflow":
            size = 0xFFFFFFFF
        request.add_bytes(struct.pack(">I", size))
        request.add_bytes(signature)
        if self.case == "packet-trailing":
            request.add_bytes(b"unsigned")
        elif self.case == "truncated":
            request = Message(request.asbytes()[:-1])
        self.transport._send_message(request)


def probe(args, key, case, replay):
    with socket.create_connection((args.host, args.port), args.timeout) as sock:
        with ProbeTransport(sock) as transport:
            transport.start_client(timeout=args.timeout)
            if not transport.is_active() or not transport.initial_kex_done:
                raise RuntimeError("key exchange did not complete")
            auth = ProofAuth(transport, key, case, replay)
            transport.auth_handler = auth
            event = threading.Event()
            auth.auth_none(args.username, event)
            if not event.wait(args.timeout):
                raise TimeoutError("authentication response timed out")
            if auth.reply is not None:
                return auth.reply
            error = transport.get_exception()
            if isinstance(error, EOFError) or transport.disconnect is not None:
                return "closed"
            raise RuntimeError(f"unexpected transport error: {error!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--username", default="root")
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    agent = paramiko.Agent()
    try:
        keys = [key for key in agent.get_keys() if key.get_name() == "ssh-ed25519"]
        if not keys:
            raise RuntimeError("an authorized Ed25519 SSH agent key is required")
        key = keys[0]
        replay = []
        cases = ["valid", "outer-zero", "outer-short", "outer-long", "outer-overflow",
                 "packet-trailing", "inner-trailing", "truncated", "bad-signature",
                 "wrong-session", "replay", "valid-after-denials"]
        failed = 0
        for case in cases:
            try:
                actual = probe(args, key, case, replay)
            except Exception as error:
                actual = f"ERROR {type(error).__name__}: {error}"
            expected = "success" if case.startswith("valid") else "denied"
            passed = actual == "success" if expected == "success" else actual in ("failure", "closed")
            failed += not passed
            print(f"{'PASS' if passed else 'FAIL'} {case}: {actual}; expected {expected}", flush=True)
            if case == "valid" and not passed:
                raise RuntimeError("positive authentication control failed; stop before further probes")
            time.sleep(2)
        print(f"SSH auth proof: {len(cases)} cases, {failed} failed", flush=True)
        return int(failed != 0)
    finally:
        agent.close()


if __name__ == "__main__":
    raise SystemExit(main())
