#!/usr/bin/env python3
"""Check WOS broker-backed key exchange and rekey on a disposable VM."""

import argparse
import socket

import paramiko


def command(transport, marker, timeout):
    with transport.open_session(timeout=timeout) as channel:
        channel.settimeout(timeout)
        channel.exec_command("printf " + marker)
        output = bytearray()
        while True:
            chunk = channel.recv(4096)
            if not chunk:
                break
            output.extend(chunk)
        if channel.recv_exit_status() != 0 or output != marker.encode():
            raise RuntimeError("SSH command output/status mismatch")


def exercise(args, key, algorithm):
    with socket.create_connection((args.host, args.port), args.timeout) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = args.timeout
            transport.get_security_options().kex = (algorithm,)
            transport.start_client(timeout=args.timeout)
            if not transport.initial_kex_done:
                raise RuntimeError("initial key exchange incomplete")
            transport.auth_publickey(args.username, key)
            if not transport.is_authenticated():
                raise RuntimeError("agent key is not authorized")
            session_id = transport.session_id
            command(transport, "broker_initial", args.timeout)
            for _ in range(args.rekeys):
                transport.renegotiate_keys()
                if transport.session_id != session_id:
                    raise RuntimeError("session ID changed during rekey")
                command(transport, "broker_rekey", args.timeout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--username", default="root")
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--rekeys", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.rekeys <= 20:
        parser.error("--rekeys must be between 1 and 20")
    algorithms = ["curve25519-sha256@libssh.org", "ecdh-sha2-nistp256",
                  "ecdh-sha2-nistp384", "ecdh-sha2-nistp521",
                  "diffie-hellman-group14-sha256"]
    agent = paramiko.Agent()
    failed = 0
    try:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        for algorithm in algorithms:
            try:
                exercise(args, keys[0], algorithm)
                print(f"PASS {algorithm}: authenticated commands before and after {args.rekeys} rekeys", flush=True)
            except Exception as error:
                failed += 1
                print(f"FAIL {algorithm}: {type(error).__name__}: {error}", flush=True)
        print(f"KEX broker: {len(algorithms)} algorithms, {failed} failed", flush=True)
        return int(failed != 0)
    finally:
        agent.close()


if __name__ == "__main__":
    raise SystemExit(main())
