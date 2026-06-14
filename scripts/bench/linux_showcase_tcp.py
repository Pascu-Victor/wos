#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import time


def run_server(args: argparse.Namespace) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", args.port))
        server.listen(args.sessions)
        total_bytes = 0
        started = time.monotonic()
        for _ in range(args.sessions):
            conn, addr = server.accept()
            with conn:
                received = 0
                while True:
                    chunk = conn.recv(1024 * 1024)
                    if not chunk:
                        break
                    received += len(chunk)
                total_bytes += received
                conn.sendall(f"{received}\n".encode())
                peer = addr[0]
        elapsed = time.monotonic() - started
    print(
        json.dumps(
            {
                "role": "server",
                "port": args.port,
                "sessions": args.sessions,
                "bytes": total_bytes,
                "seconds": elapsed,
                "last_peer": peer if args.sessions else "",
            },
            sort_keys=True,
        )
    )
    return 0


def run_client(args: argparse.Namespace) -> int:
    payload = bytes((index % 251 for index in range(args.bytes)))
    total_bytes = 0
    started = time.monotonic()
    for _ in range(args.repeat):
        with socket.create_connection((args.host, args.port), timeout=args.timeout) as conn:
            conn.sendall(payload)
            conn.shutdown(socket.SHUT_WR)
            ack = conn.recv(128).decode(errors="replace").strip()
        if ack != str(args.bytes):
            raise RuntimeError(f"server acknowledged {ack!r}, expected {args.bytes}")
        total_bytes += args.bytes
    elapsed = time.monotonic() - started
    mib = total_bytes / (1024.0 * 1024.0)
    print(
        json.dumps(
            {
                "role": "client",
                "host": args.host,
                "port": args.port,
                "repeat": args.repeat,
                "bytes": total_bytes,
                "seconds": elapsed,
                "throughput_mib_per_s": mib / elapsed if elapsed > 0 else 0.0,
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tiny TCP helper for the Linux showcase suite.")
    sub = parser.add_subparsers(dest="mode", required=True)

    server = sub.add_parser("server")
    server.add_argument("--port", type=int, required=True)
    server.add_argument("--sessions", type=int, default=1)
    server.set_defaults(func=run_server)

    client = sub.add_parser("client")
    client.add_argument("--host", required=True)
    client.add_argument("--port", type=int, required=True)
    client.add_argument("--bytes", type=int, default=1024 * 1024)
    client.add_argument("--repeat", type=int, default=1)
    client.add_argument("--timeout", type=float, default=20.0)
    client.set_defaults(func=run_client)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
