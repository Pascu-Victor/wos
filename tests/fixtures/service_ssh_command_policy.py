#!/usr/bin/env python3
"""Exercise command decisions and channel-state reuse in a disposable WOS VM."""

import argparse
import contextlib
import socket

import paramiko
from paramiko.common import cMSG_CHANNEL_REQUEST
from paramiko.message import Message

from service_ssh_broker_lifetime import run


@contextlib.contextmanager
def connect(args, key):
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = 10
            transport.start_client(timeout=10)
            transport.auth_publickey("root", key)
            if not transport.is_authenticated():
                raise RuntimeError("authorized agent login required")
            yield transport


def request(transport, kind, argument=None, trailing=b""):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(10)
        message = Message()
        message.add_byte(cMSG_CHANNEL_REQUEST)
        message.add_int(channel.remote_chanid)
        message.add_string(kind)
        message.add_boolean(True)
        if argument is not None:
            message.add_string(argument)
        message.add_bytes(trailing)
        channel._event_pending()
        transport._send_user_message(message)
        try:
            channel._wait_for_event()
        except paramiko.SSHException:
            if not transport.is_active():
                raise AssertionError("malformed command should reject only its channel")
            return
        raise AssertionError("malformed command accepted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--channels", type=int, default=1024)
    args = parser.parse_args()
    agent = paramiko.Agent()
    try:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized root SSH agent key is required")
        with connect(args, keys[0]) as transport:
            if run(transport, "printf command_policy; exit 23") != (23, "command_policy"):
                raise AssertionError("ordinary exec result changed")
            print("PASS ordinary exec output and nonzero exit status", flush=True)
            cases = [
                ("exec-NUL", b"exec", b"printf unexpected\0suffix", b""),
                ("exec-trailing", b"exec", b"printf unexpected", b"extra"),
                ("request-name-NUL", b"exec\0suffix", b"printf unexpected", b""),
                ("shell-trailing", b"shell", None, b"extra"),
                ("subsystem-NUL", b"subsystem", b"sftp\0suffix", b""),
                ("subsystem-trailing", b"subsystem", b"sftp", b"extra"),
                ("unknown-subsystem", b"subsystem", b"unknown", b""),
            ]
            for name, kind, argument, trailing in cases:
                request(transport, kind, argument, trailing)
                if run(transport, "printf after_denial") != (0, "after_denial"):
                    raise AssertionError("denial damaged later command state")
                print("PASS " + name + ": rejected; next command succeeds", flush=True)
            if run(transport, "") != (0, ""):
                raise AssertionError("empty command changed")
            print("PASS empty command", flush=True)
            with transport.open_session(timeout=10) as channel:
                channel.settimeout(10)
                channel.invoke_shell()
                channel.sendall(b"printf shell_policy; exit\n")
                channel.shutdown_write()
                output = bytearray()
                while chunk := channel.recv(4096):
                    output.extend(chunk)
                    if len(output) > 4096:
                        raise AssertionError("unexpected shell output")
                if b"shell_policy" not in output or channel.recv_exit_status() != 0:
                    raise AssertionError("login shell did not execute input")
            print("PASS login shell", flush=True)
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                if sftp.stat("/etc/passwd").st_size <= 0:
                    raise AssertionError("SFTP subsystem did not start")
            print("PASS SFTP subsystem", flush=True)
            for index in range(args.channels):
                if run(transport, "printf channel_reuse") != (0, "channel_reuse"):
                    raise AssertionError(f"channel reuse failed at {index}")
                if (index + 1) % 128 == 0:
                    print(f"progress: {index + 1} channels released", flush=True)
            print(f"PASS {args.channels} sequential channel creations/releases", flush=True)
            with transport.open_session(timeout=10) as channel:
                channel.settimeout(10)
                channel.exec_command("read input; printf retained_command")
                transport.renegotiate_keys()
                channel.sendall(b"continue\n")
                channel.shutdown_write()
                with channel.makefile("rb") as output:
                    if output.read(4097) != b"retained_command" or channel.recv_exit_status() != 0:
                        raise AssertionError("active command did not survive rekey")
            print("PASS active prepared command survives rekey", flush=True)
            if run(transport, "printf after_rekey") != (0, "after_rekey"):
                raise AssertionError("command state lost after rekey")
            print("PASS commands after rekey", flush=True)
        return 0
    finally:
        agent.close()


if __name__ == "__main__":
    raise SystemExit(main())
