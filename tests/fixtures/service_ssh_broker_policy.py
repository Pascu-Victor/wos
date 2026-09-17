#!/usr/bin/env python3
"""Check broker account/key policy in a disposable WOS VM, restoring fixtures.

Uses an existing root SSH agent login for setup. Generated RSA private keys
remain in this process; only their public keys are installed in the guest.
Backups of changed public account/key files remain in the guest until restored.
"""

import argparse
import contextlib
import ipaddress
import socket
import stat

import paramiko

from service_ssh_broker_lifetime import run
from service_ssh_session_io import signal_request

KEYS = "/root/.ssh/authorized_keys"
HOME = "/tmp/goal19-broker-login"
USER = "goal19broker"
UID = 25000


@contextlib.contextmanager
def connect(args, key, username="root"):
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = 10
            transport.start_client(timeout=10)
            transport.auth_publickey(username, key)
            if not transport.is_authenticated():
                raise RuntimeError("authentication did not complete")
            yield transport


class Files:
    def __init__(self, sftp):
        self.sftp = sftp
        self.backups = {}
        self.created = []

    def read(self, path):
        with self.sftp.open(path, "rb") as file:
            data = file.read(131073)
        if len(data) > 131072:
            raise RuntimeError("fixture input exceeds size bound")
        return data

    def write_new(self, path, data, mode, uid=0, gid=0):
        with self.sftp.open(path, "wx") as file:
            file.write(data)
        self.sftp.chmod(path, mode)
        self.sftp.chown(path, uid, gid)

    def replace(self, path, data):
        attributes = self.sftp.lstat(path)
        if not stat.S_ISREG(attributes.st_mode):
            raise RuntimeError("fixture requires a regular file")
        mode = stat.S_IMODE(attributes.st_mode)
        if path not in self.backups:
            original = self.read(path)
            backup = path + ".goal19-broker-backup"
            self.write_new(backup, original, mode, attributes.st_uid, attributes.st_gid)
            self.backups[path] = (backup, original, attributes)
        temporary = path + ".goal19-broker-new"
        self.write_new(temporary, data, mode, attributes.st_uid, attributes.st_gid)
        self.created.append(temporary)
        self.sftp.posix_rename(temporary, path)
        self.created.remove(temporary)

    def restore(self):
        for path, (backup, original, attributes) in reversed(list(self.backups.items())):
            self.sftp.posix_rename(backup, path)
            restored = self.sftp.lstat(path)
            if self.read(path) != original or (restored.st_uid, restored.st_gid, restored.st_mode) != (
                    attributes.st_uid, attributes.st_gid, attributes.st_mode):
                raise RuntimeError("fixture restoration mismatch")
        for path in reversed(self.created):
            if stat.S_ISDIR(self.sftp.lstat(path).st_mode):
                self.sftp.rmdir(path)
            else:
                self.sftp.remove(path)


def public_line(key, options=""):
    return f"{options + ' ' if options else ''}{key.get_name()} {key.get_base64()} goal19\n".encode()


def denied_forward(transport, host, port):
    try:
        with transport.open_channel("direct-tcpip", (host, port), ("127.0.0.1", 12345), timeout=10):
            raise AssertionError("forwarding unexpectedly allowed")
    except paramiko.ChannelException as error:
        if error.code != 1:
            raise AssertionError("forward failed for a reason other than policy") from error


def auth_state_denials(args, key, passed):
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = 10
            transport.start_client(timeout=10)
            # The production profile permits ten counted failures. The first
            # unknown-key query is uncounted; bound this above that allowance.
            for _ in range(16):
                try:
                    transport.auth_publickey("root", key)
                    raise AssertionError("unlisted key authenticated")
                except paramiko.SSHException:
                    if not transport.is_active():
                        break
            if transport.is_active():
                raise AssertionError("authentication failure limit not enforced")
            passed("broker retains failure count and closes repeated denials")
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as transport:
            transport.auth_timeout = 10
            transport.start_client(timeout=10)
            try:
                transport.auth_none("root")
                raise AssertionError("root authenticated without a key")
            except paramiko.AuthenticationException:
                if not transport.is_active():
                    raise AssertionError("initial method query unexpectedly closed")
            try:
                transport.auth_none(USER)
                raise AssertionError("changed username authenticated")
            except paramiko.SSHException:
                if transport.is_active():
                    raise AssertionError("username change did not close the connection")
            passed("broker fixes username across authentication requests")


def identity_snapshot(args, key, sftp, files, passwd, group, passed):
    for gid in range(UID + 1, UID + 10):
        if f":{gid}:".encode() in passwd or f":{gid}:".encode() in group:
            raise RuntimeError("snapshot fixture identity is already in use")
    account = passwd + f"\n{USER}:!:{UID}:{UID}:Goal19 fixture:{HOME}:/bin/sh\n".encode()
    primary = group + f"\n{USER}:!:{UID}:\n".encode()

    def membership(gids):
        return primary + b"".join(f"goal19g{gid}:!:{gid}:{USER}\n".encode() for gid in gids)

    def expect_groups(transport, expected):
        status, output = run(transport, "id -G")
        if status != 0 or set(output.split()) != {str(gid) for gid in expected}:
            raise AssertionError(f"unexpected supplementary groups: {status}, {output!r}")

    files.replace("/etc/group", membership([UID + 2]))
    with socket.create_connection((args.host, 22), 10) as sock:
        with paramiko.Transport(sock) as target:
            target.auth_timeout = 10
            target.start_client(timeout=10)
            try:
                target.auth_none(USER)
                raise AssertionError("snapshot fixture authenticated without proof")
            except paramiko.AuthenticationException:
                if not target.is_active():
                    raise AssertionError("snapshot method query closed")
            files.replace("/etc/group", membership([UID + 3]))
            target.auth_publickey(USER, key)
            expect_groups(target, [UID, UID + 2])
            passed("login uses groups retained before the signed proof")
            target.renegotiate_keys()
            expect_groups(target, [UID, UID + 2])
            passed("retained supplementary groups survive rekey")

            changed = passwd + f"\n{USER}:!:{UID + 1}:{UID + 1}:Goal19 fixture:{HOME}:/bin/sh\n".encode()
            files.replace("/etc/passwd", changed)
            with target.open_session(timeout=10) as channel:
                channel.settimeout(10)
                channel.get_pty()
                channel.exec_command("id -u; id -g; tty; read answer")
                with channel.makefile("rb") as output:
                    identity = [output.readline(128).strip() for _ in range(3)]
                    if identity[:2] != [str(UID).encode(), str(UID).encode()]:
                        raise AssertionError(f"PTY login identity changed: {identity!r}")
                    tty = identity[2].decode()
                    tty_stat = sftp.stat(tty)
                    if (not tty.startswith("/dev/pts/") or tty_stat.st_uid != UID or tty_stat.st_gid != UID
                            or stat.S_IMODE(tty_stat.st_mode) != 0o600):
                        raise AssertionError("PTY ownership did not use retained account")
                    channel.sendall(b"done\n")
                    channel.shutdown_write()
                    if len(output.read(4097)) > 4096 or channel.recv_exit_status() != 0:
                        raise AssertionError("PTY snapshot session did not finish")
            passed("PTY ownership survives a post-login account identity change")
            files.replace("/etc/passwd", account)
    with connect(args, key, USER) as target:
        expect_groups(target, [UID, UID + 3])
        passed("fresh connection captures updated group membership")

    files.replace("/etc/group", membership(range(UID + 2, UID + 9)))
    with connect(args, key, USER) as target:
        expect_groups(target, [UID, *range(UID + 2, UID + 9)])
        passed("eight-group bound accepted without truncation")
    files.replace("/etc/group", membership(range(UID + 2, UID + 10)))
    try:
        with connect(args, key, USER):
            raise AssertionError("group overflow authenticated")
    except paramiko.AuthenticationException:
        passed("group overflow denied before authentication success")
    files.replace("/etc/group", primary)
    with connect(args, key, USER) as target:
        expect_groups(target, [UID])
        passed("fresh login recovers after group-overflow policy is removed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", type=ipaddress.IPv4Address)
    args = parser.parse_args()
    args.host = str(args.host)
    agent = paramiko.Agent()
    checks = 0

    def passed(name):
        nonlocal checks
        checks += 1
        print("PASS " + name, flush=True)

    try:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized root agent key is required")
        restricted = paramiko.RSAKey.generate(2048)
        permitted = paramiko.RSAKey.generate(2048)
        signal_restricted = paramiko.RSAKey.generate(2048)
        pty_restricted = paramiko.RSAKey.generate(2048)
        with connect(args, keys[0]) as control:
            with paramiko.SFTPClient.from_transport(control) as sftp:
                files = Files(sftp)
                try:
                    try:
                        with connect(args, restricted):
                            raise AssertionError("unknown key accepted")
                    except paramiko.AuthenticationException:
                        passed("unlisted generated key rejected")
                    auth_state_denials(args, restricted, passed)
                    original = files.read(KEYS)
                    options = 'command="printf broker_policy",no-pty,no-port-forwarding,no-agent-forwarding,no-X11-forwarding'
                    permits = f'permitopen="127.0.0.1:22",permitopen="{args.host}:22"'
                    signal_options = 'command="read input; printf forced_signal_survived",no-pty'
                    files.replace(KEYS, original + b"\n" + public_line(restricted, options)
                                  + public_line(permitted, permits) + public_line(signal_restricted, signal_options)
                                  + public_line(pty_restricted, 'command="printf broker_pty_policy"'))
                    with connect(args, pty_restricted) as target:
                        with target.open_session(timeout=10) as channel:
                            channel.settimeout(10)
                            channel.get_pty()
                            channel.exec_command("printf untrusted_pty_command")
                            with channel.makefile("rb") as output:
                                if output.read(4097) != b"broker_pty_policy" or channel.recv_exit_status() != 0:
                                    raise AssertionError("PTY bypassed forced command")
                    passed("broker-owned PTY preserves forced-command policy")
                    with connect(args, signal_restricted) as target:
                        with target.open_session(timeout=10) as channel:
                            channel.settimeout(10)
                            channel.exec_command("printf untrusted_command")
                            signal_request(target, channel, "TERM", want_reply=False)
                            channel.sendall(b"continue\n")
                            channel.shutdown_write()
                            with channel.makefile("rb") as output:
                                if output.read(4097) != b"forced_signal_survived" or channel.recv_exit_status() != 0:
                                    raise AssertionError("signal terminated a forced command")
                    passed("broker denies signals to its forced-command child")
                    with connect(args, restricted) as target:
                        if run(target, "printf untrusted_command") != (0, "broker_policy"):
                            raise AssertionError("forced command not preserved")
                        passed("broker-owned forced command overrides requested command")
                        with target.open_session(timeout=10) as channel:
                            channel.settimeout(10)
                            channel.invoke_subsystem("sftp")
                            with channel.makefile("rb") as output:
                                if output.read(4097) != b"broker_policy" or channel.recv_exit_status() != 0:
                                    raise AssertionError("SFTP subsystem bypassed forced command")
                        passed("forced command overrides direct SFTP subsystem")
                        with target.open_session(timeout=10) as channel:
                            channel.settimeout(10)
                            terminals = set(sftp.listdir("/dev/pts"))
                            try:
                                channel.get_pty()
                                raise AssertionError("PTY unexpectedly allowed")
                            except paramiko.SSHException:
                                if set(sftp.listdir("/dev/pts")) != terminals:
                                    raise AssertionError("denied PTY allocated a terminal")
                                passed("no-PTY option preserved")
                        denied_forward(target, "127.0.0.1", 22)
                        passed("no-forwarding option preserved")
                        target.renegotiate_keys()
                        if run(target, "printf changed_after_rekey") != (0, "broker_policy"):
                            raise AssertionError("forced command lost after rekey")
                        passed("authenticated policy survives rekey")
                    with connect(args, permitted) as target:
                        reserved_port = 1009
                        allocated = target.request_port_forward("127.0.0.1", reserved_port)
                        if allocated != reserved_port:
                            raise AssertionError("root login lost reserved-port forwarding")
                        target.cancel_port_forward("127.0.0.1", reserved_port)
                        passed("root login retains reserved-port forwarding with a dedicated worker")
                        for host in ("127.0.0.1", args.host):
                            with target.open_channel("direct-tcpip", (host, 22), ("127.0.0.1", 12345), timeout=10) as channel:
                                channel.settimeout(10)
                                if not channel.recv(128).startswith(b"SSH-"):
                                    raise AssertionError("allowed forward did not reach SSH")
                        passed("both permitopen entries preserved")
                        denied_forward(target, "127.0.0.1", 23)
                        denied_forward(target, "localhost", 22)
                        passed("permitopen rejects unlisted host and port")

                    passwd = files.read("/etc/passwd")
                    group = files.read("/etc/group")
                    if USER.encode() in passwd or f":{UID}:".encode() in passwd or f":{UID}:".encode() in group:
                        raise RuntimeError("temporary account identity is already in use")
                    for directory in (HOME, HOME + "/.ssh"):
                        sftp.mkdir(directory, 0o700)
                        files.created.append(directory)
                        sftp.chown(directory, UID, UID)
                    user_keys = HOME + "/.ssh/authorized_keys"
                    files.write_new(user_keys, public_line(permitted), 0o600, UID, UID)
                    files.created.append(user_keys)
                    files.replace("/etc/group", group + f"\n{USER}:!:{UID}:\n".encode())
                    files.replace("/etc/passwd", passwd + f"\n{USER}:!:{UID}:{UID}:Goal19 fixture:{HOME}:/bin/sh\n".encode())
                    with connect(args, permitted, USER) as target:
                        try:
                            target.request_port_forward("127.0.0.1", reserved_port)
                            target.cancel_port_forward("127.0.0.1", reserved_port)
                            raise AssertionError("nonroot login obtained a reserved-port listener")
                        except paramiko.SSHException:
                            passed("nonroot reserved-port forwarding denied by retained login identity")
                        expected = (0, f"{UID}\n{UID}\n{HOME}")
                        if run(target, "id -u; id -g; pwd") != expected:
                            raise AssertionError("broker account identity/home not preserved")
                        passed("nonroot UID/GID/home resolved and applied")
                        target.renegotiate_keys()
                        if run(target, "id -u; id -g; pwd") != expected:
                            raise AssertionError("nonroot rekey changed session identity")
                        passed("nonroot connection rekeys through privileged broker")
                        if run(target, "test -r /etc/dropbear/dropbear_rsa_host_key")[0] != 1:
                            raise AssertionError("nonroot session can read private host key")
                        passed("nonroot session cannot read private host key")
                    identity_snapshot(args, permitted, sftp, files, passwd, group, passed)
                finally:
                    files.restore()
                    passed("original account and authorized-key files restored")
        print(f"Broker policy: {checks} checks passed", flush=True)
        return 0
    finally:
        agent.close()


if __name__ == "__main__":
    raise SystemExit(main())
