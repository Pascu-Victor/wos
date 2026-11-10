#!/usr/bin/env python3
"""Exec a Linux x86-64 program with VFIO reset ioctls denied by seccomp.

The caller owns all device descriptors and recovery. This wrapper grants no
device access, changes no driver, and cannot suppress a reset performed by the
kernel on VFIO close or by a different process. Use as a separate executable
before QEMU, not as a Python subprocess ``preexec_fn``.
"""

from __future__ import annotations

import ctypes
import errno
import os
import sys

# Linux x86-64 UAPI: _IO(';', VFIO_BASE + 11/13).
VFIO_DEVICE_RESET = 0x3B6F
VFIO_DEVICE_PCI_HOT_RESET = 0x3B71
AUDIT_ARCH_X86_64 = 0xC000003E
SYS_IOCTL_X86_64 = 16
PR_SET_NO_NEW_PRIVS = 38
PR_SET_SECCOMP = 22
SECCOMP_MODE_FILTER = 2
SECCOMP_RET_KILL_PROCESS = 0x80000000
SECCOMP_RET_ALLOW = 0x7FFF0000
SECCOMP_RET_ERRNO = 0x00050000
BPF_LD_W_ABS = 0x20
BPF_JMP_JEQ_K = 0x15
BPF_RET_K = 0x06


class SockFilter(ctypes.Structure):
    _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte),
                ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint32)]


class SockFprog(ctypes.Structure):
    _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(SockFilter))]


def install() -> None:
    """Deny both VFIO reset requests on every ioctl FD, including after exec."""
    if os.uname().machine != "x86_64":
        raise RuntimeError("VFIO reset filter requires Linux x86-64")
    # struct seccomp_data: nr at 0, arch at 4, args[1] (ioctl request) at 24.
    # Kernel ioctl dispatch uses the low 32 request bits, so load those bits.
    rows = (SockFilter * 10)(
        SockFilter(BPF_LD_W_ABS, 0, 0, 4),
        SockFilter(BPF_JMP_JEQ_K, 1, 0, AUDIT_ARCH_X86_64),
        SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_KILL_PROCESS),
        SockFilter(BPF_LD_W_ABS, 0, 0, 0),
        SockFilter(BPF_JMP_JEQ_K, 0, 3, SYS_IOCTL_X86_64),
        SockFilter(BPF_LD_W_ABS, 0, 0, 24),
        SockFilter(BPF_JMP_JEQ_K, 2, 0, VFIO_DEVICE_RESET),
        SockFilter(BPF_JMP_JEQ_K, 1, 0, VFIO_DEVICE_PCI_HOT_RESET),
        SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW),
        SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ERRNO | errno.EPERM),
    )
    program = SockFprog(len(rows), rows)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                           ctypes.c_ulong, ctypes.c_ulong)
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_NO_NEW_PRIVS failed")
    if libc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER,
                  ctypes.addressof(program), 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_SECCOMP failed")


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] != "--" or not os.path.isabs(argv[2]):
        print("usage: vfio_reset_guard.py -- /absolute/program [args...]", file=sys.stderr)
        return 2
    try:
        install()
        os.execve(argv[2], argv[2:], os.environ)
    except (OSError, RuntimeError) as error:
        print(f"VFIO reset guard refused launch: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
