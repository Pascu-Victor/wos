"""Query a held Linux VFIO cdev's physical hot-reset dependencies.

Call ``query(fd)`` only while the owner keeps the already bound device FD and
its iommufd context alive. This module does not open, bind, close or reset any
device. The result is an observation, never reset authority.
"""

from __future__ import annotations

import fcntl
import os
import re
import stat
import struct

GET_PCI_HOT_RESET_INFO = 0x3B70  # _IO(';', VFIO_BASE + 12), Linux x86-64.
DEV_ID = 1
DEV_ID_OWNED = 2
DEVID_NOT_OWNED = 0xFFFFFFFF
HEADER_BYTES = 12
DEVICE_BYTES = 8
MAX_DEPENDENCIES = 256
VFIO_CDEV = re.compile(r"/dev/vfio/devices/vfio[0-9]+")


class QueryError(Exception):
    def __init__(self, reason: str, error_number: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.error_number = error_number


def _held_cdev(fd: int) -> None:
    if not isinstance(fd, int) or isinstance(fd, bool) or fd < 3:
        raise QueryError("invalid_fd")
    try:
        mode = os.fstat(fd).st_mode
        path = os.readlink(f"/proc/self/fd/{fd}")
    except OSError as error:
        raise QueryError("fd_unavailable", error.errno) from error
    if not stat.S_ISCHR(mode) or VFIO_CDEV.fullmatch(path) is None:
        raise QueryError("not_vfio_cdev")


def query(fd: int) -> dict[str, object]:
    """Return bounded affected BDFs and cdev ownership from a held, bound FD.

    A single fixed-size query avoids a count/ownership race between calls.
    If the affected set exceeds the bound, VFIO refuses the buffer.
    """
    _held_cdev(fd)
    data = bytearray(HEADER_BYTES + MAX_DEPENDENCIES * DEVICE_BYTES)
    struct.pack_into("=III", data, 0, len(data), 0, 0)
    try:
        fcntl.ioctl(fd, GET_PCI_HOT_RESET_INFO, data, True)
    except OSError as error:
        raise QueryError("ioctl_unavailable", error.errno) from error
    returned_size, flags, count = struct.unpack_from("=III", data)
    if returned_size != len(data) or not 1 <= count <= MAX_DEPENDENCIES or flags & ~3 or not flags & DEV_ID:
        raise QueryError("invalid_dependency_response")

    devices: list[dict[str, object]] = []
    seen_bdfs: set[str] = set()
    positive_ids: set[int] = set()
    for index in range(count):
        devid, segment, bus, devfn = struct.unpack_from("=IHBB", data, HEADER_BYTES + index * DEVICE_BYTES)
        bdf = f"{segment:04x}:{bus:02x}:{devfn >> 3:02x}.{devfn & 7}"
        if bdf in seen_bdfs or (devid not in (0, DEVID_NOT_OWNED) and devid in positive_ids):
            raise QueryError("duplicate_dependency_identity")
        seen_bdfs.add(bdf)
        if devid not in (0, DEVID_NOT_OWNED):
            positive_ids.add(devid)
        ownership = "not_owned" if devid == DEVID_NOT_OWNED else "same_group" if devid == 0 else "bound"
        devices.append({"bdf": bdf, "devid": devid, "ownership": ownership})

    all_owned = bool(flags & DEV_ID_OWNED)
    if all_owned and any(device["ownership"] == "not_owned" for device in devices):
        raise QueryError("inconsistent_ownership_flag")
    return {
        "schema": 1,
        "authority": "observation_only",
        "count": count,
        "flags": flags,
        "all_owned": all_owned,
        "devices": devices,
    }
