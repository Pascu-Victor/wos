"""Bounded ABI and refusal tests for the read-only VFIO dependency fixture."""

from __future__ import annotations

import errno
import importlib.util
from pathlib import Path
import stat
import struct
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[3] / "scripts/test/vfio_hot_reset_info.py"
SPEC = importlib.util.spec_from_file_location("vfio_hot_reset_info", SOURCE)
assert SPEC is not None and SPEC.loader is not None
vfio = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(vfio)


class HotResetInfoTest(unittest.TestCase):
    def run_query(self, rows, flags, *, reported_count=None):
        calls = []

        def ioctl(fd, request, data, mutate):
            calls.append((fd, request, mutate))
            self.assertEqual((fd, request, mutate), (5, vfio.GET_PCI_HOT_RESET_INFO, True))
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(data), vfio.HEADER_BYTES + vfio.MAX_DEPENDENCIES * vfio.DEVICE_BYTES)
            struct.pack_into("=III", data, 0, len(data), flags, len(rows) if reported_count is None else reported_count)
            for i, row in enumerate(rows):
                struct.pack_into("=IHBB", data, vfio.HEADER_BYTES + i * vfio.DEVICE_BYTES, *row)
            return 0

        with patch.object(vfio.os, "fstat", return_value=SimpleNamespace(st_mode=stat.S_IFCHR | 0o600)), patch.object(
            vfio.os, "readlink", return_value="/dev/vfio/devices/vfio7"
        ), patch.object(vfio.fcntl, "ioctl", side_effect=ioctl):
            result = vfio.query(5)
        self.assertEqual(len(calls), 1)
        return result

    def test_seven_physical_dependencies_keep_six_unowned(self):
        rows = [(1, 0, 0x2a, 0)] + [(vfio.DEVID_NOT_OWNED, 0, 0x2a, function) for function in range(1, 7)]
        result = self.run_query(rows, vfio.DEV_ID)
        self.assertEqual(result["authority"], "observation_only")
        self.assertEqual(result["count"], 7)
        self.assertFalse(result["all_owned"])
        self.assertEqual([device["bdf"] for device in result["devices"]], [f"0000:2a:00.{i}" for i in range(7)])
        self.assertEqual([device["ownership"] for device in result["devices"]], ["bound"] + ["not_owned"] * 6)

    def test_all_owned_requires_kernel_flag_and_distinct_ids(self):
        rows = [(i + 1, 0, 0x2a, i) for i in range(7)]
        result = self.run_query(rows, vfio.DEV_ID | vfio.DEV_ID_OWNED)
        self.assertTrue(result["all_owned"])
        self.assertEqual([device["devid"] for device in result["devices"]], list(range(1, 8)))

    def test_prebind_einval_is_an_error(self):
        with patch.object(vfio.os, "fstat", return_value=SimpleNamespace(st_mode=stat.S_IFCHR)), patch.object(
            vfio.os, "readlink", return_value="/dev/vfio/devices/vfio7"
        ), patch.object(vfio.fcntl, "ioctl", side_effect=OSError(errno.EINVAL, "not bound")) as ioctl:
            with self.assertRaises(vfio.QueryError) as caught:
                vfio.query(5)
        self.assertEqual(caught.exception.reason, "ioctl_unavailable")
        self.assertEqual(caught.exception.error_number, errno.EINVAL)
        ioctl.assert_called_once()

    def test_oversized_dependency_count_refuses(self):
        rows = [(i + 1, 0, 0x2a, i) for i in range(7)]
        with self.assertRaises(vfio.QueryError) as caught:
            self.run_query(rows, 3, reported_count=vfio.MAX_DEPENDENCIES + 1)
        self.assertEqual(caught.exception.reason, "invalid_dependency_response")

    def test_empty_count_and_unknown_flags_refuse(self):
        rows = [(1, 0, 0x2a, 0)]
        for flags, count in ((vfio.DEV_ID, 0), (0, 1), (vfio.DEV_ID | 4, 1)):
            with self.subTest(flags=flags, count=count), self.assertRaises(vfio.QueryError) as caught:
                self.run_query(rows, flags, reported_count=count)
            self.assertEqual(caught.exception.reason, "invalid_dependency_response")

    def test_duplicate_bdf_or_devid_refuses(self):
        for rows in ([(1, 0, 0x2a, 0), (2, 0, 0x2a, 0)], [(1, 0, 0x2a, 0), (1, 0, 0x2a, 1)]):
            with self.subTest(rows=rows), self.assertRaises(vfio.QueryError) as caught:
                self.run_query(rows, 3)
            self.assertEqual(caught.exception.reason, "duplicate_dependency_identity")

    def test_inconsistent_all_owned_flag_refuses(self):
        with self.assertRaises(vfio.QueryError) as caught:
            self.run_query([(1, 0, 0x2a, 0), (vfio.DEVID_NOT_OWNED, 0, 0x2a, 1)], 3)
        self.assertEqual(caught.exception.reason, "inconsistent_ownership_flag")

    def test_non_vfio_fd_never_reaches_ioctl(self):
        with patch.object(vfio.os, "fstat", return_value=SimpleNamespace(st_mode=stat.S_IFREG)), patch.object(
            vfio.os, "readlink", return_value="/tmp/unrelated"
        ), patch.object(vfio.fcntl, "ioctl") as ioctl:
            with self.assertRaises(vfio.QueryError) as caught:
                vfio.query(5)
        self.assertEqual(caught.exception.reason, "not_vfio_cdev")
        ioctl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
