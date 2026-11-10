"""Real child-process seccomp tests; no VFIO device, root or VM needed."""

from __future__ import annotations

import errno
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

GUARD = Path(__file__).resolve().parents[3] / "scripts/test/vfio_reset_guard.py"
SPEC = importlib.util.spec_from_file_location("vfio_reset_guard", GUARD)
assert SPEC is not None and SPEC.loader is not None
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class ResetGuardTest(unittest.TestCase):
    def test_denies_both_resets_after_exec_and_keeps_other_ioctl(self):
        probe = """import errno,fcntl,json,os
fd=os.open('/dev/null',os.O_RDONLY)
out=[]
for request in (0x3b6f,0x3b71,0x3b70):
    try: fcntl.ioctl(fd,request); out.append(0)
    except OSError as error: out.append(error.errno)
print(json.dumps(out))
"""
        result = subprocess.run(
            [sys.executable, str(GUARD), "--", sys.executable, "-I", "-c", probe],
            capture_output=True, text=True, timeout=10, check=True,
        )
        self.assertEqual(result.stdout.strip(), f"[{errno.EPERM}, {errno.EPERM}, {errno.ENOTTY}]")

    def test_missing_separator_refuses_launch(self):
        result = subprocess.run([sys.executable, str(GUARD), sys.executable],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertIn("usage:", result.stderr)

    def test_relative_program_refuses_launch(self):
        result = subprocess.run([sys.executable, str(GUARD), "--", "python3"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)

    def test_filter_install_failure_never_execs(self):
        with patch.object(guard, "install", side_effect=OSError(errno.EPERM, "rejected")), patch.object(
            guard.os, "execve"
        ) as execve:
            self.assertEqual(guard.main(["guard", "--", "/usr/bin/true"]), 1)
        execve.assert_not_called()


if __name__ == "__main__":
    unittest.main()
