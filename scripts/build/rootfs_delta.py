#!/usr/bin/env python3
"""Prepare an incremental rootfs delta from the declared host-side sources.

The sync step must not prove "nothing changed" by copying or hashing the whole
rootfs. This script enumerates the complete managed source set, records cheap
metadata for every target entry, diffs that state against the previous
successful sync, and stages only entries whose source metadata changed.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import os
import shutil
import stat
from dataclasses import dataclass
from pathlib import Path


USR_MERGE_PREFIXES = (
    ("/bin/", "/usr/bin/"),
    ("/sbin/", "/usr/sbin/"),
    ("/lib/", "/usr/lib/"),
)


@dataclass(frozen=True)
class Entry:
    target: str
    kind: str
    mode: int
    size: int
    mtime_ns: int
    link: str
    source: str
    generated: bytes | None = None

    def state_line(self) -> str:
        return "\t".join(
            [
                self.target,
                self.kind,
                f"{self.mode:o}",
                str(self.size),
                str(self.mtime_ns),
                self.link,
                self.source,
            ]
        )

    def manifest_line(self) -> str:
        if self.kind == "link":
            return f"{self.target}\tL\t\t\t\t{self.link}"
        if self.kind == "dir":
            return f"{self.target}\tD\t{self.mode:o}\t\t\t"
        return f"{self.target}\tF\t{self.mode:o}\t{self.size}\t{self.mtime_ns}\tmtime-ns"


class RootfsDelta:
    def __init__(self, args: argparse.Namespace) -> None:
        self.repo = args.repo.resolve()
        self.staging = args.staging.resolve()
        self.cache = args.cache.resolve()
        self.new_cache = args.new_cache.resolve()
        self.stamp = args.stamp.resolve()
        self.changed_paths = args.changed_paths.resolve()
        self.tar_paths = args.tar_paths.resolve()
        self.build_dir = Path(os.environ.get("WOS_BUILD_DIR", "build"))
        self.sysroot = Path(os.environ.get("WOS_SYSROOT_PATH", self.repo / "toolchain/sysroot"))
        self.busybox_install = Path(os.environ.get("WOS_BUSYBOX_INSTALL_DIR", self.repo / "toolchain/busybox-install"))
        self.force = os.environ.get("WOS_ROOTFS_FORCE_SYNC", "0") == "1"
        self.entries: dict[str, Entry] = {}
        self.managed_paths: set[str] = set()

    def resolve_source(self, source: str) -> Path:
        source_path = Path(source)
        if not source_path.is_absolute() and source.startswith("build/") and str(self.build_dir) != "build":
            source_path = self.build_dir / source[len("build/") :]
        elif not source_path.is_absolute() and source.startswith("toolchain/sysroot/"):
            source_path = self.sysroot / source[len("toolchain/sysroot/") :]
        elif not source_path.is_absolute() and source.startswith("toolchain/busybox-install/"):
            source_path = self.busybox_install / source[len("toolchain/busybox-install/") :]
        elif not source_path.is_absolute():
            source_path = self.repo / source
        return source_path

    @staticmethod
    def normalize_target(target: str) -> str:
        for prefix, replacement in USR_MERGE_PREFIXES:
            if target.startswith(prefix):
                return replacement + target[len(prefix) :]
        return target

    def record_managed(self, target: str) -> None:
        self.managed_paths.add(target)
        self.managed_paths.add(self.normalize_target(target))

    def add_parent_dirs(self, target: str) -> None:
        parts = Path(target).parts
        current = ""
        for part in parts[1:-1]:
            current += "/" + part
            self.add_dir(current)

    def add_dir(self, target: str, mode: int = 0o755) -> None:
        target = self.normalize_target(target)
        if target == "/":
            return
        self.add_parent_dirs(target)
        self.entries[target] = Entry(target, "dir", mode, 0, 0, "", "dir")
        self.managed_paths.add(target)

    def add_link(self, target: str, link: str, managed_target: str | None = None) -> None:
        actual = self.normalize_target(target)
        self.add_parent_dirs(actual)
        self.entries[actual] = Entry(actual, "link", 0o777, 0, 0, link, f"link:{link}")
        self.record_managed(managed_target or target)

    def add_generated_file(self, target: str, content: bytes, mode: int = 0o644) -> None:
        actual = self.normalize_target(target)
        digest = hashlib.sha256(content).hexdigest()
        self.add_parent_dirs(actual)
        self.entries[actual] = Entry(actual, "file", mode, len(content), 0, digest, f"generated:{digest}", content)
        self.record_managed(target)

    def add_source(self, source: Path, target: str, mode_override: int | None = None, managed_target: str | None = None) -> None:
        if not source.exists() and not source.is_symlink():
            return
        target = self.normalize_target(target)
        st = source.lstat()
        if stat.S_ISLNK(st.st_mode):
            self.add_link(target, os.readlink(source), managed_target)
            return
        if stat.S_ISDIR(st.st_mode):
            self.add_dir(target, stat.S_IMODE(st.st_mode))
            for root, dirs, files in os.walk(source, followlinks=False):
                dirs.sort()
                files.sort()
                root_path = Path(root)
                rel_root = root_path.relative_to(source)
                for dirname in dirs:
                    child = root_path / dirname
                    rel = rel_root / dirname
                    child_target = target if str(rel) == "." else f"{target}/{rel.as_posix()}"
                    child_st = child.lstat()
                    if stat.S_ISLNK(child_st.st_mode):
                        self.add_link(child_target, os.readlink(child), managed_target)
                    else:
                        self.add_dir(child_target, stat.S_IMODE(child_st.st_mode))
                for filename in files:
                    child = root_path / filename
                    rel = rel_root / filename
                    self.add_source(child, f"{target}/{rel.as_posix()}", mode_override, managed_target)
            self.record_managed(managed_target or target)
            return
        if stat.S_ISREG(st.st_mode):
            mode = mode_override if mode_override is not None else stat.S_IMODE(st.st_mode)
            self.add_parent_dirs(target)
            self.entries[target] = Entry(target, "file", mode, st.st_size, st.st_mtime_ns, "", str(source))
            self.record_managed(managed_target or target)

    def add_source_pattern(self, pattern: str, target_dir: str) -> None:
        resolved_pattern = str(self.resolve_source(pattern))
        for match in sorted(glob.glob(resolved_pattern)):
            source = Path(match)
            self.add_source(source, f"{target_dir}/{source.name}")

    def collect_alias_manifest(self) -> None:
        manifest = self.repo / "configs/rootfs/aliases.tsv"
        if not manifest.exists():
            return
        with manifest.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                fields = line.split("\t")
                action = fields[0]
                if action == "copy":
                    self.add_source(self.resolve_source(fields[1]), fields[2], managed_target=fields[2])
                elif action == "copy-mode":
                    self.add_source(self.resolve_source(fields[1]), fields[2], int(fields[3], 8), fields[2])
                elif action == "copy-glob":
                    self.add_source_pattern(fields[1], fields[2])
                elif action == "symlink":
                    self.add_link(fields[2], fields[1], fields[2])
                else:
                    raise RuntimeError(f"unknown rootfs manifest action {action!r} in {manifest}")

    def collect_sysroot_libs(self) -> None:
        lib = self.sysroot / "lib"
        if not lib.is_dir():
            return
        names: set[Path] = set()
        for pattern in ("*.so", "*.so.*", "crt*.o", "Scrt1.o", "ld.so"):
            names.update(lib.glob(pattern))
        for source in sorted(names):
            self.add_source(source, f"/usr/lib/{source.name}")

    def collect_sysroot_headers(self) -> None:
        include = self.sysroot / "include"
        self.add_source(include, "/usr/include")

    def collect_sysroot_cmake_data(self) -> None:
        share = self.sysroot / "share"
        if not share.is_dir():
            return
        for source in sorted(share.glob("cmake-*")):
            self.add_source(source, f"/usr/share/{source.name}")

    def collect_sysroot_python(self) -> None:
        bin_dir = self.sysroot / "bin"
        if bin_dir.is_dir():
            for pattern in ("python", "python[0-9]", "python[0-9].[0-9]*"):
                for source in sorted(bin_dir.glob(pattern)):
                    self.add_source(source, f"/usr/bin/{source.name}")
        lib_dir = self.sysroot / "lib"
        if lib_dir.is_dir():
            for source in sorted(lib_dir.glob("python[0-9]*")):
                if source.is_dir():
                    self.add_source(source, f"/usr/lib/{source.name}")

    def collect_busybox(self) -> None:
        if not self.busybox_install.is_dir():
            return
        lib = self.busybox_install / "lib"
        if lib.is_dir():
            for source in sorted(lib.iterdir()):
                self.add_source(source, f"/usr/lib/{source.name}")
        for source_dir, target_dir in (
            ("bin", "/usr/bin"),
            ("usr/bin", "/usr/bin"),
            ("sbin", "/usr/sbin"),
            ("usr/sbin", "/usr/sbin"),
        ):
            directory = self.busybox_install / source_dir
            if not directory.is_dir():
                continue
            for source in sorted(directory.iterdir()):
                self.add_source(source, f"{target_dir}/{source.name}")

    def collect_generated_etc(self) -> None:
        self.add_dir("/etc/dropbear")
        self.add_generated_file("/etc/passwd", b"root:!:0:0:root:/root:/bin/bash\n")
        self.add_generated_file("/etc/group", b"root:x:0:root\n")
        self.add_generated_file(
            "/etc/profile",
            b'export USER="${USER:-root}"\n'
            b'export HOSTNAME="${HOSTNAME:-wos}"\n'
            b'export HOME="${HOME:-/root}"\n'
            b'export SHELL="${SHELL:-/bin/bash}"\n'
            b'export TERM="${TERM:-xterm-256color}"\n'
            b'export PS1="$USER@$HOSTNAME:\\w\\$ "\n'
            b'export ENV="/etc/profile"\n'
            b'export CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"\n',
        )
        self.add_generated_file("/etc/shells", b"/bin/bash\n/usr/bin/bash\n/bin/sh\n/usr/bin/sh\n")
        self.add_generated_file("/etc/filesystems", b"fat32\nvfat\ntmpfs\n")
        vfstab = self.repo / "configs/vfstab"
        if vfstab.exists():
            self.add_source(vfstab, "/etc/vfstab")
        else:
            self.add_generated_file("/etc/vfstab", b"# prefix route\n/wki local\n/proc local\n/dev local\n/tmp local\n/run local\n/ host\n")
        hostname = "wos"
        system_conf = self.repo / "configs/system.conf"
        if system_conf.exists():
            for line in system_conf.read_text(encoding="utf-8").splitlines():
                if line.startswith("WOS_HOSTNAME="):
                    hostname = line.split("=", 1)[1].strip().strip("'\"") or "wos"
        self.add_generated_file("/etc/hostname", hostname.encode() + b"")
        for tz in (Path("/usr/share/zoneinfo/Etc/UTC"), Path("/usr/share/zoneinfo/UTC")):
            if tz.exists():
                self.add_source(tz, "/etc/localtime")
                break

    @staticmethod
    def read_authorized_key_lines(path: Path, lines: list[str], seen: set[str]) -> None:
        if not path.exists():
            return
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.rstrip("\r")
            if not line or line in seen:
                continue
            seen.add(line)
            lines.append(line)

    def collect_root_home(self) -> None:
        lines: list[str] = []
        seen: set[str] = set()
        self.read_authorized_key_lines(
            self.repo / "configs/rootfs/root/.ssh/authorized_keys",
            lines,
            seen,
        )
        for key in (Path.home() / ".ssh/id_ed25519.pub", Path.home() / ".ssh/id_rsa.pub", Path.home() / ".ssh/id_ecdsa.pub"):
            if key.exists():
                self.read_authorized_key_lines(key, lines, seen)
                break
        if lines:
            self.add_generated_file("/root/.ssh/authorized_keys", ("\n".join(lines) + "\n").encode(), 0o600)
        self.add_dir("/root/.ssh", 0o700)

    def collect_srv(self) -> None:
        self.add_dir("/srv")
        srv = self.repo / "configs/drive/srv"
        if srv.is_dir():
            for child in sorted(srv.iterdir()):
                self.add_source(child, f"/srv/{child.name}")
        self.add_generated_file("/srv/hello.txt", b"Hello from XFS filesystem!\n")
        self.add_generated_file("/srv/test.bin", b"Binary test data 1234567890ABCDEF")

    def collect_misc(self) -> None:
        for directory in ("/home", "/dev/pts", "/tmp", "/run", "/var/log/journal", "/oldroot"):
            self.add_dir(directory)
        self.add_link("/lib", "usr/lib")
        self.add_link("/bin", "usr/bin")
        self.add_link("/sbin", "usr/sbin")

    def collect(self) -> None:
        self.collect_sysroot_libs()
        self.collect_sysroot_headers()
        self.collect_sysroot_cmake_data()
        self.collect_sysroot_python()
        self.collect_busybox()
        self.collect_alias_manifest()
        self.collect_generated_etc()
        self.collect_root_home()
        self.collect_srv()
        self.collect_misc()
        self.managed_paths.add("/etc/wos-managed-paths")
        self.managed_paths.add("/etc/wos-rootfs-source-state.tsv")

    @staticmethod
    def read_state(path: Path) -> dict[str, str]:
        if not path.exists():
            return {}
        state: dict[str, str] = {}
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.rstrip("\n")
                if not line:
                    continue
                target = line.split("\t", 1)[0]
                state[target] = line
        return state

    def entries_newer_than_stamp(self) -> set[str]:
        if self.force or not self.stamp.exists():
            return set(self.entries)
        for control in (
            self.repo / "configs/rootfs/aliases.tsv",
            self.repo / "configs/system.conf",
            self.repo / "configs/vfstab",
        ):
            if control.exists() and control.stat().st_mtime_ns > self.stamp.stat().st_mtime_ns:
                return set(self.entries)
        changed: set[str] = set()
        stamp_ns = self.stamp.stat().st_mtime_ns
        for target, entry in self.entries.items():
            if entry.generated is not None:
                continue
            if entry.kind == "file" and entry.mtime_ns > stamp_ns:
                changed.add(target)
            elif entry.kind == "link":
                source = Path(entry.source.removeprefix("link:"))
                if source.exists() and source.lstat().st_mtime_ns > stamp_ns:
                    changed.add(target)
        return changed

    def changed_targets(self, old: dict[str, str]) -> set[str]:
        if self.force:
            return set(self.entries)
        if not old:
            return self.entries_newer_than_stamp()
        changed = {target for target, entry in self.entries.items() if old.get(target) != entry.state_line()}
        changed.update(target for target in old if target not in self.entries)
        return changed

    def stage_entry(self, entry: Entry) -> None:
        target = self.staging / entry.target.lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry.kind == "dir":
            target.mkdir(parents=True, exist_ok=True)
            os.chmod(target, entry.mode)
        elif entry.kind == "link":
            if target.exists() or target.is_symlink():
                target.unlink()
            os.symlink(entry.link, target)
        elif entry.generated is not None:
            target.write_bytes(entry.generated)
            os.chmod(target, entry.mode)
        else:
            shutil.copy2(entry.source, target, follow_symlinks=False)
            os.chmod(target, entry.mode)

    def write_state_files(self, changed: set[str]) -> None:
        state_lines = [self.entries[target].state_line() for target in sorted(self.entries)]
        self.new_cache.parent.mkdir(parents=True, exist_ok=True)
        self.new_cache.write_text("\n".join(state_lines) + "\n", encoding="utf-8")

        managed_rel = Path("etc/wos-managed-paths")
        managed_target = self.staging / managed_rel
        managed_target.parent.mkdir(parents=True, exist_ok=True)
        managed_target.write_text("\n".join(sorted(self.managed_paths)) + "\n", encoding="utf-8")

        source_state_rel = Path("etc/wos-rootfs-source-state.tsv")
        source_state_target = self.staging / source_state_rel
        source_state_target.write_text("\n".join(state_lines) + "\n", encoding="utf-8")

        content_manifest_rel = Path("etc/wos-rootfs-manifest.tsv")
        content_manifest_target = self.staging / content_manifest_rel
        content_manifest_target.write_text(
            "\n".join(self.entries[target].manifest_line() for target in sorted(self.entries)) + "\n",
            encoding="utf-8",
        )

        if changed:
            changed.update({"/etc/wos-managed-paths", "/etc/wos-rootfs-source-state.tsv", "/etc/wos-rootfs-manifest.tsv"})

    def write_path_lists(self, changed: set[str]) -> None:
        self.changed_paths.write_text("\n".join(sorted(changed)) + ("\n" if changed else ""), encoding="utf-8")
        tar_entries = [path.lstrip("/") for path in sorted(changed)]
        self.tar_paths.write_text("\n".join(tar_entries) + ("\n" if tar_entries else ""), encoding="utf-8")

    def run(self) -> None:
        self.collect()
        old = self.read_state(self.cache)
        changed = self.changed_targets(old)
        for target in sorted(changed):
            entry = self.entries.get(target)
            if entry is not None:
                self.stage_entry(entry)
        self.write_state_files(changed)
        self.write_path_lists(changed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--new-cache", type=Path, required=True)
    parser.add_argument("--stamp", type=Path, required=True)
    parser.add_argument("--changed-paths", type=Path, required=True)
    parser.add_argument("--tar-paths", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    RootfsDelta(parse_args()).run()


if __name__ == "__main__":
    main()
