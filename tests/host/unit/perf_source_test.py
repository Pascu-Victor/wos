#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PERF_SRC_DIR = ROOT / "modules" / "perf" / "src"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
PROCFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.hpp"
MEMACC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "memacc.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def read_perf_source() -> str:
    paths = [*sorted(PERF_SRC_DIR.glob("*.cpp")), *sorted(PERF_SRC_DIR.glob("*.hpp"))]
    return "\n".join(path.read_text() for path in paths)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def test_perf_reads_are_byte_capped() -> None:
    source = read_perf_source()
    require_tokens(
        source,
        [
            "READ_CHUNK_CAPACITY",
            "PROCFS_READ_LIMIT",
            "PERF_PROC_READ_LIMIT",
            "PERF_DATA_READ_LIMIT",
            "auto read_fd(ScopedFd& fd, std::size_t initial_capacity = INITIAL_FILE_CAPACITY, std::size_t max_bytes = PERF_DATA_READ_LIMIT)",
            "-> std::optional<std::string>",
        ],
        "perf bounded read surface",
    )

    limit_body = function_body(source, "read_limit_for_path")
    require_tokens(
        limit_body,
        [
            "path == KPERF_PATH",
            "path == KWKISTAT_PATH",
            "path == KIPCSTAT_PATH",
            "path == KCPUSTAT_PATH",
            "path == KCONTSTAT_PATH",
            "return PERF_PROC_READ_LIMIT",
            "path.starts_with(PROC_ROOT)",
            "path.starts_with(DEV_NODES_ROOT)",
            "return PROCFS_READ_LIMIT",
            "return PERF_DATA_READ_LIMIT",
        ],
        "perf path read caps",
    )

    read_fd_body = function_body(source, "read_fd")
    require_tokens(
        read_fd_body,
        [
            "max_bytes - buffer.size()",
            "read(fd.get(), &extra, 1)",
            "COUNT < 0 && errno == EINTR",
            "COUNT < 0 || COUNT > 0",
            "return std::nullopt",
            "std::min(chunk.size(), REMAINING)",
            "buffer.append(chunk.data(), static_cast<std::size_t>(COUNT))",
        ],
        "perf bounded read loop",
    )
    if "buffer.resize(buffer.size() * 2)" in read_fd_body:
        fail("perf read_fd must not use an unbounded doubling loop")

    read_file_body = function_body(source, "read_file")
    require_tokens(
        read_file_body,
        ["return read_fd(fd, initial_capacity, read_limit_for_path(path));"],
        "perf read_file cap dispatch",
    )


def test_perf_run_waits_for_descendant_process_group() -> None:
    source = read_perf_source()
    cmd_run_body = function_body(source, "cmd_run")
    require_tokens(
        cmd_run_body,
        [
            "ker::process::setpgid(0, 0)",
            "ker::process::setpgid(child_pid, child_pid)",
            "int64_t target_pgid = child_pid",
            "std::cmp_equal(stat.pgid, target_pgid)",
            "last_group_alive = any_alive",
            "command_exited = true",
            "if (command_exited && !last_group_alive)",
            "set_recording_enabled(false)",
        ],
        "perf run process-group tracing",
    )
    if "if (command_exited || !any_alive)" in cmd_run_body:
        fail("perf run must not stop recording while same-PGID descendants are still alive")
    if "if (command_exited || !last_group_alive)" in cmd_run_body:
        fail("perf run must not stop recording while same-PGID descendants are still alive")


def test_runtime_image_observability_is_authoritative_and_append_only() -> None:
    perf = read_perf_source()
    procfs = PROCFS_CPP.read_text()
    procfs_header = PROCFS_HPP.read_text()
    memacc = MEMACC_CPP.read_text()

    require_tokens(
        procfs_header + procfs,
        [
            "IMAGES_FILE",
            'strcmp(path, "self/images")',
            'strcmp(task_sub, "images")',
            'strcmp(sub, "images")',
            "generate_images(pfd->node.pid",
        ],
        "procfs runtime image surface",
    )
    images_body = function_body(procfs, "generate_images")
    require_tokens(
        images_body,
        [
            "SharedVmemPublicationGuard",
            "snapshot_proc_runtime_images",
            "status=%s count=%llu",
            "base=0x%016llx",
            "text_start=0x%016llx",
            "build_id=%s path=%s",
        ],
        "procfs authoritative image catalog",
    )
    maps_body = function_body(procfs, "generate_maps")
    require_tokens(
        maps_body,
        [
            "SharedVmemPublicationGuard",
            "snapshot_proc_runtime_images",
            "IMAGE_EXACT_MAPPING",
            "next_image_boundary",
            "constexpr uint64_t MAP_OFFSET = 0",
            "IMAGE_PATH",
        ],
        "procfs catalog-backed maps",
    )
    if "executable_load_base" in maps_body or "Elf64_Ehdr" in maps_body:
        fail("procfs maps must not guess executable bases from cached ELF headers")

    require_tokens(
        memacc,
        [
            "UserMemoryLayout",
            "layout.image_count",
            "layout.images.at(i)",
            "user_layout::MMAP_WINDOW",
            "user_layout::THREAD_WINDOW",
            "next_region_boundary",
        ],
        "memacc authoritative layout classification",
    )
    for stale in ("CODE_REGION_START", "CODE_REGION_END", "MMAP_REGION_START", "STACK_REGION_START"):
        if stale in memacc:
            fail(f"memacc must not retain stale hard-coded region boundary {stale}")
    if "build_id" in memacc.lower():
        fail("memacc must leave build-ID reporting to the runtime image catalog")
    for caller in ("collect_memacc_process_totals", "generate_memacc_procs"):
        require_tokens(
            function_body(procfs, caller),
            ["SharedVmemPublicationGuard", "task_memacc_layout_locked", "collect_user_memory_breakdown(task->pagemap, LAYOUT)"],
            f"{caller} publication snapshot",
        )
    memacc_layout_body = function_body(procfs, "task_memacc_layout_locked")
    require_tokens(
        memacc_layout_body,
        ["snapshot_proc_runtime_images", "IMAGE_EXACT_MAPPING", "image.image_start", "image.image_end", "layout.image_count++"],
        "memacc bounded full-image catalog",
    )
    if "range_start - image->load_base" in maps_body:
        fail("procfs maps must not present ELF virtual offsets as segment file offsets")

    require_tokens(
        perf,
        [
            "SECTION_IMAGE_MAP",
            "SECTION_IMAGE_MAP_END",
            "PROC_IMAGES_SUFFIX",
            "struct ImageMapEntry",
            "read_proc_images(stat.pid)",
            "write_section_image_map(data_fd.get(), &tracked)",
            "parse_image_map_line",
            "parse_image_map_section",
        ],
        "perf IMAGE_MAP data path",
    )
    save_body = function_body(perf, "save_perf_data")
    if save_body.find("write_section_image_map") < save_body.find("write_section_memacc_alloc_totals"):
        fail("IMAGE_MAP must be appended after the pre-existing save_perf_data sections")
    parser_body = function_body(perf, "parse_image_map_line")
    require_tokens(
        parser_body,
        [
            'line.contains(" image ")',
            'out.load_base = parse_u64(extract_value(line, " base="), 0)',
            'out.image_start = parse_u64(extract_value(line, " start="), 0)',
            'out.image_end = parse_u64(extract_value(line, " end="), 0)',
            'out.text_start = parse_u64(extract_value(line, " text_start="), 0)',
            'out.text_end = parse_u64(extract_value(line, " text_end="), 0)',
            'out.entry = parse_u64(extract_value(line, " entry="), 0)',
            'out.flags = parse_u32(extract_value(line, " flags="), 0)',
            'extract_value(line, " build_id=")',
            'percent_decode(line.substr(PATH_POS + std::string_view(" path=").size()))',
            "out.image_start < out.image_end",
        ],
        "IMAGE_MAP exact-field parser",
    )
    snapshot_writer_body = function_body(perf, "write_image_map_snapshot")
    require_tokens(
        snapshot_writer_body,
        ["output += std::to_string(pid)", "output += LINE", "write_all(fd, output)"],
        "IMAGE_MAP lossless row prefixing",
    )


def main() -> None:
    test_perf_reads_are_byte_capped()
    test_perf_run_waits_for_descendant_process_group()
    test_runtime_image_observability_is_authoritative_and_append_only()
    print("perf reads, runtime images, and append-only perf.data mapping are guarded")


if __name__ == "__main__":
    main()
