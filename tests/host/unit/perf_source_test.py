#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PERF_SRC_DIR = ROOT / "modules" / "perf" / "src"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
PROCFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.hpp"
MEMACC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "memacc.cpp"
PERF_EVENTS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "perf" / "perf_events.hpp"
PERF_CMAKE = ROOT / "modules" / "perf" / "CMakeLists.txt"


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


def test_structured_perf_is_opt_in_atomic_and_legacy_projected() -> None:
    source = read_perf_source()
    cmake = PERF_CMAKE.read_text()
    perf_events = PERF_EVENTS_HPP.read_text()

    require_tokens(
        perf_events,
        [
            "struct PerfEvent",
            "uint64_t ts_ns",
            "uint64_t pid",
            "static_assert(sizeof(PerfEvent) == 48",
        ],
        "unchanged hot PerfEvent contract",
    )
    require_tokens(
        cmake,
        ["src/perf_data.cpp", "src/perf_event_json.cpp", "wos::telemetry"],
        "perf shared telemetry build integration",
    )
    require_tokens(
        source,
        [
            'MAGIC{"WOSPERF\\0", 8}',
            "CONTAINER_MAJOR = 1",
            "MAX_CONTAINER_BYTES",
            "wos::telemetry::encode_container",
            "wos::telemetry::decode_container",
            "wos::telemetry::crc32c",
            'ARG == "--structured"',
            '.name = "data-info"',
            '.name = "data-convert"',
            '.name = "data-export"',
        ],
        "structured perf surface",
    )

    read_file_body = function_body(source, "read_file")
    require_tokens(
        read_file_body,
        [
            "path == PERF_DATA_FILE",
            "perf_data::load(path)",
            "std::move(loaded.file->legacy_snapshot)",
            "return read_fd(fd, initial_capacity, read_limit_for_path(path))",
        ],
        "transparent exact legacy projection",
    )

    require_tokens(source, ["auto cmd_record(int ms, const char* filter, bool structured) -> bool"], "opt-in perf record signature")
    record_body = function_body(source, "cmd_record")
    require_tokens(
        record_body,
        ["return !structured || finalize_structured_perf_data()"],
        "opt-in perf record failure propagation",
    )
    run_body = function_body(source, "cmd_run")
    require_tokens(
        run_body,
        ['ARG == "--structured"', "if (structured && !finalize_structured_perf_data())", "return output_ok"],
        "opt-in perf run failure propagation",
    )
    typed_body = function_body(source, "build_typed_perf_events")
    require_tokens(
        typed_body,
        ["result.jsonl.size() >= perf_data::MAX_TYPED_EVENTS_BYTES"],
        "typed perf payload checked subtraction",
    )

    convert_body = function_body(source, "convert_legacy_file_atomic")
    require_tokens(
        convert_body,
        [
            "O_WRONLY | O_CREAT | O_EXCL",
            "write_all_checked",
            "fsync(temp_fd)",
            "close(temp_fd)",
            "rename(temp_path.c_str(), owned_path.c_str())",
            "unlink(temp_path.c_str())",
        ],
        "atomic structured conversion",
    )
    parse_body = function_body(source, "parse")
    require_tokens(
        parse_body,
        [
            "PARTIAL_MAGIC",
            "HAS_MAGIC",
            "validate_legacy_text(bytes, format, legacy_error)",
            "unknown required structured section",
        ],
        "corruption must not fall back to legacy",
    )
    legacy_body = function_body(source, "validate_legacy_text")
    require_tokens(
        legacy_body,
        [
            "legacy raw event input ends with a partial record",
            "legacy section payload ends with a partial record",
            "legacy section markers are nested or mismatched",
        ],
        "partial legacy rejection",
    )
    export_body = function_body(source, "cmd_data_export")
    require_tokens(
        export_body,
        [
            "perf_data::load(path)",
            "typed_events_jsonl",
            "build_typed_perf_events",
            "write(STDOUT_FILENO",
            "errno == EINTR",
        ],
        "validated WOSDBG JSONL export",
    )


def test_typed_perf_mapping_is_truthful() -> None:
    source = read_perf_source()
    mapper = function_body(source, "serialize_typed_perf_event")
    require_tokens(
        mapper,
        [
            '"node_id"',
            '"pid"',
            '"cpu"',
            '"boot_monotonic"',
            '"thread_identity_quality"',
            '"unavailable_in_legacy_kperf"',
            '"instance_id"',
        ],
        "typed perf event mapping",
    )
    require_tokens(
        source,
        ['"perf.sample"', '"perf.switch"', '"perf.wake"', '"perf.sleep"', '"perf.container_stat"', '"perf.wki"'],
        "typed perf event kinds",
    )
    if 'identity.emplace("tid"' in mapper or '{"tid"' in mapper:
        fail("legacy /proc/kperf conversion must not invent a distinct TID")


def test_kernel_kperf_formatter_is_transactionally_bounded() -> None:
    procfs = PROCFS_CPP.read_text()
    writer = procfs[procfs.find("class PerfEventTextWriter") : procfs.find("// Generate content for /proc/kperf")]
    require_tokens(
        writer,
        [
            "if (cursor_ >= end_)",
            "complete_ = false",
            "append_dec(0U - static_cast<uint64_t>(value))",
            "[[nodiscard]] auto complete() const -> bool",
        ],
        "bounded /proc/kperf event writer",
    )

    formatter = function_body(procfs, "generate_kperf")
    require_tokens(
        formatter,
        [
            "char* const EVENT_START = p",
            "PerfEventTextWriter writer(p, end)",
            "writer.append('\\n')",
            "if (!writer.complete())",
            "p = EVENT_START",
            "output_full = true",
        ],
        "transactional /proc/kperf formatter",
    )
    if "*p++" in formatter:
        fail("/proc/kperf formatting must not bypass the bounded event writer")


def main() -> None:
    test_perf_reads_are_byte_capped()
    test_perf_run_waits_for_descendant_process_group()
    test_runtime_image_observability_is_authoritative_and_append_only()
    test_structured_perf_is_opt_in_atomic_and_legacy_projected()
    test_typed_perf_mapping_is_truthful()
    test_kernel_kperf_formatter_is_transactionally_bounded()
    print("perf reads, runtime images, and structured perf compatibility are guarded")


if __name__ == "__main__":
    main()
