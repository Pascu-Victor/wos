#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
ELF_LOADER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "loader" / "elf_loader.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
COREDUMP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "dbg" / "coredump.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    starts: set[int] = set()
    for needle in [
        f"auto {name}(",
        f"inline auto {name}(",
        f"void {name}(",
        f"inline void {name}(",
    ]:
        candidate = source.find(needle)
        while candidate >= 0:
            starts.add(candidate)
            candidate = source.find(needle, candidate + 1)
    for start in sorted(starts):
        close = source.find(")", start)
        brace = source.find("{", close)
        semicolon = source.find(";", close)
        if close >= 0 and brace >= 0 and (semicolon < 0 or brace < semicolon):
            end = find_matching_brace(source, brace)
            return source[brace + 1 : end]
    fail(f"{name} function not found")


def function_body_containing(source: str, name: str, marker: str) -> str:
    starts: set[int] = set()
    for needle in [
        f"auto {name}(",
        f"inline auto {name}(",
        f"void {name}(",
        f"inline void {name}(",
    ]:
        candidate = source.find(needle)
        while candidate >= 0:
            starts.add(candidate)
            candidate = source.find(needle, candidate + 1)
    for start in sorted(starts):
        close = source.find(")", start)
        brace = source.find("{", close)
        semicolon = source.find(";", close)
        if close >= 0 and brace >= 0 and (semicolon < 0 or brace < semicolon):
            end = find_matching_brace(source, brace)
            body = source[brace + 1 : end]
            if marker in body:
                return body
    fail(f"{name} function containing {marker!r} not found")


def require_sparse_exec_reads(source: str) -> None:
    for snippet in [
        "constexpr size_t EXEC_SPARSE_ELF_MIN_SIZE = static_cast<size_t>(64) * 1024",
        "constexpr size_t EXEC_SHEBANG_PROBE_SIZE = 4096",
        "struct ExecImageReadResult",
        "read_exec_image_for_loader",
        "read_exec_load_segment_for_loader",
        "exec_lazy_file_page_range",
    ]:
        if snippet not in source:
            fail(f"exec sparse-read support missing: {snippet}")

    sparse_body = function_body(source, "read_sparse_elf_image")
    section_body = function_body(source, "read_exec_section_if_needed")
    for snippet in [
        "bool const LAZY_FILE_SEGMENTS = allow_lazy_file_segments && (has_dynamic_interp || !read_static_relocation_metadata)",
        "read_exec_load_segment_for_loader(fd, dst, file_size, ph, program_headers, elf_header->e_phnum, i, path",
        "ph.p_type != PT_NOTE && ph.p_type != PT_INTERP",
    ]:
        if snippet not in sparse_body:
            fail(f"sparse ELF reader must preserve loader-touched range: {snippet}")

    load_segment_body = function_body(source, "read_exec_load_segment_for_loader")
    for snippet in [
        "(ph.p_flags & PF_W) != 0U",
        "ph.p_memsz < ph.p_filesz",
        "exec_lazy_file_page_range(ph, page_no, lazy_file_offset, lazy_vaddr)",
        "!exec_pt_load_page_overlaps_other_segment(program_headers, ph_count, ph_index, lazy_vaddr)",
        "read_file_range_fully(fd, dst, file_size, ph.p_offset + bytes_before_this_page, COPY_SIZE",
    ]:
        if snippet not in load_segment_body:
            fail(f"sparse ELF load segment reader must match lazy-loader policy: {snippet}")

    lazy_page_body = function_body(source, "exec_lazy_file_page_range")
    for snippet in [
        "page_no == 0",
        "(ph.p_flags & PF_W) != 0U",
        "ph.p_memsz < ph.p_filesz",
        "DST_IN_PAGE != 0 || COPY_SIZE != ker::mod::mm::virt::PAGE_SIZE",
    ]:
        if snippet not in lazy_page_body:
            fail(f"exec lazy page policy must stay conservative: {snippet}")

    for snippet in [
        "if (!read_relocation_metadata)",
        "section.sh_type == SHT_STRTAB",
        "section.sh_type == SHT_SYMTAB",
        "section.sh_type == SHT_DYNSYM",
        "section.sh_type == SHT_REL",
        "section.sh_type == SHT_RELA",
        'std::strcmp(section_name, ".relr") == 0',
        'std::strcmp(section_name, ".relr.dyn") == 0',
    ]:
        if snippet not in section_body:
            fail(f"sparse ELF section reader must preserve loader-touched range: {snippet}")

    image_body = function_body(source, "read_exec_image_for_loader")
    for snippet in [
        "file_size <= EXEC_SPARSE_ELF_MIN_SIZE",
        "read_full_exec_image(fd, dst, file_size, path)",
        "EXEC_SHEBANG_PROBE_SIZE",
        "read_sparse_elf_image(fd, dst, file_size, path, bytes_read, read_static_relocation_metadata, USE_LAZY_FILE_SEGMENTS)",
        "result.shebang_probe_size = PROBE_SIZE",
    ]:
        if snippet not in image_body:
            fail(f"exec image reader policy missing: {snippet}")

    for snippet in [
        "bool has_dynamic_interp = false",
        "has_dynamic_interp = has_dynamic_interp || ph.p_type == PT_INTERP",
        "read_static_relocation_metadata && !has_dynamic_interp",
    ]:
        if snippet not in sparse_body:
            fail(f"dynamic ELF sparse reader must skip relocation-only metadata: {snippet}")


def require_callers_use_sparse_reader(source: str) -> None:
    if source.count("read_exec_image_for_loader(FD, elf_buffer") < 2:
        fail("both exec and execve must read executable images through the sparse reader")
    if "read_exec_image_for_loader(INTERP_FD, interp_buf" not in source:
        fail("PT_INTERP load must use the sparse reader")
    if "INTERP_PATH, false, INTERP_LAZY_FILE_SEGMENTS" not in source:
        fail("PT_INTERP load must not read static-only relocation metadata")

    forbidden = [
        "read_file_fully(FD, elf_buffer",
        "read_file_fully(INTERP_FD, interp_buf",
        "BYTES_READ != FILE_SIZE",
    ]
    for snippet in forbidden:
        if snippet in source:
            fail(f"exec path must not gate success on full-file reads: {snippet}")

    if source.count("READ_RESULT.shebang_probe_size") < 2:
        fail("shebang parsing must be bounded to the bytes actually read by the probe")
    if "READ_RESULT.bytes_read > 0 ? static_cast<uint64_t>(READ_RESULT.bytes_read) : 0" not in source:
        fail("ELF_READ perf summary must report actual bytes read")

    for snippet in [
        "vfs::File* exec_file = vfs::vfs_get_file_retain(task, FD)",
        "release_exec_file_once",
        "ElfLoadOptions const MAIN_LOAD_OPTIONS",
        ".lazy_file_ranges = EXEC_LAZY_FILE_SEGMENTS ? &main_loader_lazy_ranges : nullptr",
        "append_exec_lazy_file_ranges(new_lazy_ranges, main_loader_lazy_ranges, exec_file, exec_stat)",
        "vfs::File* interp_file = vfs::vfs_get_file_retain(task, INTERP_FD)",
        "append_exec_lazy_file_ranges(new_lazy_ranges, interp_loader_lazy_ranges, interp_file, interp_stat)",
        "publish_exec_lazy_ranges(task, new_lazy_ranges)",
        "new_lazy_ranges_published = true",
    ]:
        if snippet not in source:
            fail(f"execve lazy ELF range lifetime invariant missing: {snippet}")


def require_large_dynamic_elf_is_file_backed(source: str) -> None:
    for snippet in [
        "EXEC_FILE_BACKED_ELF_MIN_SIZE",
        "struct PreparedFileBackedElf",
        "prepare_file_backed_elf",
        "FILE_BACKED_ELF ? prepared_elf.take_task_metadata() : new uint8_t[FILE_SIZE]",
        "loader::elf::load_elf(prepared_elf.view",
        "new_task->initialize_process_image(prepared_elf.view",
        "new_task->exec_image_file = exec_file",
        "task->exec_image_file = FILE_BACKED_ELF ? exec_file : nullptr",
    ]:
        if snippet not in source:
            fail(f"large dynamic ELF file-backed invariant missing: {snippet}")

    prepare_body = function_body(source, "prepare_file_backed_elf")
    for snippet in [
        "file_size < EXEC_FILE_BACKED_ELF_MIN_SIZE",
        "phdr_bytes > EXEC_FILE_BACKED_METADATA_MAX",
        "shdr_bytes > EXEC_FILE_BACKED_METADATA_MAX",
        "shstrtab.sh_size > EXEC_FILE_BACKED_METADATA_MAX",
        "if (!has_interp)",
        "out.view.read_at = exec_file_read_at",
        "out.view.contiguous_base = nullptr",
    ]:
        if snippet not in prepare_body:
            fail(f"bounded ELF metadata validation missing: {snippet}")

    task_header = TASK_HPP.read_text()
    for snippet in ["exec_image_file", "exec_image_size", "elf_buffer_complete"]:
        if snippet not in task_header:
            fail(f"task file-backed ELF lifetime field missing: {snippet}")

    coredump_source = COREDUMP_CPP.read_text()
    for snippet in [
        "if (req.exec_image_file != nullptr)",
        "hdr.elf_size = req.exec_image_size",
        "vfs_pread_file(req.exec_image_file",
        "req.exec_image_file = task->exec_image_file",
        "task->exec_image_file = nullptr",
    ]:
        if snippet not in coredump_source:
            fail(f"coredump must preserve full file-backed ELF payload: {snippet}")

    remote_source = REMOTE_COMPUTE_CPP.read_text()
    if "task->elf_buffer_complete && task->elf_buffer != nullptr" not in remote_source:
        fail("WKI must not transmit compact ELF metadata as a complete inline executable")


def require_stdio_fallback_access_modes(source: str) -> None:
    body = function_body(source, "ensure_exec_stdio_fallbacks")
    for snippet in [
        "constexpr int STDIN_OPEN_FLAGS = 0;",
        "constexpr int STDOUT_OPEN_FLAGS = 1;",
        "int const OPEN_FLAGS = fd == 0 ? STDIN_OPEN_FLAGS : STDOUT_OPEN_FLAGS;",
        'vfs::devfs::devfs_open_path("/dev/console", OPEN_FLAGS, 0)',
        "install_exec_fd_file_checked(task, fd, new_file)",
    ]:
        if snippet not in body:
            fail(f"exec stdio fallback must preserve VFS access modes: {snippet}")


def require_spawn_actions_snapshot_user_memory_and_preserve_cloexec_sources(source: str) -> None:
    snapshot = function_body(source, "snapshot_spawn_options")
    for snippet in [
        "usercopy::copy_value_from_task",
        "usercopy::copy_from_task",
        "usercopy::copy_cstring_from_task",
        "snapshot.options.actions = ACTION_COUNT != 0 ? snapshot.actions.data() : nullptr",
    ]:
        if snippet not in snapshot:
            fail(f"spawn options must be snapshotted from user memory: {snippet}")

    clone = function_body(source, "clone_exec_fd_table_checked")
    if "bool preserve_cloexec = false" not in source:
        fail("spawn FD clone must select whether CLOEXEC descriptors survive for actions")
    for snippet in [
        "if (CLOEXEC && !preserve_cloexec)",
        "child->set_fd_cloexec(static_cast<unsigned>(key))",
    ]:
        if snippet not in clone:
            fail(f"spawn FD clone must retain action source descriptors: {snippet}")

    spawn = function_body(source, "wos_proc_spawn")
    for snippet in [
        "new (std::nothrow) SpawnOptionsSnapshot",
        "snapshot_spawn_options(*parent, options, *snapshot)",
        "wos_proc_exec_impl(path, argv, envp, &snapshot->options, 0)",
        "delete snapshot",
    ]:
        if snippet not in spawn:
            fail(f"spawn syscall must own its options snapshot: {snippet}")

    exec_impl = function_body(source, "wos_proc_exec_impl")
    clone_pos = exec_impl.find("clone_exec_fd_table_checked(parent_task, new_task, spawn_options != nullptr)")
    apply_pos = exec_impl.find("apply_spawn_options(new_task, spawn_options)")
    close_pos = exec_impl.find("close_spawn_cloexec_fds(new_task)")
    if clone_pos < 0 or apply_pos <= clone_pos or close_pos <= apply_pos:
        fail("spawn must clone CLOEXEC sources, apply ordered actions, then close remaining CLOEXEC descriptors")
    if "spawn_options == nullptr && !ensure_exec_stdio_fallbacks(new_task)" not in exec_impl:
        fail("option-aware spawn must not reopen descriptors intentionally closed by file actions")

    apply = function_body(source, "apply_spawn_options")
    if "task->pgid = options->pgroup == 0 ? task->pid : static_cast<uint64_t>(options->pgroup)" not in apply:
        fail("spawn SETPGROUP must create Ninja's requested child process group")

    if source.count("publication lost child ownership for PID %x") != 2:
        fail("spawn publication ownership diagnostics must cover local and remote children")
    for marker in [
        'dbg::log("wos_proc_exec: remote publication lost child ownership for PID %x", CHILD_PID);',
        'dbg::log("wos_proc_exec: local publication lost child ownership for PID %x", CHILD_PID);',
    ]:
        marker_pos = source.find(marker)
        if marker_pos < 0 or "return CHILD_PID;" not in source[marker_pos : marker_pos + 450]:
            fail("published spawn failures must return the child PID instead of triggering duplicate fallback")


def require_loader_does_not_scan_unread_symbol_payloads(loader_source: str) -> None:
    body = function_body_containing(loader_source, "load_elf_impl", "if (REGISTER_SPECIAL_SYMBOLS)")
    registration_gate = body.find("if (REGISTER_SPECIAL_SYMBOLS)")
    registration = body.find("debug::register_process", registration_gate)
    if registration_gate < 0 or registration < registration_gate:
        fail("interpreter loads must not create a duplicate PID debug-registry row")
    marker = body.find("// Print debug info for verification", registration)
    if marker < 0:
        fail("loader sparse-symbol scan boundary missing")
    tail = body[marker:]
    for forbidden in [
        "SHT_SYMTAB",
        "SHT_DYNSYM",
        "section->sh_link",
        "reinterpret_cast<Elf64_Sym*>",
    ]:
        if forbidden in tail:
            fail(f"loader must not touch optional symbol payloads after sparse exec reads: {forbidden}")


def require_loader_lazy_file_ranges(loader_source: str) -> None:
    for snippet in [
        "struct ElfLazyLoadRange",
        "struct ElfLoadOptions",
        "ElfLazyLoadRangeVec* lazy_file_ranges",
        "append_lazy_load_range",
        "lazy_load_page_range",
    ]:
        if snippet not in (ROOT / "modules" / "kern" / "src" / "platform" / "loader" / "elf_loader.hpp").read_text() + loader_source:
            fail(f"loader lazy range API missing: {snippet}")

    load_body = function_body_containing(loader_source, "load_elf_impl", "ENABLE_LAZY_FILE_RANGES")
    for snippet in [
        "options.lazy_file_ranges != nullptr && (has_dynamic_interp || BASE_ADDRESS != 0)",
        "lazy_load_page_range(current_header, j, elf_file.load_base, lazy_range)",
        "!pt_load_page_overlaps_other_segment(elf_file, current_header, lazy_range.vaddr)",
        "!mod::mm::virt::is_page_mapped(pagemap, lazy_range.vaddr)",
        "append_lazy_load_range(options.lazy_file_ranges, lazy_range)",
        "if (!mod::mm::virt::is_page_mapped(pagemap, va))",
    ]:
        if snippet not in load_body:
            fail(f"loader lazy mapping invariant missing: {snippet}")

    lazy_body = function_body(loader_source, "lazy_load_page_range")
    for snippet in [
        "page_no == 0",
        "(program_header->p_flags & PF_W) != 0U",
        "program_header->p_memsz < program_header->p_filesz",
        "DST_IN_PAGE != 0 || COPY_SIZE != mod::mm::virt::PAGE_SIZE",
        ".flags = ker::abi::vmem::MAP_PRIVATE",
    ]:
        if snippet not in lazy_body:
            fail(f"loader lazy page policy must stay conservative: {snippet}")


def main() -> None:
    source = EXEC_CPP.read_text()
    loader_source = ELF_LOADER_CPP.read_text()
    require_sparse_exec_reads(source)
    require_callers_use_sparse_reader(source)
    require_large_dynamic_elf_is_file_backed(source)
    require_stdio_fallback_access_modes(source)
    require_spawn_actions_snapshot_user_memory_and_preserve_cloexec_sources(source)
    require_loader_does_not_scan_unread_symbol_payloads(loader_source)
    require_loader_lazy_file_ranges(loader_source)
    print("exec sparse-read source invariants hold")


if __name__ == "__main__":
    main()
