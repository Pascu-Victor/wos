#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
ELF_LOADER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "loader" / "elf_loader.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
COREDUMP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "dbg" / "coredump.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"
MLIBC_RTLD_MAIN_CPP = ROOT / "toolchain" / "src" / "mlibc" / "options" / "rtld" / "generic" / "main.cpp"


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
    if source.count("read_exec_image_for_loader(exec_file, elf_buffer") < 2:
        fail("both exec and execve must read executable images through the sparse reader")
    if "read_exec_image_for_loader(interp_file, interp_buf" not in source:
        fail("PT_INTERP load must use the sparse reader")
    if "INTERP_PATH, false, INTERP_LAZY_FILE_SEGMENTS" not in source:
        fail("PT_INTERP load must not read static-only relocation metadata")

    forbidden = [
        "read_file_fully(exec_file, elf_buffer",
        "read_file_fully(interp_file, interp_buf",
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
        "vfs::vfs_open_file_as(*task, request->admitted_credentials, exec_path, 0, &exec_file)",
        "release_exec_file_once",
        "ElfLoadOptions const MAIN_LOAD_OPTIONS",
        ".lazy_file_ranges = EXEC_LAZY_FILE_SEGMENTS ? &main_loader_lazy_ranges : nullptr",
        "append_exec_lazy_file_ranges(new_lazy_ranges, main_loader_lazy_ranges, exec_file, exec_stat)",
        "vfs::vfs_open_file_as(*task, request->admitted_credentials, INTERP_PATH, interp_open_flags,",
        "append_exec_lazy_file_ranges(new_lazy_ranges, interp_loader_lazy_ranges, interp_file, interp_stat)",
        "swap_exec_lazy_ranges(task, image.lazy_ranges, retired.lazy_ranges)",
        "prepared.release_after_commit()",
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
        ".exec_image_file = FILE_BACKED_ELF ? exec_file : nullptr",
        "task->exec_image_file = image.exec_image_file",
    ]:
        if snippet not in source:
            fail(f"large dynamic ELF file-backed invariant missing: {snippet}")

    prepare_body = function_body(source, "prepare_file_backed_elf")
    if "size_t main_minimum_size = EXEC_FILE_BACKED_ELF_MIN_SIZE)" not in source:
        fail("default main ELF compact threshold must remain 8 MiB")
    for snippet in [
        "file_size < main_minimum_size",
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
    if "BINARY_COMPLETE && BINARY != nullptr" not in remote_source:
        fail("WKI must not transmit compact ELF metadata as a complete inline executable")


def require_stdio_fallback_access_modes(source: str) -> None:
    body = function_body(source, "ensure_exec_stdio_fallbacks")
    for name in ["ensure_exec_stdio_fallbacks", "collect_cloexec_fds_locked"]:
        owner_body = function_body(source, name)
        owner = "task = task != nullptr ? task->credential_owner() : nullptr;"
        if owner not in owner_body or owner_body.index(owner) > owner_body.index("task->fd_table_lock.lock_irqsave()"):
            fail(f"{name} must resolve the retained process FD owner before table access")
    for snippet in [
        "constexpr int STDIN_OPEN_FLAGS = 0;",
        "constexpr int STDOUT_OPEN_FLAGS = 1;",
        "int const OPEN_FLAGS = fd == 0 ? STDIN_OPEN_FLAGS : STDOUT_OPEN_FLAGS;",
        'vfs::devfs::devfs_open_path(CREDENTIALS, "/dev/console", OPEN_FLAGS, 0)',
        "install_exec_fd_file_checked(task, fd, new_file)",
    ]:
        if snippet not in body:
            fail(f"exec stdio fallback must preserve VFS access modes: {snippet}")


def require_spawn_actions_snapshot_user_memory_and_preserve_cloexec_sources(source: str) -> None:
    snapshot = function_body(source, "snapshot_spawn_options")
    for snippet in [
        "usercopy::copy_value_from_task",
        "usercopy::copy_from_task",
        "usercopy::copy_cstring_from_task_status",
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
        "snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, arguments)",
        "new (std::nothrow) SpawnOptionsSnapshot",
        "snapshot_spawn_options(*parent, options_addr, *snapshot)",
        "wos_proc_exec_impl(arguments.path.data(), arguments.argv, arguments.envp, &snapshot->options, 0, ADMITTED_CREDENTIALS, MEMORY_POLICY)",
        "delete snapshot",
    ]:
        if snippet not in " ".join(spawn.split()):
            fail(f"spawn syscall must own its options snapshot: {snippet}")

    exec_entry = function_body(source, "wos_proc_exec")
    for snippet in [
        "snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, snapshot)",
        "wos_proc_exec_impl(snapshot.path.data(), snapshot.argv, snapshot.envp, nullptr, 0, ADMITTED_CREDENTIALS, MEMORY_POLICY)",
    ]:
        if snippet not in " ".join(exec_entry.split()):
            fail(f"exec syscall must deep-snapshot user path/vector memory: {snippet}")

    exec_impl = function_body(source, "wos_proc_exec_impl")
    clone_pos = exec_impl.find("clone_exec_fd_table_checked(parent_task, new_task, spawn_options != nullptr)")
    apply_pos = exec_impl.find("apply_spawn_options(new_task, spawn_options, &options_error)")
    close_pos = exec_impl.find("close_spawn_cloexec_fds(new_task)")
    if clone_pos < 0 or apply_pos <= clone_pos or close_pos <= apply_pos:
        fail("spawn must clone CLOEXEC sources, apply ordered actions, then close remaining CLOEXEC descriptors")
    if "spawn_options == nullptr && !ensure_exec_stdio_fallbacks(new_task)" not in exec_impl:
        fail("option-aware spawn must not reopen descriptors intentionally closed by file actions")

    apply = function_body(source, "apply_spawn_options")
    group_admission = apply.find("session::prepare_spawn_group(*task, options->pgroup)")
    file_actions = apply.find("for (uint64_t i = 0; i < options->action_count;")
    if group_admission < 0 or file_actions <= group_admission:
        fail("spawn must admit the requested process group before destructive file actions")

    if source.count("publication lost child ownership for PID %x") != 2:
        fail("spawn publication ownership diagnostics must cover local and remote children")
    if "uint64_t const CHILD_VISIBLE_PID = new_task->namespace_tid();" not in exec_impl:
        fail("spawn publication results must capture the child's exact namespace-visible PID")
    for marker in [
        'dbg::log("wos_proc_exec: remote publication lost child ownership for PID %x", CHILD_PID);',
        'dbg::log("wos_proc_exec: local publication lost child ownership for PID %x", CHILD_PID);',
    ]:
        marker_pos = source.find(marker)
        failure_if = source.rfind("if (!sched::task::release_unpublished_process(", 0, marker_pos)
        if marker_pos < 0 or failure_if < 0:
            fail("published spawn ownership failure branch is missing")
        brace = source.index("{", failure_if)
        end = find_matching_brace(source, brace)
        failure = source[brace:end]
        if marker_pos >= end or "return CHILD_VISIBLE_PID;" not in failure or "return 0;" in failure or "return static_cast<uint64_t>(remote_error);" in failure:
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


def require_interpreter_preserves_primary_debug_mappings(loader_source: str) -> None:
    load_body = function_body_containing(loader_source, "load_elf_impl", "REGISTER_SPECIAL_SYMBOLS")
    if "load_section_headers(elf_file, pagemap, DEBUG_PID, debug_metadata_plan)" not in load_body:
        fail("ELF loader must route primary-image debug mappings through the selected registry PID")
    for snippet in [
        "uint64_t const DEBUG_PID = options.debug_registry_pid != 0 ? options.debug_registry_pid : pid",
        "bool const IS_PRIMARY_IMAGE = options.image_role == mod::mm::user_layout::ImageRole::MAIN",
    ]:
        if snippet not in load_body:
            fail(f"staged debug registry and explicit image role missing: {snippet}")

    section_body = function_body(loader_source, "load_section_headers")
    gate = section_body.find("if (metadata_plan.install)")
    section_mapping = section_body.find("DEBUG_SECTION_HEADERS_VADDR")
    string_mapping = section_body.find("DEBUG_STRING_TABLE_VADDR")
    section_iteration = section_body.find("for (size_t section_index")
    if gate < 0 or section_mapping <= gate or string_mapping <= gate:
        fail("fixed PID debug metadata mappings must be gated to the primary image")
    if section_iteration <= string_mapping:
        fail("interpreter section rows must remain appendable without replacing primary debug mappings")


def require_bounded_contiguous_loader(loader_source: str) -> None:
    header = (ROOT / "modules" / "kern" / "src" / "platform" / "loader" / "elf_loader.hpp").read_text()
    task_source = (ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp").read_text()
    remote_source = REMOTE_COMPUTE_CPP.read_text()
    for forbidden in ["load_elf(ElfFile*", "extract_tls_info(void*", "elf.logical_size = std::numeric_limits<uint64_t>::max()"]:
        if forbidden in header + loader_source:
            fail(f"unbounded contiguous ELF API survived: {forbidden}")
    for snippet in [
        "load_elf(const uint8_t* data, size_t size",
        "parse_elf(const uint8_t* base, uint64_t size, ElfFile& elf)",
        "inspect_tls(const uint8_t* data, size_t size, TlsModule& out)",
        "load_elf(interp_buf, interp_size",
        "new_task->initialize_process_image(elf_buffer, binary_len)",
    ]:
        if snippet not in header + loader_source + task_source + remote_source:
            fail(f"bounded ELF length propagation missing: {snippet}")
    file_source = EXEC_CPP.read_text()
    wrapper = function_body(file_source, "create_file_backed_process_task")
    assert "return create_file_backed_process_task_impl(" in wrapper
    assert "failure_observations, interpreter_error, nullptr, nullptr, true)" in " ".join(wrapper.split())
    file_body = function_body(file_source, "create_file_backed_process_task_impl")
    ordered = ["claim_unpublished_process(OWNER, new_task)", "new_task->resources = resources",
               "initialize_credentials(*new_task, credentials)", "reservation.attach(*new_task)", "initialize_process_image(prepared_elf.view"]
    positions = [file_body.find(token) for token in ordered]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        fail("file-backed receiver must own the child and apply policy/population before image initialization")


def require_remote_execfn_auxv() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    for snippet in [
        "AT_EXECFN_ADDR = push_string(new_task->exe_path.data())",
        "constexpr uint64_t AT_EXECFN = 31",
        "append_auxv(AT_EXECFN, AT_EXECFN_ADDR)",
        "size_t const AUXV_QWORDS = 18 + ker::mod::security::exec_policy::IDENTITY_AUXV_WORDS + (new_task->interp_base != 0 ? 2 : 0)",
    ]:
        if snippet not in source:
            fail(f"WKI receiver AT_EXECFN invariant missing: {snippet}")


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
        "options.lazy_file_ranges != nullptr && (has_dynamic_interp || !IS_PRIMARY_IMAGE)",
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


def require_coredump_entropy_redaction() -> None:
    source = COREDUMP_CPP.read_text()
    body = function_body(source, "perform_coredump")
    for snippet in [
        "REDACT_AT_RANDOM = overlaps_page(req.at_random_addr, AT_RANDOM_SIZE)",
        "REDACT_TCB_CANARY = overlaps_page(TCB_CANARY_ADDR, STACK_CANARY_SIZE)",
        "redact_range(req.at_random_addr, AT_RANDOM_SIZE)",
        "redact_range(TCB_CANARY_ADDR, STACK_CANARY_SIZE)",
    ]:
        if snippet not in body:
            fail(f"coredump entropy redaction invariant missing: {snippet}")


def require_rtld_self_relocation_is_single_pass() -> None:
    source = MLIBC_RTLD_MAIN_CPP.read_text()
    for snippet in [
        "relocateSelf() completed before interpreterMain()",
        "ldso_soname, frg::string<MemoryAllocator>{getAllocator()}, ldso_base, _DYNAMIC, 0",
    ]:
        if snippet not in source:
            fail(f"initial ld.so must be older than the first normal relocation transaction: {snippet}")


def require_main_exec_retirement_order(source: str) -> None:
    body = function_body(source, "finish_native_exec_image")
    commit = function_body(source, "commit_native_exec_image")
    tokens = ["MutexGuard action_guard", "SharedVmemPublicationGuard publication_guard",
              "prepare_exec_credentials(", "exec_admission.begin(*task)", "publish_staged_process(",
              "if (commit_published)", "retired.elf_buffer = task->elf_buffer", "task->mark_exec_completed()",
              "publish_exec_credentials(", "replace_pagemap_after_usercopy_quiescence(", "swap_exec_lazy_ranges(", "prepared.release_after_commit()"]
    positions = [commit.index(token) for token in tokens]
    if positions != sorted(positions):
        fail("native image commit must validate authorization and admission before publishing or detaching old storage")
    for token in ["get_current_task(", "syscall_kernel_stack", "syscall_scratch_area", "retire_exec_siblings(", "vfs_close("]:
        if token in commit:
            fail("image publication must not infer its subject or change executor ownership/cleanup")
    tokens = ["commit_native_exec_image(task, *PREPARED)", "if (COMMIT_RESULT < 0)",
              "cleanup_exec_preparation_with_status(*task,", "retire_exec_siblings(*task)",
              "task->reset_signal_state_for_exec()", "collect_cloexec_fds_locked(task, fds)",
              "ensure_exec_stdio_fallbacks(task)", "Exec admission and owned storage are gone", "// === CRITICAL SECTION"]
    positions = [body.index(token) for token in tokens]
    if positions != sorted(positions):
        fail("exec retirement must follow validated commit and precede userspace return")
    failure_start = body.index("if (COMMIT_RESULT < 0)")
    failure = body[failure_start:body.index("return static_cast<uint64_t>(COMMIT_RESULT)", failure_start)]
    if "cleanup_exec_preparation_with_status(*task," not in failure or "retire_exec_siblings" in failure:
        fail("failed exec must recover owned admission/storage without retiring siblings")
    if body.rindex("cleanup_exec_preparation_with_status(*task,") < body.index("ensure_exec_stdio_fallbacks(task)"):
        fail("successful exec must retain recovery ownership and admission through final shared state")
    if body.index('asm volatile("mov %0, %%cr3"') > body.index("retire_exec_siblings(*task)"):
        fail("retirement must wait only after replacement CR3 is active")
    if "*reinterpret_cast<cpu::GPRegs*>(stack_base) = INITIAL_REGS;" not in body:
        fail("exec user entry must use private startup registers across retirement/preemption")
    resource = (ROOT / "modules/kern/src/platform/sched/resource.cpp").read_text()
    if "owner->resource_cpu_admission_closed || owner->resource_exec_admission_closed" not in resource:
        fail("thread binding must observe temporary exec admission")
    finish = function_body(resource, "ExecAdmission::finish")
    if "resource_cpu_admission_closed" in finish:
        fail("exec token must not reopen permanent exit admission")
    exit_source = (ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp").read_text()
    retirement = function_body(exit_source, "retire_exec_siblings")
    if not (retirement.index("executor.is_thread") < retirement.index("request_thread_group_exit") <
            retirement.index("resource_cpu_threads.load") < retirement.index("kern_yield")):
        fail("main exec must use same-group cooperative requests and wait for CPU cleanup")


def require_tracked_exec_cleanup(source: str) -> None:
    cleanup = function_body(source, "cleanup_exec_preparation_with_status")
    order = ["compare_exchange_strong", "owned_exec_preparation.load", "prepared->discard()",
             "owned_exec_preparation.exchange(nullptr", "delete prepared", "teardown_in_progress.store(false"]
    positions = [cleanup.index(token) for token in order]
    if positions != sorted(positions):
        fail("exec recovery must stay registered and guarded through blocking discard")
    if "process_exit_requested.store" in cleanup:
        fail("exec cleanup must preserve pending group exit")
    adoption = function_body(source, "adopt_exec_preparation")
    if adoption.index("compare_exchange_strong") > adoption.index("prepared.release()"):
        fail("only successful slot adoption may consume preparation ownership")
    body = function_body(source, "finish_native_exec_image")
    begin = body.index("int COMMIT_RESULT = prepare_native_exec_quiescence")
    guard = "task->exec_preparation_teardown_in_progress.store(true"
    unguard = "task->exec_preparation_teardown_in_progress.store(false"
    failed_return = body.index("return static_cast<uint64_t>(COMMIT_RESULT)", begin)
    active = body.index("PREPARED->mark_activated()", failed_return)
    if not (body.rfind(guard, 0, begin) >= 0 and
            body.index(unguard, begin) < failed_return and
            body.index(unguard, failed_return) > active and
            body.index('asm volatile("mov %0, %%cr3"', failed_return) < active and
            body.index("cpu_set_msr(IA32_KERNEL_GS_BASE", failed_return) < active):
        fail("fatal recovery must remain deferred from partial publication through CR3/TLS activation")
    for forbidden in ["ExecAdmission exec_admission;", "RetiredExecImage retired;", "exec_admission.finish()"]:
        if forbidden in body:
            fail("noreturn exec resources must not be owned only by the caller stack")
    prepared = source[source.index("class PreparedNativeExecImage {"):]
    discard = function_body(prepared, "discard")
    if not (discard.index("m_phase == Phase::PUBLISHED") < discard.index("std::exchange(m_phase, Phase::DISCARDED)") <
            discard.index("discard_retired()") < discard.index("m_admission.finish()")):
        fail("active cleanup must protect activation and release old storage before admission")
    if discard.rindex("m_admission.finish()") > discard.index("loader::debug::unregister_process"):
        fail("failed publication must reopen temporary admission before private VFS/MM cleanup")
    retired = function_body(prepared, "discard_retired")
    for token in ["m_retired.pagemap != m_image.pagemap", "!m_retired.pagemap_has_other_publishers",
                  "release_file_mmap_ranges_for_pagemap(m_retired.pagemap)", "release_pagemap(m_retired.pagemap, m_origin_pid)",
                  "m_retired.thread != m_image.thread", "destroy_thread(m_retired.thread)"]:
        if token not in retired:
            fail("retired cleanup must preserve replacement and last-publisher ownership")
    exit_source = (ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp").read_text()
    thread_source = (ROOT / "modules/kern/src/syscalls_impl/multiproc/threadControl.cpp").read_text()
    signal_source = (ROOT / "modules/kern/src/platform/sys/signal.cpp").read_text()
    for text, function, task in [(exit_source, "wos_proc_exit_with_wait_status", "current_task"),
                                  (thread_source, "wos_thread_exit_current", "task")]:
        code = function_body(text, function)
        tokens = [f"{task}->exit_in_progress = true", f"{task}->quiesce_usercopy_pagemap()",
                  f"release_lazy_vmem_ranges(*{task})", f"cleanup_exec_preparation(*{task})",
                  f"wki_wait_cleanup_for_task({task})", f"{task}->has_exited = true"]
        positions = [code.index(token) for token in tokens]
        if positions != sorted(positions):
            fail("process/thread exit must recover exec storage before waiter teardown and death")
        if function == "wos_thread_exit_current" and code.index(f"release_lazy_vmem_ranges(*{task})") > code.index("finish_cpu()"):
            fail("worker executable references must retire before the owner can publish group exit")
    vmem_source = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    add_range = function_body(vmem_source, "add_lazy_vmem_range_entry")
    if not (add_range.index("lazy_vmem_lock.lock_irqsave()") < add_range.index("usercopy_pagemap_closing.load") <
            add_range.index("for (const auto& range")):
        fail("shared mmap must not refill a closing task's lazy references")
    for text, function in [(exit_source, "take_requested_process_exit"),
                           (signal_source, "exit_current_on_pending_fatal_default_signal")]:
        code = function_body(text, function)
        if "exec_preparation_teardown_in_progress.load" not in code[:code.index("return")]:
            fail("both fatal/group exit entrypoints must defer during exec preparation cleanup")


def require_native_exec_borrows_kernel_argument_snapshot(source: str) -> None:
    wrapper = function_body(source, "wos_proc_execve")
    if wrapper.index("snapshot_exec_arguments(*task") >= wrapper.index("wos_proc_execve_impl(snapshot.path.data(), snapshot.argv, snapshot.envp"):
        fail("native exec must snapshot user arguments before borrowing")
    body = function_body(source, "wos_proc_execve_impl")
    for token in ["K_ARGV = argv != nullptr ? argv : empty_vector.data()",
                  "K_ENVP = envp != nullptr ? envp : empty_vector.data()",
                  "try_process_remote_exec(*task", "exec_stat, EXEC_NOSUID, remote_error", "push_string(K_ARGV[i])", "push_string(K_ENVP[i])"]:
        if token not in body:
            fail("native exec must use snapshotted argument storage and preserve empty-vector remote classification")
    for forbidden in ["new char*[argv_count", "new char*[envp_count", "free_kernel_arg_env", "delete[] K_ARGV", "delete[] K_ENVP"]:
        if forbidden in body:
            fail("native exec must not duplicate or release borrowed argument ownership")


def require_request_arguments_are_published_before_usercopy(source: str) -> None:
    wrapper = function_body(source, "wos_proc_execve")
    if wrapper.index("adopt_exec_arguments(*task, pending)") > wrapper.index("snapshot_exec_arguments(*task"):
        fail("root argument storage must be recoverable before faulting usercopy")
    vector = function_body(source, "snapshot_exec_vector")
    if "auto& string_addrs = snapshot.input_string_addrs" not in vector:
        fail("input-address spill must belong to snapshot recovery")
    cleanup = function_body(source, "cleanup_exec_preparation_with_status")
    order = ["compare_exchange_strong", "prepared->discard()", "owned_exec_arguments.load", "delete arguments",
             "owned_exec_arguments.exchange", "exec_preparation_teardown_in_progress.store(false"]
    if [cleanup.index(token) for token in order] != sorted(cleanup.index(token) for token in order):
        fail("shared guard must protect image and argument recovery through final disposal")
    shebang = function_body(source, "exec_shebang_script")
    if "request->prepare_shebang" not in shebang or "return static_cast<uint64_t>(RESULT)" not in shebang:
        fail("native shebang must own forwarded bytes and preserve preparation errno")
    native = function_body(source, "wos_proc_execve_impl")
    if "task->owned_exec_arguments.load" not in native:
        fail("native recursion must use its task-owned argument request")


def require_native_loader_scratch_is_request_owned(source: str) -> None:
    native = function_body(source, "wos_proc_execve_impl")
    if native.index("request->native_scratch(shebang_depth)") > native.index("vfs::vfs_open_file_as(*task"):
        fail("native scratch must be retained before potentially blocking loader work")
    for snippet in ["auto& prepared_elf = scratch->prepared_elf",
                    "auto& prepared_interp = scratch->prepared_interp",
                    "auto*& interp_buf = scratch->interp_buf",
                    "scratch->reset_interp_storage()",
                    "auto& main_loader_lazy_ranges = scratch->main_lazy_ranges",
                    "auto& interp_loader_lazy_ranges = scratch->interp_lazy_ranges",
                    "auto*& argv_addrs = scratch->argv_addrs",
                    "auto*& envp_addrs = scratch->envp_addrs"]:
        if snippet not in native:
            fail(f"native temporary storage must belong to the request: {snippet}")
    for snippet in ["PreparedFileBackedElf prepared_elf;", "PreparedFileBackedElf prepared_interp;",
                    "auto* interp_buf = new", "delete[] interp_buf", "ElfLazyLoadRangeVec main_loader_lazy_ranges;",
                    "ElfLazyLoadRangeVec interp_loader_lazy_ranges;", "delete[] argv_addrs", "delete[] envp_addrs"]:
        if snippet in native:
            fail(f"native scratch must not retain independent lexical ownership: {snippet}")
    owner_start = source.index("class ExecRequestArguments {")
    owner = source[owner_start:source.index("// Owns prepared/retired storage", owner_start)]
    for snippet in ["std::array<NativeScratch, MAX_SHEBANG_DEPTH + 1>", "~NativeScratch()",
                    "delete[] argv_addrs", "delete[] envp_addrs"]:
        if snippet not in owner:
            fail("every allowed recursion depth must have scratch with one cleanup owner")


def require_preparation_unwinds_before_completion(source: str) -> None:
    prepare = function_body(source, "wos_proc_execve_impl")
    wrapper = function_body(source, "wos_proc_execve")
    finish = function_body(source, "finish_native_exec_image")
    for forbidden in ["commit_native_exec_image(", "retire_exec_siblings(", "mark_activated(", "mov %0, %%cr3"]:
        if forbidden in prepare:
            fail("recursive loader must return its owned preparation before image completion")
    if prepare.index("adopt_exec_preparation(*task, prepared)") > prepare.index("request->completion ="):
        fail("only an adopted preparation may publish completion metadata")
    order = ["wos_proc_execve_impl(snapshot.path.data()", "owned_exec_preparation.load",
             "if (RESULT == 0 && prepared != nullptr)", "return finish_native_exec_image(task, prepared, request->completion)"]
    if [wrapper.index(t) for t in order] != sorted(wrapper.index(t) for t in order):
        fail("wrapper must complete only successful native preparation after recursive unwind")
    if "auto const INITIAL_REGS = PREPARED->view().registers" not in finish:
        fail("completion must copy startup registers before destroying the image owner")
    after_cleanup = finish[finish.rindex("cleanup_exec_preparation_with_status(*task,"):]
    for forbidden in ["PREPARED->", "request->", "completion."]:
        if forbidden in after_cleanup:
            fail("return-frame patch must not borrow disposed preparation/request data")


def require_worker_handoff_ownership_and_exit_order(source: str) -> None:
    submit = function_body(source, "handoff_worker_exec")
    order = ["SharedVmemPublicationGuard", "prepare_exec_credentials", "prepared.admission().begin(worker)",
             "resource_lock.lock_irqsave", "pending_exec_handoff.store", "resource_lock.unlock_irqrestore",
             "wake_task_for_signal", "handoff.result.load", "wos_thread_exit_current"]
    if [submit.index(t) for t in order] != sorted(submit.index(t) for t in order):
        fail("worker admission/publication/wait/retirement must follow single-owner protocol")
    claim = function_body(source, "claim_pending_exec_handoff")
    order = ["resource_lock.lock_irqsave", "owned_exec_preparation.exchange(nullptr", "owned_exec_preparation.store(prepared",
             "pending_exec_handoff.store(nullptr", "active_exec_handoff.store(handoff"]
    if [claim.index(t) for t in order] != sorted(claim.index(t) for t in order):
        fail("owner claim must atomically transfer the parked worker image slot")
    ack = function_body(source, "acknowledge_worker_exec")
    if ack.index("active_exec_handoff.store(nullptr") > ack.index("handoff.result.store"):
        fail("owner must detach the request before allowing worker destruction")
    exit_source = (ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp").read_text()
    exiting = function_body(exit_source, "wos_proc_exit_with_wait_status")
    if exiting.index("cancel_pending_exec_handoff(*current_task)") > exiting.index("while (current_task->resource_cpu_threads"):
        fail("owner exit must cancel handoff before waiting for worker CPU cleanup")
    cancel = function_body(source, "cancel_pending_exec_handoff")
    for token in ["pending_exec_handoff.exchange(nullptr", "active_exec_handoff.exchange(nullptr", "-ECANCELED"]:
        if token not in cancel:
            fail("exit must cancel both queued and claimed worker waits")
    prepare = function_body(source, "wos_proc_execve_impl")
    if prepare.index("if (PROCESS_OWNER == nullptr)") > prepare.index("PROCESS_OWNER->namespace_tid()"):
        fail("native initial TCB identity requires a validated process owner")
    finish = function_body(source, "finish_native_exec_image")
    entry_start = finish.index("if (OWNER_HANDOFF)")
    entry = finish[entry_start:]
    order = ['asm volatile("cli"', "set_voluntary_blocked(false)", "context.frame.rip = new_initial_rip",
             "SYNTHETIC_USER_RETURN", "wos_asm_enter_exec_owner"]
    if [entry.index(t) for t in order] != sorted(entry.index(t) for t in order):
        fail("owner entry must rebuild user provenance after blocking cleanup")
    assembly = (ROOT / "modules/kern/src/platform/sys/context_switch.asm").read_text()
    entry = assembly[assembly.index("wos_asm_enter_exec_owner:"):assembly.index("global wos_asm_enter_usermode")]
    if not (entry.index("cli") < entry.index("mov rsp, rdx") < entry.index("call wos_discard_abandoned_return_fpu_state") < entry.index("jmp wos_asm_enter_usermode")):
        fail("owner entry must reset the canonical kernel stack before userspace entry")


def require_spawn_open_child_context(source: str) -> None:
    body = function_body(source, "spawn_open_fd")
    assert "vfs::vfs_openat(task, vfs::AT_FDCWD, action.path, action.oflag" in body
    assert "vfs::vfs_open_file(" not in body
    assert body.index("vfs::fd_limit_locked(*task)") < body.index("spawn_close_fd(task") < body.index("vfs::vfs_openat(")
    assert "if (OPENED < 0) return OPENED" in body
    assert "task->get_fd_cloexec" in body and "task->set_fd_cloexec" in body
    assert "vfs::fd_usage_changed_locked(*task)" in body
    duplicate = function_body(source, "spawn_dup2_fd")
    assert "return -EBADF" in duplicate and "int result = -EBADF" in duplicate
    assert "vfs::fd_usage_changed_locked(*task)" in duplicate
    assert duplicate.index("unlock_irqrestore(IRQF)", duplicate.index("int result")) < duplicate.index("spawn_put_file(displaced)")
    apply = function_body(source, "apply_spawn_options")
    assert "*error_out = RESULT" in apply
    implementation = function_body(source, "wos_proc_exec_impl")
    assert implementation.index("initialize_credentials(*new_task, PARENT_CREDENTIALS)") < implementation.index("apply_spawn_options(new_task")
    assert implementation.index("parent_task->capture_filesystem_context(filesystem)") < implementation.index("new_task->cwd = filesystem.cwd") < implementation.index("apply_spawn_options(new_task")
    assert ".cwd = new_task->cwd.data()" in implementation
    assert ".cwd = parent_task->cwd.data()" not in implementation
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    opening = function_body(core, "vfs_openat")
    assert "open_at_for_task(task, dirfd, pathname, flags, mode, nullptr, true)" in opening
    implementation = function_body(core, "open_at_for_task")
    assert "consume_lookup_locked(lookup, false, consume_open, &context, task, &REQUEST_CREDENTIALS)" in implementation


def require_local_exec_content_views(source: str) -> None:
    for name in ("wos_proc_exec_impl", "wos_proc_execve_impl"):
        body = function_body(source, name)
        assert body.index("vfs_stabilize_exec_file(&exec_file)") < body.index("vfs_fstat_file(exec_file")
    native = function_body(source, "wos_proc_execve_impl")
    assert native.index("vfs_stabilize_exec_file(&interp_file)") < native.index("vfs_fstat_file(interp_file")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    stable = function_body(core, "vfs_stabilize_exec_file")
    assert "tmpfs_clone_exec_file" in stable and "xfs_clone_exec_file" in stable
    assert "MountOpenFileReservation reservation(SOURCE->mount_owner)" in stable
    assert stable.index("*owned = view.release()") < stable.index("vfs_put_file(SOURCE)")
    stat = function_body(core, "vfs_fstat_file")
    assert stat.index("exec_read_view->metadata") < stat.index("file_stat_snapshot_lookup")
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    boot = function_body(task, "read_boot_file_fully")
    assert boot.index("vfs_stabilize_exec_file(&file)") < boot.index("ker::vfs::Stat freshness")
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    for name in ("tmpfs_write_locked", "tmpfs_resize_locked"):
        assert "!n->exec_content.can_mutate()" in function_body(tmpfs, name)
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    for name in ("xfs_vfs_write_locked", "xfs_vfs_truncate_impl", "xfs_collect_swap_extents"):
        assert "!ip->exec_content.can_mutate()" in function_body(xfs, name)
    vm = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    for name in ("file_mmap_key_equal", "file_mmap_key_hash", "file_mmap_cached_page_for_file"):
        assert "exec_content_id" in function_body(vm, name)


def require_one_shot_routing_commit(source: str) -> None:
    native = function_body(source, "wos_proc_execve_impl")
    assert native.index("if (REMOTE != ker::net::wki::WkiRemoteSpawnResult::LOCAL)") < native.index("consume_successful_one_shot_wki_target(task, REMOTE)")
    compact = "".join(native.split())
    assert ".one_shot_wki_target=(task->wki_target_flags&sched::task::Task::WKI_TARGET_FLAG_ONESHOT)!=0" in compact
    prepared = source[source.index("class PreparedNativeExecImage {"):]
    assert ".one_shot_wki_target = image.one_shot_wki_target" in prepared
    commit = "".join(function_body(source, "commit_native_exec_image").split())
    assert commit.index("if(commit_published)") < commit.index("publish_exec_credentials(*task") < commit.index("if(PUBLISH_RESULT==0&&image.one_shot_wki_target)") < commit.index("pin_completed_exec_locally(*task)") < commit.index("path_metadata_lock.unlock_irqrestore(PATH_IRQF)")


def require_interpreter_execute_admission(source: str) -> None:
    native = function_body(source, "wos_proc_execve_impl")
    assert native.index("vfs_fstat_file(interp_file") < native.index("check_exec_permission_from_stat(request->admitted_credentials, interp_stat)") < native.index("new uint8_t[INTERP_SIZE]")
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    boot = function_body(task, "read_boot_file_fully")
    compact_boot = "".join(boot.split())
    assert compact_boot.index("vfs_fstat_file(file,&freshness)") < compact_boot.index("authorize_file_access(CREDENTIALS,") < compact_boot.index("boot_file_cache_lookup_copy")
    assert "*interpreter_error = ACCESS_RESULT" in boot
    complete = function_body(task, "complete_unpublished_process_construction")
    assert "&interp_size, interpreter_error" in complete
    spawn = function_body(source, "wos_proc_exec_impl")
    assert "complete_unpublished_process_construction(new_task, &interpreter_error)" in spawn
    assert "return static_cast<uint64_t>(interpreter_error)" in spawn
    remote = REMOTE_COMPUTE_CPP.read_text()
    for name in ("exec_elf_buffer", "exec_elf_file"):
        body = function_body(remote, name)
        assert "interpreter_error" in body and "TaskRejectReason::UNAUTHORIZED" in body


def require_exec_credential_admission(source: str) -> None:
    require_one_shot_routing_commit(source)
    require_interpreter_execute_admission(source)
    finish = function_body(source, "finish_native_exec_image")
    assert finish.index("prepare_native_exec_quiescence") < finish.index("commit_native_exec_image") < finish.index("retire_exec_siblings")
    commit = function_body(source, "commit_native_exec_image")
    assert commit.index("exec_admission.quiesced(*task)") < commit.index("publish_staged_process") < commit.index("publish_exec_credentials")
    handoff = function_body(source, "handoff_worker_exec")
    assert handoff.index("ExecSafeWait") < handoff.index("handoff.result.load") < handoff.index("wos_thread_exit_current")
    assert handoff.index("if (result != 0) safe_wait.wait_for_release()") < handoff.index("wos_thread_exit_current")
    wrapper = function_body(source, "wos_proc_execve")
    assert wrapper.index("pending->admitted_credentials = task->credentials()") < wrapper.index("snapshot_exec_arguments(*task")
    for name in ("wos_proc_exec", "wos_proc_spawn"):
        body = function_body(source, name)
        assert body.index("ADMITTED_CREDENTIALS = parent->credentials()") < body.index("snapshot_exec_arguments(*parent")
    for name in ("handoff_worker_exec", "commit_native_exec_image"):
        assert "&image.admitted_credentials" in function_body(source, name)
    assert ".admitted_credentials = image.admitted_credentials" in source
    assert ".admitted_credentials = request->admitted_credentials" in source
    assert "check_exec_permission_from_stat(request->admitted_credentials, exec_stat)" in source
    assert "check_exec_permission_from_stat(PARENT_CREDENTIALS, exec_stat)" in source
    for name in ("wos_proc_exec_impl", "wos_proc_execve_impl"):
        assert "vfs::vfs_open_file(" not in function_body(source, name)
    resource = (ROOT / "modules/kern/src/platform/sched/resource_process.cpp").read_text()
    prepare = function_body(resource, "prepare_exec_credentials")
    assert prepare.index("before != *admitted") < prepare.index("before.prepare_exec")


def require_exec_memory_policy_capture_and_commit(source: str) -> None:
    # Source ordering complements native tests: it verifies the value travels
    # with owned inputs rather than being resampled by a loader/worker.
    for name in ("wos_proc_exec", "wos_proc_spawn"):
        body = function_body(source, name)
        capture = "auto const MEMORY_POLICY = parent->memory_policy_snapshot();"
        assert body.count(capture) == 1
        assert body.index(capture) < body.index("snapshot_exec_arguments(*parent")
        assert "ADMITTED_CREDENTIALS, MEMORY_POLICY)" in body
    impl = function_body(source, "wos_proc_exec_impl")
    assert "->memory_policy_snapshot(" not in impl and ".memory_policy_snapshot(" not in impl
    assert "[spawn_options, &PARENT_CREDENTIALS, memory_policy]" in impl
    assert "wos_proc_exec_impl(interp, argv2, envp2, spawn_options, depth, PARENT_CREDENTIALS, memory_policy)" in " ".join(impl.split())
    assert impl.index("new_task->publish_memory_policy_snapshot(memory_policy);") < impl.index("new_task->initialize_process_image(")

    wrapper = function_body(source, "wos_proc_execve")
    capture = "pending->memory_policy = task->memory_policy_snapshot();"
    assert wrapper.count(capture) == 1
    assert wrapper.index(capture) < wrapper.index("adopt_exec_arguments(*task") < wrapper.index("snapshot_exec_arguments(*task")
    native = function_body(source, "wos_proc_execve_impl")
    assert "->memory_policy_snapshot(" not in native and ".memory_policy_snapshot(" not in native
    assert ".memory_policy = request->memory_policy" in native
    assert ".memory_policy = image.memory_policy" in source  # owned PreparedNativeExecImage copy

    preparation = function_body(source, "prepare_native_compartment_inputs_for_owner")
    assert preparation.count("pending->memory_policy = executor.memory_policy_snapshot();") == 1
    assert preparation.index("pending->memory_policy = executor.memory_policy_snapshot();") < preparation.index("adopt_exec_arguments(executor")
    assert "&error, request->memory_policy, &guard)" in preparation
    creation = function_body(source, "create_native_process_task_impl")
    assert creation.index("child->publish_memory_policy_snapshot(memory_policy);") < creation.index("child->initialize_process_image(")

    commit = function_body(source, "commit_native_exec_image")
    publication = "task->publish_memory_policy_snapshot(image.memory_policy);"
    assert commit.count(publication) == 1
    committed = commit.index("if (commit_published)")
    brace = commit.index("{", committed)
    end = find_matching_brace(commit, brace)
    assert publication in commit[brace:end]
    assert commit.index("publish_staged_process(") < committed < commit.index(publication)


def require_remote_exec_uses_canonical_owned_request(source: str) -> None:
    prepare = function_body(source, "prepare_remote_exec_owner")
    order = ["SharedVmemPublicationGuard", "owner.credentials() != plan.credentials", "plan.admission.begin(owner)",
             "plan.admission.quiesce(owner)", "wki_exec_signal_snapshot(*plan.actor)"]
    if [prepare.index(t) for t in order] != sorted(prepare.index(t) for t in order):
        fail("remote exec must revalidate/quiesce canonical authority before copying actor signal state")
    accepted = function_body(source, "accept_remote_exec_owner")
    if accepted.index("acknowledge_worker_exec") > accepted.index("retire_exec_siblings"):
        fail("accepted worker must be released before owner waits for its retirement")
    run = function_body(source, "run_remote_exec_owner")
    for token in ["CANDIDATE = plan.candidate()", ".exec_candidate = &CANDIDATE", ".prepare = prepare_remote_exec_owner", ".accepted = accept_remote_exec_owner",
                  ".deferred_proxy_id = &request.remote_proxy_id", "plan.handoff", "request.remote.reset()"]:
        if token not in run:
            fail("remote owner needs private candidate input and deferred scheduler identity")
    plan = source[source.index("struct RemoteExecPlan {"):source.index("struct WorkerExecHandoff {", source.index("struct RemoteExecPlan {"))]
    assert "void bind(" not in plan and "void restore(" not in plan
    assert "owner.elf_buffer =" not in plan
    assert "consume_successful_one_shot_wki_target" not in run
    cutover = function_body(source, "cutover_remote_exec_owner")
    assert cutover.index("publish_exec_credentials") < cutover.index("if (ERROR != 0) return ERROR") < cutover.index("plan.publish_metadata(owner)")
    wrapper = function_body(source, "wos_proc_execve")
    if wrapper.rindex("cleanup_exec_preparation_with_status(*task,") > wrapper.index("publish_remote_exec_proxy(*task"):
        fail("scheduler proxy identity cannot precede outer exec cleanup")
    service = function_body(source, "service_pending_exec_handoff")
    order = ["run_remote_exec_owner", "cleanup_exec_preparation_with_status(*owner,", "publish_remote_exec_proxy", "wos_asm_park_exec_owner"]
    if [service.index(t) for t in order] != sorted(service.index(t) for t in order):
        fail("canonical remote success must clean ownership and abandon the old owner continuation")
    remote = function_body(source, "try_process_remote_exec")
    for token in ["copy_arguments(argv, envp)", "std::memcpy(plan->bytes, elf, elf_size)", "ExecSafeWait safe_wait(actor)",
                  "handoff.remote = false", "handoff.result.store(-EINPROGRESS"]:
        if token not in remote:
            fail("remote handoff input and optional native fallback must keep distinct ownership")
    wki = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    placement = function_body(wki, "wki_try_remote_spawn")
    if placement.index("spec.prepare(*task") > placement.index("wki_ipc_export_task_fds"):
        fail("canonical quiescence must precede FD export")
    if placement.index("spec.accepted(*task") > placement.index("close_proxy_fd_table(task, true)"):
        fail("accepted old-worker retirement must precede CLOEXEC cleanup")
    if "!NEEDS_PROXY_PUBLICATION && spec.deferred_proxy_id == nullptr" not in placement:
        fail("current exec needs to defer scheduler identity until all ownership is restored")


def require_exec_auxv_matches_committed_snapshot() -> None:
    source = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    native = function_body(source, "commit_native_exec_image")
    assert native.index("after_credentials != image.stack_credentials") < native.index("publish_staged_process")
    assert ".stack_credentials = image.stack_credentials" in source
    assert ".stack_credentials = stack_credentials" in source
    assert "identity_auxv(EXEC_CREDENTIALS)" in source and "identity_auxv(stack_credentials)" in source
    remote = function_body(REMOTE_COMPUTE_CPP.read_text(), "handle_task_submit_work")
    assert remote.index("identity_auxv(CREDENTIAL_TRANSITION ? replacement_credentials : mapped_credentials)") < remote.index("publish_exec_credentials(*new_task")


def require_exec_trace_ownership(source: str) -> None:
    wrapper = function_body(source, "wos_proc_execve")
    order = ["adopt_exec_arguments(*task, pending)", "request->trace.begin(task, 0",
             "SNAPSHOT_STAGE", "snapshot_exec_arguments(*task", "SNAPSHOT_RESULT, 0, WOS_PERF_CALLSITE()",
             "wos_proc_execve_impl(snapshot.path.data()"]
    assert [wrapper.index(t) for t in order] == sorted(wrapper.index(t) for t in order)
    assert wrapper.index("prepared->trace = std::exchange(request->trace, {})") < wrapper.index("handoff_worker_exec(*task")
    assert "cleanup_exec_preparation_with_status(*task, static_cast<int32_t>(HANDOFF_RESULT))" in wrapper
    assert "cleanup_exec_preparation_with_status(*task, static_cast<int32_t>(RESULT))" in wrapper
    native = function_body(source, "wos_proc_execve_impl")
    assert "request->trace.begin(task, TRACE_DEPTH" in native
    assert "next_wki_trace_correlation" not in native and "time::get_us" not in native
    for op in ["ARG_COPY", "OPEN_ACCESS", "ELF_READ", "REMOTE_SPAWN", "NEW_IMAGE", "LOAD_ELF", "LOAD_INTERP", "STACK_SETUP", "COMMIT"]:
        assert f"request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::{op}" in native
    assert "WkiRemoteSpawnResult::FAILED ? remote_error" in " ".join(native.split())
    cleanup = function_body(source, "cleanup_exec_preparation_with_status")
    order = ["compare_exchange_strong", "std::exchange(prepared->trace, {})", "prepared->discard()", "delete prepared",
             "std::exchange(arguments->trace, {})", "delete arguments", "owned_exec_arguments.exchange",
             "image_trace.finish", "argument_trace.finish", "teardown_in_progress.store(false"]
    assert [cleanup.index(t) for t in order] == sorted(cleanup.index(t) for t in order)
    assert "cleanup_exec_preparation_with_status(task, -ECANCELED)" in function_body(source, "cleanup_exec_preparation")
    claim = function_body(source, "claim_pending_exec_handoff")
    order = ["resource_lock.lock_irqsave", "auto* holder = std::exchange(handoff->remote_request, nullptr)",
             "holder->trace = std::exchange(worker_request->trace, {})", "owned_exec_arguments.store(holder", "pending_exec_handoff.store(nullptr"]
    assert [claim.index(t) for t in order] == sorted(claim.index(t) for t in order)
    for forbidden in ["record_wki", "trace.finish", "get_us()", "next_wki_trace", "new "]:
        assert forbidden not in claim
    service = function_body(source, "service_pending_exec_handoff")
    order = ["run_remote_exec_owner", "worker_request->trace = std::exchange(request->trace, {})",
             "cleanup_exec_preparation_with_status(*owner", "acknowledge_worker_exec(*owner", "publish_remote_exec_proxy"]
    assert [service.index(t) for t in order] == sorted(service.index(t) for t in order)
    finish = function_body(source, "finish_native_exec_image")
    assert "COMMIT_STAGE, COMMIT_RESULT, 0" in finish
    assert "auto& COMMIT_STAGE = PREPARED->trace.pending.at(completion.trace_depth)" in finish
    assert "auto& DESTROY_OLD_STAGE = PREPARED->trace.pending.at(completion.trace_depth)" in finish
    assert finish.index("TRACE_SUBJECT_PID, perf::WkiPerfLocalProcOp::DESTROY_OLD") < finish.index("shm_cleanup_for_task(task)")
    remote = function_body(source, "run_remote_exec_owner")
    assert remote.index("WORKER_HANDOFF = handoff != nullptr") < remote.index("wki_try_remote_spawn(&owner")
    after = remote[remote.index("outcome.result = ker::net::wki::wki_try_remote_spawn"):]
    assert "handoff->" not in after and "request.trace.pending" in after
    trace = source[source.index("struct ExecTraceState {"):source.index("template <typename T, size_t N>")]
    assert trace.count("std::array<LocalProcStage, MAX_SHEBANG_DEPTH + 1>") == 2
    assert "depth != 0; --depth" in trace


def require_remote_exec_memory_policy_origin(source: str) -> None:
    run = function_body(source, "run_remote_exec_owner")
    assert ".memory_policy = request.memory_policy" in run
    for forbidden in ["owner.memory_policy_snapshot()", "plan.actor->memory_policy_snapshot()", "get_current_task()"]:
        assert forbidden not in run
    remote = function_body(source, "try_process_remote_exec")
    order = ["auto holder = std::unique_ptr<ExecRequestArguments>", "holder->memory_policy = request->memory_policy",
             "holder->remote = std::move(plan)", "owner->pending_exec_handoff.store"]
    assert [remote.index(t) for t in order] == sorted(remote.index(t) for t in order)
    copy = remote[remote.index(order[0]):remote.index(order[2])]
    assert "memory_policy_snapshot()" not in copy
    spawn = function_body(source, "wos_proc_exec_impl")
    spec = spawn[spawn.index("WkiRemoteSpawnSpec const REMOTE_SPAWN"):spawn.index("new_task->mark_exec_completed()")]
    assert ".memory_policy = memory_policy" in spec and "memory_policy_snapshot()" not in spec
    capture = function_body(source, "wos_proc_execve")
    assert capture.index("pending->memory_policy = task->memory_policy_snapshot()") < capture.index("adopt_exec_arguments(*task, pending)")
    one_shot = function_body(source, "consume_successful_one_shot_wki_target")
    assert one_shot.index("result == ker::net::wki::WkiRemoteSpawnResult::FAILED") < one_shot.index("pin_completed_exec_locally")


def require_remote_exec_memory_policy_mutants(source: str) -> None:
    require_remote_exec_memory_policy_origin(source)
    for before, after in [
        (".memory_policy = request.memory_policy", ".memory_policy = owner.memory_policy_snapshot()"),
        ("holder->memory_policy = request->memory_policy", "holder->memory_policy = actor.memory_policy_snapshot()"),
        ("holder->memory_policy = request->memory_policy;", ""),
        (".memory_policy = memory_policy,", ".memory_policy = parent_task->memory_policy_snapshot(),"),
    ]:
        assert before in source
        # Last token is specific to spawn; mutate its actual spec rather than
        # an earlier owned-image aggregate with the same member name.
        if before == ".memory_policy = memory_policy,":
            start = source.index("WkiRemoteSpawnSpec const REMOTE_SPAWN")
            mutant = source[:start] + source[start:].replace(before, after, 1)
        else:
            mutant = source.replace(before, after, 1)
        try:
            require_remote_exec_memory_policy_origin(mutant)
        except (AssertionError, ValueError):
            continue
        fail(f"remote policy source oracle accepted mutant: {before}")


def main() -> None:
    require_exec_trace_ownership(EXEC_CPP.read_text())
    require_exec_auxv_matches_committed_snapshot()
    source = EXEC_CPP.read_text()
    require_remote_exec_memory_policy_mutants(source)
    loader_source = ELF_LOADER_CPP.read_text()
    require_spawn_open_child_context(source)
    require_exec_credential_admission(source)
    require_exec_memory_policy_capture_and_commit(source)
    require_local_exec_content_views(source)
    require_sparse_exec_reads(source)
    require_callers_use_sparse_reader(source)
    require_large_dynamic_elf_is_file_backed(source)
    require_stdio_fallback_access_modes(source)
    require_spawn_actions_snapshot_user_memory_and_preserve_cloexec_sources(source)
    require_loader_does_not_scan_unread_symbol_payloads(loader_source)
    require_interpreter_preserves_primary_debug_mappings(loader_source)
    require_bounded_contiguous_loader(loader_source)
    require_remote_execfn_auxv()
    require_loader_lazy_file_ranges(loader_source)
    require_coredump_entropy_redaction()
    require_rtld_self_relocation_is_single_pass()
    require_main_exec_retirement_order(source)
    require_tracked_exec_cleanup(source)
    require_native_exec_borrows_kernel_argument_snapshot(source)
    require_request_arguments_are_published_before_usercopy(source)
    require_native_loader_scratch_is_request_owned(source)
    require_preparation_unwinds_before_completion(source)
    require_worker_handoff_ownership_and_exit_order(source)
    require_remote_exec_uses_canonical_owned_request(source)
    print("exec sparse-read source invariants hold")


if __name__ == "__main__":
    main()
