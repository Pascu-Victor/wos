#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
ELF_LOADER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "loader" / "elf_loader.cpp"


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
        "read_sparse_elf_image(fd, dst, file_size, path, bytes_read, read_static_relocation_metadata, allow_lazy_file_segments)",
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
    if "read_exec_image_for_loader(INTERP_FD, interp_buf, static_cast<size_t>(INTERP_SIZE), INTERP_PATH, false, interp_file != nullptr)" not in source:
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
        ".lazy_file_ranges = exec_file != nullptr ? &main_loader_lazy_ranges : nullptr",
        "append_exec_lazy_file_ranges(new_lazy_ranges, main_loader_lazy_ranges, exec_file, exec_stat)",
        "vfs::File* interp_file = vfs::vfs_get_file_retain(task, INTERP_FD)",
        "append_exec_lazy_file_ranges(new_lazy_ranges, interp_loader_lazy_ranges, interp_file, interp_stat)",
        "publish_exec_lazy_ranges(task, new_lazy_ranges)",
        "new_lazy_ranges_published = true",
    ]:
        if snippet not in source:
            fail(f"execve lazy ELF range lifetime invariant missing: {snippet}")


def require_loader_does_not_scan_unread_symbol_payloads(loader_source: str) -> None:
    body = function_body_containing(loader_source, "load_elf", "(void)REGISTER_SPECIAL_SYMBOLS;")
    if "(void)REGISTER_SPECIAL_SYMBOLS;" not in body:
        fail("loader must not scan symbol/string table payloads after sparse exec reads")
    marker = body.find("(void)REGISTER_SPECIAL_SYMBOLS;")
    if marker < 0:
        fail("register_special_symbols marker missing")
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

    load_body = function_body_containing(loader_source, "load_elf", "ENABLE_LAZY_FILE_RANGES")
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
    require_loader_does_not_scan_unread_symbol_payloads(loader_source)
    require_loader_lazy_file_ranges(loader_source)
    print("exec sparse-read source invariants hold")


if __name__ == "__main__":
    main()
