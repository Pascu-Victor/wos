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


def require_sparse_exec_reads(source: str) -> None:
    for snippet in [
        "constexpr size_t EXEC_SPARSE_ELF_MIN_SIZE = static_cast<size_t>(64) * 1024",
        "constexpr size_t EXEC_SHEBANG_PROBE_SIZE = 4096",
        "struct ExecImageReadResult",
        "read_exec_image_for_loader",
    ]:
        if snippet not in source:
            fail(f"exec sparse-read support missing: {snippet}")

    sparse_body = function_body(source, "read_sparse_elf_image")
    section_body = function_body(source, "read_exec_section_if_needed")
    for snippet in [
        "ph.p_type != PT_LOAD && ph.p_type != PT_NOTE && ph.p_type != PT_INTERP",
        "read_file_range_fully(fd, dst, file_size, ph.p_offset, ph.p_filesz",
    ]:
        if snippet not in sparse_body:
            fail(f"sparse ELF reader must preserve loader-touched range: {snippet}")

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
        "read_sparse_elf_image(fd, dst, file_size, path, bytes_read, read_static_relocation_metadata)",
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
    if "read_exec_image_for_loader(INTERP_FD, interp_buf, static_cast<size_t>(INTERP_SIZE), INTERP_PATH, false)" not in source:
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


def require_loader_does_not_scan_unread_symbol_payloads(loader_source: str) -> None:
    body = function_body(loader_source, "load_elf")
    if "(void)register_special_symbols;" not in body:
        fail("loader must not scan symbol/string table payloads after sparse exec reads")
    marker = body.find("(void)register_special_symbols;")
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


def main() -> None:
    source = EXEC_CPP.read_text()
    loader_source = ELF_LOADER_CPP.read_text()
    require_sparse_exec_reads(source)
    require_callers_use_sparse_reader(source)
    require_loader_does_not_scan_unread_symbol_payloads(loader_source)
    print("exec sparse-read source invariants hold")


if __name__ == "__main__":
    main()
