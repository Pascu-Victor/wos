#!/usr/bin/env python3

import re
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL = ROOT / "modules" / "kern" / "src"
SYSCALL_IMPL = KERNEL / "syscalls_impl"
EXEC_CPP = SYSCALL_IMPL / "process" / "exec.cpp"
VMEM_CPP = SYSCALL_IMPL / "vmem" / "sys_vmem.cpp"
TASK_CPP = KERNEL / "platform" / "sched" / "task.cpp"
SMT_CPP = KERNEL / "platform" / "smt" / "smt.cpp"
VFS_CORE_CPP = KERNEL / "vfs" / "core.cpp"
SYS_VFS_CPP = SYSCALL_IMPL / "vfs" / "sys_vfs.cpp"
MM_KTEST_CPP = KERNEL / "test" / "mm_ktest.cpp"

REGISTER_POINTER_CAST = re.compile(r"reinterpret_cast<[^>]+>\((?:a[1-6]|arg[1-6])\)")

# These casts are address tokens only. Their consumers are locked below to
# syscall-layer usercopy helpers or to the four VFS byte-I/O bounce entrypoints.
EXPECTED_REGISTER_POINTER_CASTS = {
    "modules/kern/src/syscalls_impl/process/process.cpp": Counter(
        {
            "reinterpret_cast<int32_t*>(a3)": 1,
            "reinterpret_cast<KernelUtsname*>(a2)": 1,
            "reinterpret_cast<const KernelStackT*>(a2)": 1,
            "reinterpret_cast<KernelStackT*>(a3)": 1,
        }
    ),
    "modules/kern/src/syscalls_impl/vfs/sys_vfs.cpp": Counter(
        {
            "reinterpret_cast<void*>(a2)": 2,
            "reinterpret_cast<const void*>(a2)": 2,
            "reinterpret_cast<size_t*>(a4)": 2,
            "reinterpret_cast<off_t*>(a4)": 1,
            "reinterpret_cast<off_t*>(a3)": 1,
            "reinterpret_cast<ker::vfs::Stat*>(a2)": 4,
            "reinterpret_cast<int*>(a3)": 1,
            "reinterpret_cast<const ker::abi::vfs::metadata_batch_header*>(a1)": 1,
            "reinterpret_cast<const MetadataBatchUserEntry*>(a2)": 1,
            "reinterpret_cast<MetadataBatchUserResult*>(a3)": 1,
            "reinterpret_cast<ker::vfs::Stat*>(a3)": 1,
            "reinterpret_cast<char*>(a1)": 1,
            "reinterpret_cast<ker::vfs::EpollEvent*>(a4)": 1,
            "reinterpret_cast<char*>(a2)": 1,
            "reinterpret_cast<uint32_t*>(a4)": 1,
            "reinterpret_cast<ker::vfs::Statvfs*>(a2)": 2,
        }
    ),
}


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


def function_body(source: str, spelling: str) -> str:
    start = source.find(spelling)
    if start < 0:
        fail(f"missing function {spelling}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"missing body for {spelling}")
    return source[brace + 1 : find_matching_brace(source, brace)]


def code_without_comments_or_literals(source: str) -> str:
    pattern = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', flags=re.DOTALL)
    return pattern.sub(lambda match: "\n" * match.group(0).count("\n"), source)


def require_code(source: str, tokens: list[str], description: str) -> None:
    code = re.sub(r"\s+", "", code_without_comments_or_literals(source))
    for token in tokens:
        if re.sub(r"\s+", "", token) not in code:
            fail(f"{description} lost {token}")


def require_order(source: str, tokens: list[str], description: str) -> None:
    code = re.sub(r"\s+", "", code_without_comments_or_literals(source))
    position = 0
    for token in tokens:
        found = code.find(re.sub(r"\s+", "", token), position)
        if found < 0:
            fail(f"{description} lost ordering before {token}")
        position = found + len(re.sub(r"\s+", "", token))


def test_register_to_pointer_conversions_are_closed() -> None:
    boundary_files = sorted(SYSCALL_IMPL.rglob("*.cpp")) + [
        KERNEL / "platform" / "sys" / "syscall.cpp",
        KERNEL / "platform" / "sys" / "signal.cpp",
        KERNEL / "platform" / "debug" / "ptrace.cpp",
    ]
    actual: dict[str, Counter[str]] = {}
    for path in boundary_files:
        casts = Counter(REGISTER_POINTER_CAST.findall(path.read_text()))
        if casts:
            actual[str(path.relative_to(ROOT))] = casts

    if actual != EXPECTED_REGISTER_POINTER_CASTS:
        fail(f"syscall register-to-pointer audit drifted:\nexpected={EXPECTED_REGISTER_POINTER_CASTS}\nactual={actual}")

    for path in boundary_files:
        source = code_without_comments_or_literals(path.read_text())
        relative = path.relative_to(ROOT)
        forbidden = [
            re.compile(r"\([^()]*\*\)\s*(?:a[1-6]|arg[1-6])\b"),
            re.compile(r"\*\s*(?:a[1-6]|arg[1-6])\b"),
            re.compile(r"(?:a[1-6]|arg[1-6])\s*(?:\[|->)"),
        ]
        for pattern in forbidden:
            if pattern.search(source):
                fail(f"{relative} directly casts/dereferences a syscall register with {pattern.pattern}")

    # GET and GET_DEFAULT share one address-token call, while retaining distinct
    # backends, subject admission and bounded usercopy for both optional outputs.
    vfs = SYS_VFS_CPP.read_text()
    rules = function_body(vfs, "auto wki_rules_for_subject(")
    require_order(rules, ["remote_namespace_admission(subject)", "if (SCOPE != 0) return SCOPE;", "switch (OP)"], "WKI subject admission")
    require_code(rules, ["case ops::WKI_RULE_GET: case ops::WKI_RULE_GET_DEFAULT:",
                        "copy_wki_rule_to_user(subject, static_cast<uint32_t>(a1), reinterpret_cast<char*>(a2), static_cast<size_t>(a3), reinterpret_cast<uint32_t*>(a4), OP == ops::WKI_RULE_GET_DEFAULT)"], "shared WKI GET dispatch")
    require_code(function_body(vfs, "auto sys_vfs("), ["case ops::WKI_RULE_ADD: case ops::WKI_RULE_GET: case ops::WKI_RULE_GET_DEFAULT: case ops::WKI_RULE_CLEAR:",
                 "return wki_rules_for_subject(ker::mod::sched::get_current_task(), op_raw, a1, a2, a3, a4);"], "WKI syscall delegate")
    copy = function_body(vfs, "auto copy_wki_rule_to_user(")
    require_code(copy, ["std::array<char, ker::mod::sched::task::Task::CWD_MAX> kernel_prefix{};",
                       "usercopy::ensure_writable(*subject, address, size)",
                       "preflight(reinterpret_cast<uint64_t>(prefix_buf), std::min(prefix_buf_size, kernel_prefix.size()))",
                       "preflight(reinterpret_cast<uint64_t>(route_out), sizeof(uint32_t))",
                       "char* prefix_arg = prefix_buf != nullptr ? kernel_prefix.data() : nullptr;",
                       "uint32_t* route_arg = route_out != nullptr ? &route : nullptr;",
                       "default_rules ? ker::vfs::vfs_wki_default_rule_get_for_task(subject, index, prefix_arg, PREFIX_ARG_SIZE, route_arg) : ker::vfs::vfs_wki_rule_get_for_task(subject, index, prefix_arg, PREFIX_ARG_SIZE, route_arg)",
                       "static_cast<size_t>(RET) >= PREFIX_ARG_SIZE || static_cast<size_t>(RET) >= kernel_prefix.size()",
                       "size_t const COPY_SIZE = static_cast<size_t>(RET) + 1;",
                       "copy_value_to_user_for_task(subject, route_out, route)"], "WKI bounded output consumer")
    require_order(copy, ["preflight(reinterpret_cast<uint64_t>(route_out), sizeof(uint32_t))", "int const RET = default_rules", "return -EOVERFLOW;",
                        "usercopy::copy_to_task(*subject, reinterpret_cast<uint64_t>(prefix_buf), kernel_prefix.data(), COPY_SIZE)"], "WKI preflight/backend/capacity/copy ordering")
    require_code(function_body(vfs, "auto copy_value_to_user_for_task("), ["usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(user_ptr), value)"], "WKI route copyout")


def test_raw_translation_exceptions_are_only_unpublished_images() -> None:
    translation_files: dict[str, str] = {}
    for path in sorted(SYSCALL_IMPL.rglob("*.cpp")):
        source = path.read_text()
        if re.search(r"\b(?:translate|get_virt_pointer)\s*\(", source):
            translation_files[str(path.relative_to(ROOT))] = source

    expected_files = {str(EXEC_CPP.relative_to(ROOT)), str(VMEM_CPP.relative_to(ROOT))}
    if set(translation_files) != expected_files:
        fail(f"raw VM access appeared at a syscall boundary: {sorted(set(translation_files) - expected_files)}")

    exec_source = translation_files[str(EXEC_CPP.relative_to(ROOT))]
    expected_exec_calls = {
        "translate(new_task->pagemap, PAGE_VIRT)": 1,
        "translate(new_pagemap, PAGE_VIRT)": 1,
        "translate(new_pagemap, new_thread->fsbase)": 1,
        "translate(new_pagemap, DEST_VADDR)": 1,
        "get_virt_pointer(PAGE_PHYS)": 2,
        "get_virt_pointer(TCB_PADDR)": 1,
        "get_virt_pointer(DEST_PADDR)": 1,
        "translate(child.pagemap, VIRTUAL)": 1,
        "get_virt_pointer(PHYSICAL)": 1,
    }
    for call, count in expected_exec_calls.items():
        if exec_source.count(call) != count:
            fail(f"exec raw unpublished-image exception drifted: {call} count={exec_source.count(call)}, expected={count}")
    if exec_source.count("::translate(") != 5 or exec_source.count("::get_virt_pointer(") != 5:
        fail("exec gained a raw VM access outside its enumerated unpublished-image initialization")

    stack = function_body(exec_source, "auto prepare_native_stack(")
    if stack.count("::translate(") != 1 or stack.count("::get_virt_pointer(") != 1 or exec_source.count("prepare_native_stack(") != 2:
        fail("native stack raw access must remain one bounded pair with one owned builder caller")
    require_order(stack, ["offset > CAPACITY || bytes > CAPACITY - offset", "uint64_t const ADDRESS = TOP - offset - bytes;",
                         "ensure_stack_backing(thread, child.pagemap, ADDRESS, ADDRESS + bytes, &child, allocation_context)",
                         "translate(child.pagemap, VIRTUAL)", "PHYSICAL == mod::mm::virt::PADDR_INVALID",
                         "std::min(bytes - copied, mod::mm::paging::PAGE_SIZE - (VIRTUAL % mod::mm::paging::PAGE_SIZE))",
                         "std::memcpy(mod::mm::addr::get_virt_pointer(PHYSICAL), source + copied, CHUNK)"], "native stack capacity/backing/page copy")
    require_code(stack, ["push(snapshot.argv[i], std::strlen(snapshot.argv[i]) + 1)", "push(snapshot.envp[i], std::strlen(snapshot.envp[i]) + 1)",
                        "push(snapshot.path.data(), std::strlen(snapshot.path.data()) + 1)", "push(random.data(), random.size())",
                        "push(auxv.data(), words * sizeof(uint64_t))", "push(scratch.envp_addrs, (scratch.envp_count + 1) * sizeof(uint64_t))",
                        "push(scratch.argv_addrs, (scratch.argv_count + 1) * sizeof(uint64_t))", "push(&ARGC, sizeof(ARGC))"], "kernel-owned native stack inputs")
    builder = function_body(exec_source, "auto create_native_process_task_impl(")
    require_order(builder, ["auto* child = new (std::nothrow) mod::sched::task::Task{};", "claim_unpublished_process(owner, child)",
                           "adopt_exec_arguments(*child, pending) ? capture_native_arguments(context.arguments, *request, *scratch)",
                           "child->initialize_process_image", "guarded([&] { return prepare_native_stack(*child, request->snapshot, *scratch, request->allocation_context); })"], "unpublished native stack ownership")
    require_code(builder, ["NativeConstructionExecutorGuard guard(*owner);", "if (!guard.acquired()) return -EBUSY;", "return operation();"], "native stack construction guard")
    if "post_task_for_cpu(" in builder:
        fail("native stack builder must not publish its private child")
    publication = function_body(exec_source, "auto publish_native_compartment_for_owner(")
    require_order(publication, ["executor.owned_unpublished_process.load(std::memory_order_acquire) != child",
                               "child->scheduler_published.load(std::memory_order_acquire)", "mod::sched::post_task_for_cpu(cpu, child)"], "separate checked native publication")

    vmem_source = translation_files[str(VMEM_CPP.relative_to(ROOT))]
    table_from_entry = function_body(vmem_source, "auto table_from_entry(")
    # Three named private-root native fixtures and the bounded vacancy observation
    # of the explicitly unpublished producer are the only additional sites.
    start = vmem_source.find("auto selftest_file_mapping_admission(")
    if start < 0 or vmem_source.rfind("#ifdef WOS_SELFTEST", 0, start) <= vmem_source.rfind("#endif", 0, start):
        fail("VMEM named fixture must remain inside WOS_SELFTEST")
    brace = vmem_source.find("{", start)
    end = find_matching_brace(vmem_source, brace) + 1
    fixture = vmem_source[brace + 1 : end - 1]
    remaining = vmem_source[:start] + vmem_source[end:]
    projection_start = remaining.find("auto selftest_lazy_file_projection(")
    if (projection_start < 0 or remaining.rfind("#ifdef WOS_SELFTEST", 0, projection_start)
            <= remaining.rfind("#endif", 0, projection_start)):
        fail("VMEM projection fixture must remain inside WOS_SELFTEST")
    projection_brace = remaining.find("{", projection_start)
    projection_end = find_matching_brace(remaining, projection_brace) + 1
    projection = remaining[projection_brace + 1 : projection_end - 1]
    remaining = remaining[:projection_start] + remaining[projection_end:]
    zero_start = remaining.find("auto selftest_map_anonymous_zero_page(")
    if (zero_start < 0 or remaining.rfind("#ifdef WOS_SELFTEST", 0, zero_start)
            <= remaining.rfind("#endif", 0, zero_start)):
        fail("VMEM genuine-zero fixture must remain inside WOS_SELFTEST")
    zero_brace = remaining.find("{", zero_start)
    zero_end = find_matching_brace(remaining, zero_brace) + 1
    zero = remaining[zero_brace + 1 : zero_end - 1]
    remaining = remaining[:zero_start] + remaining[zero_end:]
    if (zero.count("mm::virt::translate(ROOT, address)") != 1
            or zero.count("translate(") != 1 or "get_virt_pointer(" in zero):
        fail("VMEM genuine-zero fixture permits one exact vacancy check and no raw data access")
    require_order(zero, ["auto* const ROOT = subject.pagemap;", "if (ROOT == nullptr",
                        "mm::virt::translate(ROOT, address) != mm::virt::PADDR_INVALID)",
                        "return false;", "return materialize_reserved_page(&subject, address,"],
                  "VMEM genuine-zero vacancy precedes production materialization")
    require_code(zero, ["ROOT == mm::virt::get_kernel_pagemap()", "address < USER_SPACE_START",
                       "address >= USER_SPACE_END", "address % mm::paging::PAGE_SIZE != 0",
                       "subject.type != task::TaskType::PROCESS", "subject.pid != 0", "subject.is_thread",
                       "subject.thread != nullptr", "subject.state.load(std::memory_order_acquire) != task::TaskState::ACTIVE",
                       "subject.scheduler_published.load(std::memory_order_acquire)",
                       "subject.shares_user_pagemap.load(std::memory_order_acquire)",
                       "ker::mod::sched::can_query_current_task() && ker::mod::sched::get_current_task() == &subject",
                       "mm::virt::active_pagemap_is(ROOT)",
                       "ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE"],
                 "VMEM genuine-zero fixed private inactive Task and aligned user geometry")
    for forbidden in ["map_page(", "get_anon_zero_page(", "get_virt_pointer(", "memcpy(", "page_ref_inc("]:
        if forbidden in zero:
            fail(f"VMEM genuine-zero fixture must delegate real singleton ownership: {forbidden}")
    reserved_start = remaining.find("auto selftest_materialize_anonymous_reserved(")
    if (reserved_start < 0 or remaining.rfind("#ifdef WOS_SELFTEST", 0, reserved_start)
            <= remaining.rfind("#endif", 0, reserved_start)):
        fail("VMEM reserved-page fixture must remain inside WOS_SELFTEST")
    reserved_brace = remaining.find("{", reserved_start)
    reserved_end = find_matching_brace(remaining, reserved_brace) + 1
    reserved = remaining[reserved_brace + 1 : reserved_end - 1]
    remaining = remaining[:reserved_start] + remaining[reserved_end:]
    if (reserved.count("mm::virt::translate(subject.pagemap, address)") != 1
            or reserved.count("translate(") != 1 or "get_virt_pointer(" in reserved):
        fail("VMEM reserved fixture permits one exact vacancy check and no raw data access")
    require_order(reserved, ["selftest_owned_anonymous_target(subject, address, mm::paging::PAGE_SIZE)",
                            "mm::virt::translate(subject.pagemap, address) != mm::virt::PADDR_INVALID)",
                            "return false;", "subject.memory_policy_snapshot()",
                            "numa::capture_allocation_context(MEMORY_POLICY, placement)",
                            "SharedVmemPublicationGuard publication;",
                            "return materialize_reserved_page(&subject, address, prot, placement, fail_private_allocation);"],
                  "VMEM reserved vacancy and captured policy precede real private materialization")
    target = function_body(vmem_source, "auto selftest_owned_anonymous_target(")
    require_code(target, ["ROOT != nullptr", "ROOT != mm::virt::get_kernel_pagemap()", "address >= USER_SPACE_START",
                          "length != 0", "length <= 16 * mm::paging::PAGE_SIZE", "address <= USER_SPACE_END - length",
                          "(address | length) % mm::paging::PAGE_SIZE == 0", "subject.type == task::TaskType::PROCESS",
                          "subject.pid == 0", "!subject.is_thread", "subject.thread == nullptr",
                          "subject.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE",
                          "!subject.scheduler_published.load(std::memory_order_acquire)",
                          "!subject.shares_user_pagemap.load(std::memory_order_acquire)",
                          "subject.native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr",
                          "subject.usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0",
                          "!ker::mod::sched::can_query_current_task() || ker::mod::sched::get_current_task() != &subject",
                          "!mm::virt::active_pagemap_is(ROOT)", "mm::virt::selftest_user_pagemap_reader_count(ROOT) == 0"],
                 "VMEM reserved fixed owned inactive target and bounded aligned geometry")
    for forbidden in ["map_page(", "get_virt_pointer(", "memcpy(", "page_ref_inc(", "create_page_table_entry("]:
        if forbidden in reserved or forbidden in target:
            fail(f"VMEM reserved fixture must delegate backing ownership: {forbidden}")
    producer_start = remaining.find("auto prepare_unpublished_file_mapping_impl(")
    if producer_start < 0:
        fail("VMEM unpublished mapping producer must remain explicitly named")
    producer_brace = remaining.find("{", producer_start)
    producer_end = find_matching_brace(remaining, producer_brace) + 1
    producer = remaining[producer_brace + 1 : producer_end - 1]
    remaining = remaining[:producer_start] + remaining[producer_end:]
    for name, mode in (("prepare_unpublished_file_mapping", "false"),
                       ("prepare_unpublished_read_shared_tmpfs_mapping", "true")):
        wrapper = function_body(remaining, f"auto {name}(")
        expected = f"return prepare_unpublished_file_mapping_impl(owner, child, file, address, length, offset, prot, {mode});"
        if wrapper.strip() != expected:
            fail(f"VMEM {name} must only select its fixed native backing mode")
    if (fixture.count("virt::translate(pagemap, BASE)") != 2 or fixture.count("get_virt_pointer(PADDR)") != 1
            or fixture.count("translate(") != 2 or fixture.count("get_virt_pointer(") != 1
            or vmem_source.count("translate(") != 7 or vmem_source.count("get_virt_pointer(") != 3):
        fail("VMEM named fixture raw access must remain its exact marker/content checks")
    if (producer.count("ker::mod::mm::virt::translate(child.pagemap, page)") != 1
            or producer.count("translate(") != 1 or "get_virt_pointer(" in producer):
        fail("VMEM unpublished producer permits one exact translation site and no raw data access")
    require_code(producer, ["constexpr uint64_t MAX_NATIVE_PAGES = 64;", "get_current_task() != &owner",
                           "owner.owned_unpublished_process.load(std::memory_order_acquire) != &child",
                           "child.thread != nullptr", "child.is_thread", "child.shares_user_pagemap.load()",
                           "child.scheduler_published.load(std::memory_order_acquire)",
                           "child.gc_queued.load(std::memory_order_acquire)",
                           "length > USER_SPACE_END + 1 - address", "(address % PAGE) != 0", "(length % PAGE) != 0",
                           "if (length / PAGE > MAX_NATIVE_PAGES) return -E2BIG;"],
                 "VMEM raw vacancy observation fixed private Task and bounded geometry")
    vacancy_start = producer.find("auto range_vacant_locked = [&]")
    vacancy_brace = producer.find("{", vacancy_start)
    if vacancy_start < 0 or vacancy_brace < 0 or producer.count("range_vacant_locked()") != 2:
        fail("VMEM vacancy lambda must retain exactly its initial and publication-locked callers")
    vacancy = producer[vacancy_brace + 1 : find_matching_brace(producer, vacancy_brace)]
    require_order(vacancy, ["child.pagemap != nullptr && child.lazy_vmem_ranges.size() < 8",
                            "for (uint64_t page = address; vacant && page < address + length; page += PAGE)",
                            "ker::mod::mm::virt::translate(child.pagemap, page) == ker::mod::mm::virt::PADDR_INVALID;"],
                  "VMEM at-most64-page vacancy-only translation")
    for forbidden in ["map_page(", "get_virt_pointer(", "memcpy(", "create_page_table_entry("]:
        if forbidden in producer:
            fail(f"VMEM unpublished producer cannot write a raw File leaf or data frame: {forbidden}")
    require_order(producer, ["uint64_t const VACANCY_FLAGS = child.lazy_vmem_lock.lock_irqsave();",
                            "bool const VACANT = range_vacant_locked();", "child.lazy_vmem_lock.unlock_irqrestore(VACANCY_FLAGS);",
                            "prepare_unpublished_file_mapping_tables(child.pagemap, address, length, child.pid)",
                            "SharedVmemPublicationGuard publication;", "child.lazy_vmem_lock.lock_irqsave()",
                            "if (range_vacant_locked())"], "VMEM private vacancy checks and prepared-table publication order")
    if (projection.count("mm::virt::translate(pagemap, address)") != 1
            or projection.count("mm::virt::translate(pagemap, BASE)") != 1
            or projection.count("mm::addr::get_virt_pointer(PHYSICAL)") != 1
            or projection.count("translate(") != 2 or projection.count("get_virt_pointer(") != 1):
        fail("VMEM projection fixture raw access must remain exact cold-byte/no-effect checks")
    if "get_virt_pointer" not in table_from_entry or remaining.count("get_virt_pointer(") != 1 or "translate(" in remaining:
        fail("VMEM raw access must remain limited to walking kernel page-table frames")
    require_code(fixture, ["constexpr uint64_t BASE = USER_SPACE_START;", "constexpr uint64_t SIZE = paging::PAGE_SIZE;", "task.pagemap = pagemap;",
                          "phys::page_alloc(ker::mod::mm::PhysicalPageOwner::SELFTEST, SIZE,", "virt::translate(pagemap, BASE) == MARKER_PHYS",
                          "static_cast<uint8_t*>(marker)[0] == 0x5a", "state.reads == 0 && state.closes == 0", "data[0] == 0x41",
                          "remove_lazy_vmem_range(&task, BASE, SIZE);", "unregister_file_mmap_range(pagemap, BASE, SIZE);", "rollback_mapped_pages(&task, BASE, 1);",
                          "vfs::vfs_release_fd(&task, FD);", "vfs::vfs_put_file(file);"], "private VMEM fixture identity/effects/cleanup")
    require_order(fixture, ["auto const PADDR = virt::translate(pagemap, BASE);", "if (PADDR != virt::PADDR_INVALID)", "get_virt_pointer(PADDR)"], "VMEM content read after valid translation")
    require_code(projection, ["constexpr uint64_t BASE = 0x400000;", "constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;",
                             "constexpr uint64_t SIZE = 6 * PAGE;", "task.pagemap = pagemap;",
                             "file->fs_type = vfs::FSType::SOCKET;", "remove_lazy_vmem_range(&task, BASE, SIZE)",
                             "rollback_mapped_pages(&task, BASE, SIZE / PAGE);", "selftest_evict_file_mmap_cache_exact(*page.file, page.offset, page.frame)",
                             "mm::phys::page_ref_dec(page.frame);", "vfs::vfs_put_file(file);", "vfs::vfs_put_file(replacement);",
                             "mm::virt::translate(pagemap, BASE) == mm::virt::PADDR_INVALID && state.reads == 0 && state.closes == 0",
                             "unregister_file_mmap_range(pagemap, BASE, PAGE)", "rollback_mapped_pages(&task, BASE, 1);",
                             "vfs::vfs_release_fd(&task, FD);"], "private VMEM projection fixture ownership/no-effects/cleanup")
    guard_start = projection.find("SharedVmemPublicationGuard guard;")
    guard_brace = projection.rfind("{", 0, guard_start)
    if guard_start < 0 or guard_brace < 0 or projection[guard_brace + 1 : guard_start].strip():
        fail("VMEM projection byte checks must keep their explicit publication guard")
    guarded = projection[guard_brace + 1 : find_matching_brace(projection, guard_brace)]
    if guarded.count("translate(") != 1 or guarded.count("get_virt_pointer(") != 1:
        fail("VMEM projection content translation/dereference must stay inside publication guard")
    require_order(guarded, ["for (uint64_t address = BASE; ok && address < BASE + SIZE; address += PAGE)",
                           "auto const PHYSICAL = mm::virt::translate(pagemap, address);",
                           "if (PHYSICAL == mm::virt::PADDR_INVALID)", "break;", "mm::addr::get_virt_pointer(PHYSICAL)",
                           "for (size_t i = 0; i < PAGE; ++i)", "BYTE(SEED, EXPECTED_OFFSET + i) == bytes[i]",
                           "rollback_mapped_pages(&task, BASE, SIZE / PAGE);"], "VMEM bounded guarded projection byte access")
    native = MM_KTEST_CPP.read_text()
    if native.count("selftest_file_mapping_admission(") != 1:
        fail("VMEM fixture must retain exactly one private native caller")
    caller = function_body(native, "KTEST(MM, FileMappingAdmissionPreservesDenialsAndRetainsOneDescription)")
    require_order(caller, ["TestUserTree tree;", "KREQUIRE_TRUE(init_test_user_tree(tree));", "selftest_file_mapping_admission(tree.root)", "cleanup_test_user_tree(tree);"], "VMEM private root caller")
    require_code(function_body(native, "auto init_test_user_tree("), ["tree.root = alloc_test_page_table();", "cleanup_test_user_tree(tree);"], "VMEM owned test root")
    require_code(function_body(native, "auto alloc_test_page_table("), ["alloc_test_pages()", "phys::page_mark_kind(table, mm::PageKind::PAGE_TABLE)"], "VMEM test page-table allocation")
    require_code(function_body(native, "auto alloc_test_pages("), ["phys::page_alloc(mm::PhysicalPageOwner::SELFTEST, size,"], "VMEM test physical ownership")
    if native.count("selftest_lazy_file_projection(") != 1:
        fail("VMEM projection fixture must retain exactly one private native caller")
    projection_caller = function_body(native, "KTEST(MM, LazyFileSplitsPreserveColdBytesAndRejectOffsetOverflow)")
    require_order(projection_caller, ["TestUserTree tree;", "KREQUIRE_TRUE(init_test_user_tree(tree));",
                                    "selftest_lazy_file_projection(tree.root)", "cleanup_test_user_tree(tree);"], "VMEM projection owned private root caller")


def test_transitive_address_consumers_do_not_raw_access_userspace() -> None:
    smt_set_tcb = function_body(SMT_CPP.read_text(), "auto set_tcb(")
    for forbidden in ["reinterpret_cast", "static_cast<uint64_t*>", "get_virt_pointer", "translate("]:
        if forbidden in smt_set_tcb:
            fail(f"IRQ-disabled set_tcb consumes a raw user address: {forbidden}")

    create_user_thread = function_body(TASK_CPP.read_text(), "Task* Task::create_user_thread(")
    for forbidden in ["get_virt_pointer", "translate(", "PA_ENTRY", "PA_ARG"]:
        if forbidden in create_user_thread:
            fail(f"thread construction raw-reads its syscall-supplied stack: {forbidden}")

    vfs = VFS_CORE_CPP.read_text()
    expected_vfs_bounces = {
        "auto vfs_read(": "vfs_read_user_bounced(*t, f, buf, count",
        "auto vfs_write(": "vfs_write_file(f, buf, count, actual_size)",
        "auto vfs_pread(": "vfs_read_user_bounced(*task, f, buf, count",
        "auto vfs_pwrite(": "vfs_pwrite_file(file, buf, count, offset)",
        "auto vfs_pwrite_file(": "vfs_pwrite_user_bounced(*task, f, buf, count",
    }
    for function, bounce in expected_vfs_bounces.items():
        if bounce not in function_body(vfs, function):
            fail(f"{function} no longer routes syscall byte buffers through the bounded VFS usercopy seam")

    write_file = function_body(vfs, "auto vfs_write_file(")
    if "vfs_write_user_bounced(*task, f, buf, count, actual_size)" not in write_file:
        fail("vfs_write must route eligible syscall buffers through vfs_write_user_bounced")


def main() -> None:
    test_register_to_pointer_conversions_are_closed()
    test_raw_translation_exceptions_are_only_unpublished_images()
    test_transitive_address_consumers_do_not_raw_access_userspace()
    print("syscall raw user-access audit is closed")


if __name__ == "__main__":
    main()
