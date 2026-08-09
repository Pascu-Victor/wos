#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
USERCOPY_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "usercopy.hpp"
USERCOPY_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "usercopy.cpp"
PHYS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.hpp"
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
VIRT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.hpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
WAITPID_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "waitpid.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
FUTEX_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "futex" / "futex.cpp"
TIME_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "time" / "time.cpp"
SHM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "shm" / "shm.cpp"
VFS_SYSCALL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vfs" / "sys_vfs.cpp"
VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"
LOG_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "log" / "sys_log.cpp"
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
SMT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "smt" / "smt.cpp"
PTRACE_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "debug" / "ptrace.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"
MM_KTEST_CPP = ROOT / "modules" / "kern" / "src" / "test" / "mm_ktest.cpp"


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
    candidates = [
        source.find(f"auto {name}("),
        source.find(f"void {name}("),
        source.find(f"inline auto {name}("),
        source.find(f"inline void {name}("),
        source.find(f"int64_t {name}("),
    ]
    start = min((candidate for candidate in candidates if candidate >= 0), default=-1)
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def case_body(source: str, name: str) -> str:
    marker = f"case ops::{name}: {{"
    start = source.find(marker)
    if start < 0:
        fail(f"VFS syscall case {name} not found")
    brace = source.find("{", start)
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def require_common_usercopy_helper(header: str, source: str) -> None:
    for snippet in [
        "namespace ker::mod::sys::usercopy",
        "USER_ADDR_LIMIT = 0x0000800000000000ULL",
        "copy_from_task",
        "copy_to_task",
        "copy_to_task_mapped",
        "CopyResult",
        "copy_from_task_partial",
        "copy_to_task_partial",
        "copy_to_task_mapped_partial",
        "copy_from_task_mapped",
        "copy_cstring_from_task",
        "copy_cstring_from_task_status",
        "copy_cstring_from_task_strict",
        "CStringCopyStatus",
        "StableUserPage",
        "pin_task_user_page",
        "mapped_physical_address",
        "commit_write",
        "copy_value_from_task",
        "copy_value_to_task",
        "copy_value_to_task_mapped",
    ]:
        if snippet not in header:
            fail(f"usercopy header missing public helper: {snippet}")

    for snippet in [
        "__builtin_add_overflow",
        "mm::virt::pin_user_page_for_task(&task, pagemap, user_addr, require_writable, fault_in, out.m_pin)",
        "pin_task_user_page(task, page, true, true, pin)",
        "pin_task_user_page(task, CUR, true, fault_in, pin)",
        "pin_task_user_page(task, CUR, false, fault_in, pin)",
        "mm::virt::user_page_pin_still_mapped(m_pin)",
        "mm::virt::user_page_pin_commit_write(m_pin)",
        "mm::virt::unpin_user_page(m_pin)",
        "if (!pin.commit_write())",
        "copy_to_task_common(task, user_addr, src, size, true)",
        "copy_to_task_common(task, user_addr, src, size, false)",
        "copy_cstring_from_task_status(task, user_addr, dst, dst_size) == CStringCopyStatus::COMPLETE",
    ]:
        if snippet not in source:
            fail(f"usercopy implementation missing safety snippet: {snippet}")

    for forbidden in [
        "ACTIVE_COPY",
        "reinterpret_cast<uint8_t*>(CUR)",
        "reinterpret_cast<const uint8_t*>(CUR)",
    ]:
        if forbidden in source:
            fail(f"usercopy must never dereference an active userspace VA directly: {forbidden}")


def require_mapping_stable_pin_contract() -> None:
    phys_hpp = PHYS_HPP.read_text()
    phys_cpp = PHYS_CPP.read_text()
    virt_hpp = VIRT_HPP.read_text()
    virt_cpp = VIRT_CPP.read_text()
    task_hpp = TASK_HPP.read_text()
    task_cpp = TASK_CPP.read_text()
    exit_cpp = EXIT_CPP.read_text()
    exec_cpp = EXEC_CPP.read_text()
    scheduler_cpp = SCHEDULER_CPP.read_text()
    ktest = MM_KTEST_CPP.read_text()

    for snippet in [
        "page_ref_try_inc(void* page)",
        "page_ref_try_inc(void* page, PageLookupHint* hint)",
    ]:
        if snippet not in phys_hpp or snippet not in phys_cpp:
            fail(f"physical frame try-pin contract is missing: {snippet}")
    try_pin_start = phys_cpp.find("auto page_ref_try_inc(void* page, PageLookupHint* hint) -> bool")
    try_pin_end = phys_cpp.find("void page_ref_add", try_pin_start)
    if try_pin_start < 0 or try_pin_end < 0:
        fail("physical frame try-pin implementation is missing")
    try_pin = phys_cpp[try_pin_start:try_pin_end]
    for snippet in ["old_ref != 0", "old_ref != UINT32_MAX", "compare_exchange_weak"]:
        if snippet not in try_pin:
            fail(f"physical frame try-pin must reject dead/overflowed frames: {snippet}")

    for snippet in [
        "struct UserPagePin",
        "pin_user_page",
        "pin_user_page_for_task",
        "user_page_pin_still_mapped",
        "user_page_pin_commit_write",
        "unpin_user_page",
        "bool require_writable",
    ]:
        if snippet not in virt_hpp:
            fail(f"VM user-page pin API is missing: {snippet}")

    pin = function_body(virt_cpp, "pin_user_page")
    pin_common = function_body(virt_cpp, "pin_user_page_common")
    for snippet in [
        "acquire_user_pagemap_access(pagemap, wait_for_teardown, access_gate_index)",
        "user_mapping_pin_lock.lock_irqsave()",
        "snapshot_user_page_mapping_locked",
        "phys::page_ref_try_inc",
        "confirmed_page == physical_page",
        ".require_writable = require_writable",
        "user_mapping_pin_lock.unlock_irqrestore",
    ]:
        if snippet not in pin_common:
            fail(f"VM user-page pin is not mapping-stable: {snippet}")

    task_pin = function_body(virt_cpp, "pin_user_page_for_task")
    if "pin_user_page_common(task, expected_pagemap" not in task_pin:
        fail("task user-page pin must acquire root lifetime before fault resolution")

    write_commit = function_body(virt_cpp, "user_page_pin_commit_write")
    for snippet in [
        "user_mapping_pin_lock.lock_irqsave()",
        "CURRENT_PAGE == pin.physical_page",
        "__atomic_fetch_or(raw_entry, PTE_DIRTY_BIT, __ATOMIC_ACQ_REL)",
        "user_mapping_pin_lock.unlock_irqrestore",
    ]:
        if snippet not in write_commit:
            fail(f"stable usercopy write commit is incomplete: {snippet}")

    for snippet in [
        "USER_PAGEMAP_ACCESS_GATE_COUNT = 4096",
        "gate.teardown.load(std::memory_order_acquire)",
        "gate.readers.compare_exchange_weak",
        "begin_user_pagemap_exclusive(pagemap)",
        "while (gate.readers.load(std::memory_order_acquire) != 0)",
        "UserPagemapExclusiveScope teardown_scope(pagemap)",
        "state->access_gate_index = begin_user_pagemap_exclusive(pagemap)",
    ]:
        if snippet not in virt_cpp:
            fail(f"shared pagemap usercopy lifetime gate is missing: {snippet}")

    for name, mutation in [
        ("map_page", "entry = paging::create_page_table_entry"),
        ("map_page_batched", "entry = paging::create_page_table_entry"),
        ("unmap_page", "*entry = paging::purge_page_table_entry()"),
        ("unify_page_flags", "entry.writable ="),
    ]:
        body = function_body(virt_cpp, name)
        lock = body.find("user_mapping_pin_lock.lock_irqsave()")
        write = body.find(mutation)
        unlock = body.find("user_mapping_pin_lock.unlock_irqrestore", write)
        if lock < 0 or write < 0 or unlock < 0 or not (lock < write < unlock):
            fail(f"{name} must serialize leaf publication with usercopy pins")

    cow = function_body(virt_cpp, "resolve_user_write_mapping")
    if cow.count("user_mapping_pin_lock.lock_irqsave()") < 2:
        fail("COW must serialize both its initial leaf decision and final replacement with usercopy pins")
    first_pin_lock = cow.find("user_mapping_pin_lock.lock_irqsave()")
    old_frame_pin = cow.find("phys::page_ref_inc(old_virt, &cow_lookup)")
    first_pin_unlock = cow.find("user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS)", old_frame_pin)
    allocation = cow.find("alloc_cow_destination_page")
    final_pin_lock = cow.find("user_mapping_pin_lock.lock_irqsave()", first_pin_lock + 1)
    final_leaf_write = cow.find("pte = pte_from_raw(raw_now)", final_pin_lock)
    final_pin_unlock = cow.find("user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS)", final_leaf_write)
    if min(first_pin_lock, old_frame_pin, first_pin_unlock, allocation, final_pin_lock, final_leaf_write, final_pin_unlock) < 0:
        fail("COW usercopy-pin serialization markers are incomplete")
    if not (first_pin_lock < old_frame_pin < first_pin_unlock < allocation < final_pin_lock < final_leaf_write < final_pin_unlock):
        fail("COW must pin its old leaf under the mapping lock, allocate unlocked, then publish under the mapping lock")

    fork_cow = function_body(virt_cpp, "deep_copy_user_pagemap_cow")
    scope = fork_cow.find("UserPagemapExclusiveScope source_usercopy_scope(src)")
    first_source_leaf = fork_cow.find("auto& src_pml4e = entry_at(src, i4)")
    final_flush = fork_cow.find("flush_pagemap_after_update(src, 0, true)")
    if min(scope, first_source_leaf, final_flush) < 0 or not (scope < first_source_leaf < final_flush):
        fail("fork COW must exclude stable usercopy pins through source mutation and TLB flush")

    for snippet in [
        "usercopy_pagemap_accesses",
        "usercopy_pagemap_closing",
        "try_acquire_usercopy_pagemap",
        "detach_pagemap_after_usercopy_quiescence",
        "replace_pagemap_after_usercopy_quiescence",
    ]:
        if snippet not in task_hpp:
            fail(f"Task pagemap lifetime gate is missing: {snippet}")
    for snippet in [
        "usercopy_pagemap_closing.store(true, std::memory_order_release)",
        "while (usercopy_pagemap_accesses.load(std::memory_order_acquire) != 0)",
    ]:
        if snippet not in task_cpp:
            fail(f"Task pagemap quiescence is incomplete: {snippet}")

    if "detach_pagemap_after_usercopy_quiescence()" not in exit_cpp:
        fail("process exit must detach its pagemap after quiescing usercopy")
    if "replace_pagemap_after_usercopy_quiescence(new_pagemap)" not in exec_cpp:
        fail("exec must publish its new pagemap after quiescing usercopy")
    if "quiesce_usercopy_pagemap()" not in scheduler_cpp:
        fail("deferred scheduler teardown must quiesce usercopy before pagemap destruction")

    for snippet in [
        "KTEST(MM, RefCountTryIncPinsOnlyLivePages)",
        "KTEST(MM, UserPagePinSurvivesUnmapAndRejectsUnsafeLeaves)",
        "KTEST(MM, UsercopyMappedPinFailsClosedDuringPagemapExclusiveMutation)",
        "KTEST(MM, UsercopyCrossPageProgressIsStableAndBounded)",
        "KTEST(MM, UsercopyRejectsInvalidUnmappedAndReadonlyRanges)",
        "KTEST(MM, UsercopyMaterializesLazyAnonymousPages)",
        "KTEST(MM, UsercopyCopyoutResolvesCowWithoutMutatingPeer)",
        "KEXPECT_FALSE(virt::user_page_pin_still_mapped(pin))",
        "KEXPECT_FALSE(virt::pin_user_page(root, READONLY_VADDR, true, pin))",
        "KEXPECT_FALSE(virt::pin_user_page(root, SUPERVISOR_VADDR, false, pin))",
        "KEXPECT_EQ(read.bytes_copied, HALF)",
        "KEXPECT_EQ(write.bytes_copied, HALF)",
        "KEXPECT_TRUE(dirty_pin.dirty)",
        "KEXPECT_FALSE(usercopy::copy_from_task(task, UINT64_MAX - 1, &kernel_byte, 4))",
        "KEXPECT_FALSE(usercopy::copy_to_task(task, READONLY_VADDR, &replacement, sizeof(replacement)))",
        "KEXPECT_FALSE(usercopy::copy_from_task_mapped(task, LAZY_VADDR, output.data(), output.size()))",
        "KREQUIRE_TRUE(usercopy::copy_to_task(task, LAZY_VADDR, input.data(), input.size()))",
        "KEXPECT_NE(PARENT_PHYS, CHILD_PHYS)",
    ]:
        if snippet not in ktest:
            fail(f"mapping-stable usercopy KTEST coverage is missing: {snippet}")


def require_process_syscalls_use_usercopy(source: str) -> None:
    for name in [
        "wos_proc_uname",
        "wos_proc_sigaction",
        "wos_proc_sigaltstack",
        "wos_proc_sigprocmask",
        "wos_proc_sigpending",
        "wos_proc_sigsuspend",
        "wos_proc_clone_vm",
        "wos_proc_prctl",
        "wos_proc_arch_prctl",
        "wos_proc_setwkitarget",
        "wos_proc_getwkitarget",
    ]:
        body = function_body(source, name)
        if "usercopy::" not in body:
            fail(f"{name} must use platform/sys/usercopy for user pointers")

    process_body = function_body(source, "process")
    for snippet in [
        "copy_value_to_task(*task, a2, task->uid)",
        "copy_value_to_task(*task, a3, task->euid)",
        "copy_value_to_task(*task, a4, task->suid)",
        "copy_value_to_task(*task, a2, task->gid)",
        "copy_value_to_task(*task, a3, task->egid)",
        "copy_value_to_task(*task, a4, task->sgid)",
        "copy_to_task(*task, a2, name, LEN + 1)",
        "copy_from_task(*task, a2, name.data(), len)",
    ]:
        if snippet not in process_body:
            fail(f"process syscall dispatch still lacks usercopy snippet: {snippet}")


def require_waitpid_outputs_are_preflighted(source: str) -> None:
    for snippet in [
        "waitpid_outputs_writable",
        "usercopy::ensure_writable(task, reinterpret_cast<uint64_t>(status), sizeof(*status))",
        "usercopy::ensure_writable(task, rusage_vaddr, sizeof(KernRusage))",
        "write_status_to_user",
        "write_rusage_to_user",
    ]:
        if snippet not in source:
            fail(f"waitpid output handling missing snippet: {snippet}")

    body = function_body(source, "wos_proc_waitpid")
    preflight = body.find("waitpid_outputs_writable(*current_task, status, rusage_vaddr)")
    first_claim = body.find("claim_exited_child")
    if preflight < 0 or first_claim < 0 or preflight > first_claim:
        fail("waitpid must validate output pointers before claiming/consuming children")

    if "*status =" in source or "fill_rusage(PHYS" in source or "get_virt_pointer(rusage" in source:
        fail("waitpid must not write status/rusage through raw user pointers or physical aliases")


def require_deferred_waiters_use_usercopy() -> None:
    files = {
        "exit": EXIT_CPP.read_text(),
        "scheduler": SCHEDULER_CPP.read_text(),
        "signal": SIGNAL_CPP.read_text(),
        "ptrace": PTRACE_CPP.read_text(),
        "remote_compute": REMOTE_COMPUTE_CPP.read_text(),
    }
    for label, source in files.items():
        if "wait_status_user_addr" in source and "usercopy::copy_value_to_task" not in source:
            fail(f"{label} wait-status completion must use usercopy")
        forbidden = ["STATUS_PHYS", "RUSAGE_PHYS", "get_virt_pointer(STATUS_PHYS)", "get_virt_pointer(RUSAGE_PHYS)"]
        for snippet in forbidden:
            if snippet in source:
                fail(f"{label} still writes waitpid output through physical alias: {snippet}")

    for source, name in [
        (EXIT_CPP.read_text(), "complete_exit_wait"),
        (SCHEDULER_CPP.read_text(), "complete_waitpid_exit_for_scheduler"),
        (SCHEDULER_CPP.read_text(), "complete_registered_waitpid_exit_for_scheduler"),
        (SCHEDULER_CPP.read_text(), "complete_waitpid_ptrace_stop_for_scheduler"),
        (SIGNAL_CPP.read_text(), "complete_waitpid_stop_waiter"),
        (PTRACE_CPP.read_text(), "complete_trace_wait"),
        (REMOTE_COMPUTE_CPP.read_text(), "try_complete_proxy_wait"),
    ]:
        body = function_body(source, name)
        if "static_cast<uint64_t>(-EFAULT)" not in body:
            fail(f"{name} must report EFAULT when deferred output copy fails")


def require_futex_timeout_and_address_usercopy(source: str) -> None:
    timeout_body = function_body(source, "relative_timeout_us")
    signature_start = source.find("auto relative_timeout_us(")
    signature_end = source.find("{", signature_start)
    if signature_start < 0 or signature_end < 0 or "mod::sched::task::Task& task" not in source[signature_start:signature_end]:
        fail("futex timeout helper must accept the current task for usercopy")
    for snippet in [
        "mod::sys::usercopy::copy_value_from_task(task, timeout_user_addr, ts)",
        "return -EFAULT;",
    ]:
        if snippet not in timeout_body:
            fail(f"futex timeout must be copied from user memory; missing {snippet}")

    for name in ["futex_wait", "futex_wake"]:
        body = function_body(source, name)
        guard = body.find("mod::sys::usercopy::range_valid(user_addr, sizeof(int))")
        pin = body.find("mod::sys::usercopy::pin_task_user_page")
        if guard < 0 or pin < 0 or guard > pin:
            fail(f"{name} must reject non-user futex addresses before stable pinning")
        for forbidden in ["mod::mm::virt::translate", "mod::mm::addr::get_virt_pointer"]:
            if forbidden in body:
                fail(f"{name} bypasses usercopy through {forbidden}")


def require_fixed_size_time_and_shm_usercopy() -> None:
    time = TIME_CPP.read_text()
    shm = SHM_CPP.read_text()

    time_body = function_body(time, "sys_time_get")
    for snippet in [
        "copy_value_to_task(*task, arg1, tv)",
        "copy_value_to_task(*task, arg1, ts)",
        "copy_value_from_task(*task, arg1, req)",
        "copy_value_from_task(*task, arg2, new_value)",
        "copy_value_to_task(*task, arg2, current_value)",
    ]:
        if snippet not in time_body:
            fail(f"time syscall must use common usercopy: {snippet}")

    shmctl = function_body(shm, "shmctl_impl")
    copy = shmctl.find("usercopy::copy_value_to_task(*task, buf_addr, stat)")
    unlock = shmctl.rfind("g_lock.unlock_irqrestore(FLAGS)", 0, copy)
    if copy < 0 or unlock < 0:
        fail("SHM IPC_STAT must snapshot under g_lock and copy through usercopy after unlock")
    if "*user_buf = stat" in shmctl:
        fail("SHM IPC_STAT still writes through a raw userspace pointer")


def require_vfs_byte_io_uses_bounded_usercopy() -> None:
    source = VFS_SYSCALL_CPP.read_text()

    range_check = function_body(source, "user_io_buffer_has_bounded_user_range")
    for snippet in [
        "if (size == 0 || user_addr == 0)",
        "task != nullptr && task->pagemap != nullptr",
        "usercopy::range_valid(user_addr, size)",
    ]:
        if snippet not in range_check:
            fail(f"VFS byte-I/O range guard is incomplete: {snippet}")

    output_preflight = function_body(source, "preflight_optional_user_output")
    for snippet in [
        "if (user_addr == 0)",
        "task != nullptr && task->pagemap != nullptr",
        "usercopy::ensure_writable(*task, user_addr, size)",
    ]:
        if snippet not in output_preflight:
            fail(f"VFS byte-I/O output preflight is incomplete: {snippet}")

    calls = {
        "READ": "ker::vfs::vfs_read(FD, buf, len",
        "WRITE": "ker::vfs::vfs_write(FD, buf, len",
        "PREAD": "ker::vfs::vfs_pread(FD, buf, count, offset)",
        "PWRITE": "ker::vfs::vfs_pwrite(FD, buf, count, offset)",
    }
    for name, call in calls.items():
        body = case_body(source, name)
        count = "len" if name in ("READ", "WRITE") else "count"
        guard = body.find(f"user_io_buffer_has_bounded_user_range(a2, {count})")
        dispatch = body.find(call)
        if guard < 0 or dispatch < 0 or guard > dispatch:
            fail(f"VFS {name} must reject unbounded user ranges before backend dispatch")

        if name in ("READ", "WRITE"):
            preflight = body.find("preflight_optional_user_output(a4, sizeof(size_t))")
            if preflight < 0 or preflight > dispatch:
                fail(f"VFS {name} must preflight its optional result pointer before I/O")


def require_ptrace_and_mremap_use_stable_pages() -> None:
    ptrace = PTRACE_CPP.read_text()
    copy_memory = function_body(ptrace, "copy_task_memory")
    for snippet in [
        "write_target ? pin.commit_write() : pin.still_mapped()",
        "if (!COMMITTED)",
    ]:
        if snippet not in copy_memory:
            fail(f"ptrace WRITE_MEM must commit HHDM writes through the stable pin: {snippet}")

    write_word = function_body(ptrace, "write_task_word")
    for snippet in [
        "usercopy::range_valid(target_addr, sizeof(word))",
        "pin_task_user_page(target, target_addr, true, true, first_page)",
        "pin_task_user_page(target, target_addr + FIRST_SIZE, true, true, second_page)",
        "first_page.still_mapped()",
        "second_page.still_mapped()",
        "std::memcpy(first_page.kernel_address(), bytes, FIRST_SIZE)",
        "std::memcpy(second_page.kernel_address(), bytes + FIRST_SIZE, sizeof(word) - FIRST_SIZE)",
        "bool const FIRST_COMMITTED = first_page.commit_write()",
        "bool const SECOND_COMMITTED = !second_page.valid() || second_page.commit_write()",
    ]:
        if snippet not in write_word:
            fail(f"ptrace POKEDATA must preflight and pin an entire cross-page word: {snippet}")
    if "ret = write_task_word(*target, addr, data);" not in ptrace:
        fail("ptrace POKEDATA must use the all-pages-pinned word writer")

    mremap = function_body(VMEM_CPP.read_text(), "anon_mremap")
    for snippet in [
        "pin_task_user_page(*task, SRC_VA, false, true, src_page)",
        "ensure_user_page_writable(task, DST_VA)",
        "pin_task_user_page(*task, DST_VA, true, false, dst_page)",
        "std::memcpy(dst_page.kernel_address(), src_page.kernel_address()",
        "bool const SOURCE_STILL_MAPPED = src_page.still_mapped()",
        "bool const DESTINATION_COMMITTED = dst_page.commit_write()",
        "!SOURCE_STILL_MAPPED || !DESTINATION_COMMITTED",
        "-ker::abi::vmem::VMEM_EFAULT",
    ]:
        if snippet not in mremap:
            fail(f"mremap must copy through mapping-stable pages: {snippet}")
    for forbidden in ["virt::translate", "addr::get_virt_pointer"]:
        if forbidden in mremap:
            fail(f"mremap must not copy through a raw translated address: {forbidden}")

    vmem = VMEM_CPP.read_text()
    msync = function_body(vmem, "sync_file_mmap_range_impl")
    snapshot = function_body(vmem, "snapshot_file_mmap_page")
    for snippet in [
        "snapshot_file_mmap_page(pagemap, live_task",
        "FileMmapPageSnapshot::FAULT",
        "write_file_mapping_bytes(range.file, snapshot.data()",
    ]:
        if snippet not in msync:
            fail(f"MSYNC must use a stable kernel snapshot before backend I/O: {snippet}")
    for snippet in [
        "pin_task_user_page(*live_task, page_start, false, false, pin)",
        "std::memcpy(snapshot.data()",
        "pin.still_mapped()",
        "virt::pin_user_page(pagemap, page_start, false, pin, true)",
        "virt::user_page_pin_still_mapped(pin)",
        "virt::unpin_user_page(pin)",
    ]:
        if snippet not in snapshot:
            fail(f"MSYNC page snapshot must remain mapping-stable: {snippet}")
    backend = msync.find("write_file_mapping_bytes(range.file, snapshot.data()")
    pin = msync.find("pin_task_user_page")
    if backend < 0 or pin >= 0:
        fail("MSYNC backend I/O must receive only an unpinned kernel snapshot")


def require_string_and_ioctl_contracts_are_closed() -> None:
    log = LOG_CPP.read_text()
    safe_copy = function_body(log, "sys_log")
    if "copy_cstring_from_task_strict(*current_task, src_user_addr, dest, dest_size)" not in safe_copy:
        fail("NUL-terminated logging must reject unterminated user strings")

    vfs = VFS_SYSCALL_CPP.read_text()
    marshal = function_body(vfs, "pty_ioctl_user_marshal")
    for snippet in ["case TIOCGPTN:", "case TCSETSF:", "case TIOCSCTTY:", "case TIOCNOTTY:", "case TCFLSH:"]:
        if snippet not in marshal:
            fail(f"VFS ioctl usercopy allowlist is missing supported PTY command: {snippet}")
    ioctl = case_body(vfs, "IOCTL")
    for snippet in [
        "if (!MARSHAL.supported)",
        "return -ENOTTY;",
        "usercopy::copy_from_task(*task, a3, kernel_arg.data(), MARSHAL.size)",
        "usercopy::ensure_writable(*task, a3, MARSHAL.size)",
        "usercopy::copy_to_task(*task, a3, kernel_arg.data(), MARSHAL.size)",
    ]:
        if snippet not in ioctl:
            fail(f"VFS ioctl must be closed-world and bounce pointer arguments: {snippet}")


def require_thread_publication_uses_preflighted_mapped_usercopy() -> None:
    source = THREAD_CONTROL_CPP.read_text()
    publish = function_body(source, "publish_thread_tid_to_tcb")
    thread_control = function_body(source, "thread_control")
    clone_vm = function_body(PROCESS_CPP.read_text(), "wos_proc_clone_vm")

    set_tcb_start = thread_control.find("case abi::multiproc::threadControlOps::SET_TCB:")
    set_tcb_end = thread_control.find("case abi::multiproc::threadControlOps::YIELD:", set_tcb_start)
    if set_tcb_start < 0 or set_tcb_end < 0:
        fail("SET_TCB syscall case is missing")
    set_tcb = thread_control[set_tcb_start:set_tcb_end]
    set_tcb_steps = [
        "ensure_writable(*task, arg1, mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES)",
        "copy_value_to_task(*task, arg1, arg1)",
        "sync_task_signal_mask_cache_at(task, arg1)",
        "mod::smt::set_tcb(arg1)",
    ]
    positions = [set_tcb.find(step) for step in set_tcb_steps]
    if min(positions) < 0 or positions != sorted(positions):
        fail("SET_TCB must preflight and initialize the TCB before publishing FS_BASE")
    if "reinterpret_cast" in set_tcb:
        fail("SET_TCB must pass only an address value beyond the usercopy seam")

    smt = function_body(SMT_CPP.read_text(), "set_tcb")
    if "current_task->thread->fsbase = tcb_addr" not in smt or "cpu::wrfsbase(tcb_addr)" not in smt:
        fail("SMT set_tcb must install the validated address value")
    for forbidden in ["static_cast<uint64_t*>(tcb", "reinterpret_cast<uint64_t>(tcb", "*tcb"]:
        if forbidden in smt:
            fail(f"IRQ-disabled SMT set_tcb still accesses userspace: {forbidden}")

    arch_prctl = function_body(PROCESS_CPP.read_text(), "wos_proc_arch_prctl")
    arch_set_start = arch_prctl.find("case WOS_ARCH_SET_FS:")
    arch_set_end = arch_prctl.find("case WOS_ARCH_GET_FS:", arch_set_start)
    if arch_set_start < 0 or arch_set_end < 0:
        fail("ARCH_SET_FS syscall case is missing")
    if "usercopy::" in arch_prctl[arch_set_start:arch_set_end] or "sync_task_signal_mask_cache" in arch_prctl[arch_set_start:arch_set_end]:
        fail("ARCH_SET_FS is an address-only operation and must not dereference the supplied base")

    if "pin_task_user_page(*parent, tid_user_addr, true, false, tid_page)" not in publish:
        fail("thread TID publication must use a mapped-only stable page")
    if "pin_task_user_page(*parent, tid_user_addr, true, true, tid_page)" in publish:
        fail("thread TID publication must not fault while the publication mutex is held")
    if "return tid_page.commit_write();" not in publish:
        fail("thread TID publication must commit its HHDM write and mark the live PTE dirty")

    preflight = thread_control.find("ensure_writable(*parent, tcb_va, mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES)")
    stack_snapshot = thread_control.find(
        "copy_from_task(*parent, user_sp, prepared_stack_words.data(), sizeof(prepared_stack_words))"
    )
    guard = thread_control.find("SharedVmemPublicationGuard publication_guard")
    cache = thread_control.find("sync_task_signal_mask_cache_mapped(t)", guard)
    if min(preflight, stack_snapshot, guard, cache) < 0 or not (preflight < stack_snapshot < guard < cache):
        fail("THREAD_CREATE must snapshot its stack and preflight the TCB before mapped-only publication")
    task_source = TASK_CPP.read_text()
    for forbidden in ["mm::virt::translate(parent->pagemap, user_sp)", "get_virt_pointer(PA_ENTRY)", "get_virt_pointer(PA_ARG)"]:
        if forbidden in task_source:
            fail(f"thread construction still raw-reads the prepared userspace stack: {forbidden}")

    clone_preflight = clone_vm.find("ensure_writable(*parent, child_fsbase, sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES)")
    clone_guard = clone_vm.find("SharedVmemPublicationGuard publication_guard")
    clone_cache = clone_vm.find("sync_task_signal_mask_cache_mapped(child)", clone_guard)
    if min(clone_preflight, clone_guard, clone_cache) < 0 or not (clone_preflight < clone_guard < clone_cache):
        fail("CLONE_VM must preflight the child TCB before mapped-only publication")


def main() -> None:
    require_common_usercopy_helper(USERCOPY_HPP.read_text(), USERCOPY_CPP.read_text())
    require_mapping_stable_pin_contract()
    require_process_syscalls_use_usercopy(PROCESS_CPP.read_text())
    require_waitpid_outputs_are_preflighted(WAITPID_CPP.read_text())
    require_deferred_waiters_use_usercopy()
    require_futex_timeout_and_address_usercopy(FUTEX_CPP.read_text())
    require_fixed_size_time_and_shm_usercopy()
    require_vfs_byte_io_uses_bounded_usercopy()
    require_ptrace_and_mremap_use_stable_pages()
    require_string_and_ioctl_contracts_are_closed()
    require_thread_publication_uses_preflighted_mapped_usercopy()


if __name__ == "__main__":
    main()
