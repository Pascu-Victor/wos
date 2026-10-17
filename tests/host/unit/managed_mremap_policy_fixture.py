"""Finite actual308 policy extraction; no operation at import.

The complete token/count/survey/publisher/raw-entry bodies execute on the
existing declared Host root/current/IRQ ports. Exit/exec are exact source
frontiers/prefixes, not complete scheduler/image teardown implementations.
"""
from pathlib import Path
import hashlib
import json


class Builder:
    def __init__(self, kernel_file, support, definition, bounded):
        self.definition = definition
        self.case_path = support / "managed_mremap_policy_fixture.inc"
        names = ["syscalls_impl/vmem/sys_vmem.cpp", "syscalls_impl/vmem/sys_vmem.hpp",
                 "platform/mm/virt.opt.cpp", "platform/mm/managed_huge.hpp",
                 "platform/sched/task.hpp", "syscalls_impl/process/exit.cpp",
                 "syscalls_impl/process/exec.cpp", "syscalls_impl/multiproc/threadControl.cpp",
                 "vfs/fd_reservation.cpp"]
        self.paths = {n: kernel_file(n) for n in names}
        self.inputs = [Path(__file__), self.case_path, *self.paths.values()]
        self.rows = []
        texts = {n: p.read_text() for n, p in self.paths.items()}

        def body(name, anchor):
            value = definition(texts[name], anchor)
            self.rows.append(dict(path=str(self.paths[name]), anchor=anchor,
                                 scope="whole actual body", bytes=len(value.encode()),
                                 sha256=hashlib.sha256(value.encode()).hexdigest()))
            return value

        self.policy_class = body("syscalls_impl/vmem/sys_vmem.hpp", "class PendingMremapPsPolicy {")
        self.mm = "\n".join(body("platform/mm/virt.opt.cpp", a) for a in (
            "auto mremap_ps_access_matches(", "auto acquire_mremap_ps_policy(",
            "auto validate_mremap_ps_free(", "void release_mremap_ps_policy("))
        self.raw = body("platform/mm/virt.opt.cpp", "auto selftest_exchange_unpublished_user_entry(")
        self.mapper = bounded(texts["platform/mm/virt.opt.cpp"], "// BEGIN whole-batch managed mapper.", "// END whole-batch managed mapper.")
        self.batch_adapter = body("platform/mm/virt.opt.cpp", "auto publish_prepared_huge_batch(paging::PageTable*")
        self.rows.append(dict(path=str(self.paths["platform/mm/virt.opt.cpp"]), anchor="whole-batch mapper markers",
                             scope="whole actual bounded block", bytes=len(self.mapper.encode()),
                             sha256=hashlib.sha256(self.mapper.encode()).hexdigest()))
        ctor = "PendingMremapPsPolicy::PendingMremapPsPolicy() noexcept = default;"
        if texts["syscalls_impl/vmem/sys_vmem.cpp"].count(ctor) != 1:
            raise RuntimeError("actual defaulted token constructor changed")
        self.token = ctor + "\n" + "\n".join(body("syscalls_impl/vmem/sys_vmem.cpp", a) for a in (
            "PendingMremapPsPolicy::~PendingMremapPsPolicy()", "auto mremap_policy_current_context(",
            "void PendingMremapPsPolicy::cancel()", "void cancel_mremap_ps_policies(",
            "auto PendingMremapPsPolicy::prepare_current("))
        self.remap = body("syscalls_impl/vmem/sys_vmem.cpp", "auto anon_mremap(")
        self.align = body("syscalls_impl/vmem/sys_vmem.cpp", "auto align_user_vmem_size(")
        source = texts["syscalls_impl/vmem/sys_vmem.cpp"]
        self.constants = "\n".join(next(line for line in source.splitlines() if line.startswith("constexpr uint64_t " + n + " ="))
                                   for n in ("USER_SPACE_END", "MREMAP_MAYMOVE"))
        # Actual prefixes include their real early returns and first following
        # effect. The appended terminal return is only a finite endpoint.
        commit = body("syscalls_impl/process/exec.cpp", "auto commit_native_exec_image(")
        stop = "    auto& retired = prepared.retired();"
        self.exec_prefix = commit[:commit.index(stop)] + "    (void)image; return 0;\n}"
        pending = body("syscalls_impl/process/exec.cpp", "void service_pending_exec_handoff()")
        stop = "    if (handoff == nullptr) return;"
        self.pending_prefix = pending[:pending.index(stop)] + "    (void)handoff;\n}"
        self.exit_hooks = []
        for name, actor in (("syscalls_impl/process/exit.cpp", "current_task"), ("syscalls_impl/multiproc/threadControl.cpp", "task")):
            start = "    ker::vfs::cancel_fd_reservations(*" + actor + ");"
            text = texts[name]
            if text.count(start) != 1:
                raise RuntimeError("actual exit FD frontier changed")
            begin = text.index(start)
            following = "cancel_process_reservations(*" + actor + ");"
            end = text.index(following, begin) + len(following)
            value = text[begin:end]
            self.exit_hooks.append(value)
            self.rows.append(dict(path=str(self.paths[name]), anchor=start,
                                 scope="exact actual FD/policy/resource call frontier", bytes=len(value.encode()),
                                 sha256=hashlib.sha256(value.encode()).hexdigest()))
        for label, value in (("native commit prefix", self.exec_prefix), ("pending claim prefix", self.pending_prefix)):
            self.rows.append(dict(path=str(self.paths["syscalls_impl/process/exec.cpp"]), anchor=label,
                                 scope="actual prefix with explicit terminal endpoint", bytes=len(value.encode()),
                                 sha256=hashlib.sha256(value.encode()).hexdigest()))
        self.test_count = self.case_path.read_text().count("TEST_F(ManagedMremapPolicy,")
        if not 1 <= self.test_count <= 16:
            raise RuntimeError("policy fixture exceeds accepted16-case cap")
        self.manifest_text = json.dumps(self.rows, sort_keys=True)

    @staticmethod
    def replace(text, old, new):
        if text.count(old) != 1:
            raise RuntimeError("actual policy endpoint changed: " + old[:100])
        return text.replace(old, new, 1)

    def extend_context(self, context):
        task = context / "platform/sched/task.hpp"
        text = task.read_text()
        text = self.replace(text, "namespace ker::mod::sched::task {\nstruct LazyVmemRange;",
                            "namespace ker::syscall::vmem { class PendingMremapPsPolicy; }\nnamespace ker::mod::sched::task {\nstruct LazyVmemRange;")
        text = self.replace(text, " mm::paging::PageTable* pagemap{};", """ mm::paging::PageTable* pagemap{};
 uint64_t pid{7}; bool is_thread{}; void* thread{};
 std::atomic<bool> scheduler_published{false}, exec_preparation_teardown_in_progress{false};
 std::atomic<void*> native_file_mapping_retirement{nullptr};
 ker::syscall::vmem::PendingMremapPsPolicy* pending_mremap_ps_policies{};""")
        task.write_text(text)
        publication = context / "syscalls_impl/vmem/sys_vmem.hpp"
        publication.write_text(publication.read_text() + "\n#include <platform/mm/managed_huge_core.hpp>\nnamespace ker::syscall::vmem {\n" + self.policy_class
                               + "\nvoid cancel_mremap_ps_policies(ker::mod::sched::task::Task&) noexcept;\n}\n")

    def compose(self, text):
        prefix = """
#include <abi/callnums/vmem.h>
namespace ker::mod::mm::paging { struct PageTable; }
namespace host_policy {
inline unsigned frees{}, allocations{}, effects{}, claims{}, fd_calls{}, resource_calls{};
inline bool overlap{}, hardware_current{}, raw_current_endpoint{}, unique_ranges{}, expected_if{true};
inline ker::mod::mm::paging::PageTable* independent_root{};
inline int free_error{};
void ordinary();
}
"""
        ports = """
namespace ker::mod::mm::virt {
auto ensure_user_page_writable(ker::mod::sched::task::Task*,uint64_t) -> bool {
 ADD_FAILURE()<<"copy resolution outside this policy-only endpoint"; return false;
}
}
namespace ker::mod::sys::usercopy {
class StableUserPage {
 public:
 void* kernel_address() const { ADD_FAILURE()<<"copy backend unsupported";return nullptr; }
 bool still_mapped() const { ADD_FAILURE()<<"copy backend unsupported";return false; }
 bool commit_write() const { ADD_FAILURE()<<"copy backend unsupported";return false; }
};
struct UserAccessGuard {};
auto pin_task_user_page(ker::mod::sched::task::Task&,uint64_t,bool,bool,StableUserPage&) -> bool {
 ADD_FAILURE()<<"copy pin outside policy-only endpoint";return false;
}
}
namespace ker::syscall::vmem {
using ker::mod::sched::get_current_task;
auto page_align_up(uint64_t value) -> uint64_t {return (value+4095)&~uint64_t{4095};}
auto public_shared_file_overlap(ker::mod::sched::task::Task&,uint64_t,uint64_t) -> bool {return host_policy::overlap;}
auto anon_free(uint64_t,uint64_t) -> uint64_t {host_policy::ordinary();++host_policy::frees;return static_cast<uint64_t>(host_policy::free_error);}
auto anon_allocate(uint64_t,uint64_t,uint64_t,uint64_t) -> uint64_t {host_policy::ordinary();++host_policy::allocations;return static_cast<uint64_t>(-ENOMEM);}
}
namespace ker::vfs {void cancel_fd_reservations(ker::mod::sched::task::Task&) {++host_policy::fd_calls;}}
namespace ker::mod::sched::resource {
void cancel_process_reservations(ker::mod::sched::task::Task& task) {
 EXPECT_EQ(task.pending_mremap_ps_policies,nullptr);++host_policy::resource_calls;
}
}
namespace ker::syscall::process {
struct PreparedNativeExecImage {int view() {++host_policy::effects;return 0;}};
auto claim_pending_exec_handoff(ker::mod::sched::task::Task&) -> void* {++host_policy::claims;return nullptr;}
}
"""
        # Keep the inherited root identity for every ordinary/baseline case.
        # Only the raw-entry case selects its explicit hardware-current port.
        text = self.replace(text,
            "auto is_current_pagemap(PageTable* root) -> bool { return root == runtime_root; }",
            "auto is_current_pagemap(PageTable* root) -> bool { return host_policy::raw_current_endpoint ? host_policy::hardware_current : root == runtime_root; }")
        # This finite root-identity route is separate from every fork hook.
        # Disabled defaults preserve even the inherited null-root predicates.
        text = self.replace(text,
            "if (fork_mode && root == fork_child) return &host_child_control;",
            "if (host_policy::independent_root != nullptr && root == host_policy::independent_root) return &host_child_control;\n    if (fork_mode && root == fork_child) return &host_child_control;")
        text = self.replace(text,
            "if ((root != runtime_root && !(fork_mode && root == fork_child)) || host_range::reader_busy || host_range::exclusive) return false;",
            "if ((root != runtime_root && !(fork_mode && root == fork_child) && !(host_policy::independent_root != nullptr && root == host_policy::independent_root)) || host_range::reader_busy || host_range::exclusive) return false;")
        # Reuse the batch fixture's finite free-extent Host port. Disabled for
        # every old baseline case; no PMM/buddy allocation claim follows.
        text = self.replace(text, "return allocate(first, pages, alignment, initialization);", """
        if (host_policy::unique_ranges) {
            uint64_t chosen = first;
            for (;;) {
                bool occupied = false;
                for (auto const& block : blocks)
                    if (block.storage && chosen < block.physical + uint64_t{block.pages} * PAGE &&
                        block.physical < chosen + uint64_t{pages} * PAGE) {occupied=true;break;}
                if (!occupied) break;
                if (end - chosen < alignment) return nullptr;
                chosen += alignment;
            }
            if (chosen > end || uint64_t{pages} * PAGE > end - chosen) return nullptr;
            return allocate(chosen, pages, alignment, initialization);
        }
        return allocate(first, pages, alignment, initialization);""")
        # New bodies are appended after the exact54 old tests, which close the
        # open virt namespace. No old case byte or provider is replaced here.
        hooks = "\nnamespace host_policy { namespace mod=ker::mod;\n"
        for name, actor, body in zip(("process_exit_frontier", "worker_exit_frontier"), ("current_task", "task"), self.exit_hooks):
            hooks += "void " + name + "(ker::mod::sched::task::Task* " + actor + ") {\n" + body + "\n}\n"
        hooks += "}\n"
        return (prefix + text + ports + "\nnamespace ker::mod::mm::virt {\n" + self.mapper + "\n" + self.raw
                + "\n}\nnamespace ker::mod::mm::huge::detail {\n" + self.mm + "\n" + self.batch_adapter
                + "\n}\nnamespace ker::syscall::vmem {\n" + self.constants + "\n" + self.align + "\n" + self.token + "\n" + self.remap
                + "\n}\nnamespace ker::syscall::process {\n" + self.exec_prefix + "\n" + self.pending_prefix + "\n}\n"
                + hooks + self.case_path.read_text())

    def controls(self, text):
        bad = self.replace(text, "control->ps_admission_suspensions == 0 &&", "true &&")
        bad = self.replace(bad, "if (control != nullptr && control->ps_admission_suspensions != 0) return huge::Error::Again;", "/* omitted actual batch suspension guard */")
        yield ("mremap-publishers-ignore-count", bad, "ManagedMremapPolicy.PreparedSingleAndBatchPublishersRefuseBeforeTransfer")
        yield ("mremap-raw-allows-new-ps", self.replace(text,
               "if (NEW_PS && control != nullptr && control->ps_admission_suspensions != 0) valid_path = false;", "/* omitted actual raw new-PS guard */"),
               "ManagedMremapPolicy.RawNewPsTransitionsRetainEveryOriginalPremise")
        yield ("mremap-incomplete-final-proof", self.replace(text, "!cursor.complete || cursor.epoch_changed", "false || cursor.epoch_changed"),
               "ManagedMremapPolicy.FinalValidatorRejectsIncompleteStaleAndChangedSerial")
        yield ("mremap-release-ignores-serial", self.replace(text, "if (control != nullptr && control->serial == serial)", "if (control != nullptr)"),
               "ManagedMremapPolicy.RetiredAndReusedSerialCannotConsumeNewCount")
