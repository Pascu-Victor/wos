"""Actual public VM/rollback slices in a small TU; accepted Host ports stay explicit."""

import hashlib
from pathlib import Path


class PublicWiring:
    def __init__(self, owner):
        self.owner = owner
        self.path = owner.support / "fork_public_wiring_cases.inc"
        self.inputs = [Path(__file__), self.path]
        self.source = owner.sources["syscalls_impl/process/process.cpp"]
        self.fork = owner.definition(self.source, "auto wos_proc_fork(")
        self.test_count = self.path.read_text().count("TEST_F(PublicForkWiring,") + self.path.read_text().count("TEST(PublicForkError,")

    def body(self, anchor):
        return self.owner.body("syscalls_impl/process/process.cpp", anchor)

    def fragment(self, name, text):
        assert text in self.fork
        self.owner.extractions.append(dict(file="syscalls_impl/process/process.cpp", caller="wos_proc_fork",
            fragment=name, bytes=len(text.encode()), sha256=hashlib.sha256(text.encode()).hexdigest()))
        return text

    def extend_context(self, overlay):
        path = overlay / "platform/sched/task.hpp"
        text = path.read_text().replace("#include <platform/mm/paging.hpp>", "#include <platform/mm/paging.hpp>\n#include <platform/sched/threading.hpp>")
        anchor = " uint64_t pid{1}; bool has_run{}, is_thread{}; Task* owner{};"
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor + """
 // Additional explicit layout ports read only by the extracted rollback slices.
 char* name{}; host_fork_public::Thread* thread{};
 struct PublicContext {uint64_t syscall_scratch_area{};} context;
""")
        text = text.replace("namespace ker::vfs { struct File; }", """namespace ker::vfs { struct File; }
namespace host_fork_alloc {auto initialized_pid()->uint64_t;}
namespace host_fork_public {using Thread=ker::mod::sched::threading::Thread; void note_delete(void*); bool free_stack(void*); void lazy_lock(unsigned);}
""")
        text = text.replace("uint64_t pid{1};", "uint64_t pid{host_fork_alloc::initialized_pid()};")
        path.write_text(text)

    def components(self):
        return """
namespace host_fork_public {
void file_sync(ker::mod::mm::paging::PageTable*);
void destroy_root(ker::mod::mm::paging::PageTable*);
void release_root(ker::mod::mm::paging::PageTable*);
uint64_t finish(int64_t,uint64_t,const char*);
void vm_success(ker::mod::sched::task::Task*);
void release_fds(ker::mod::sched::task::Task*);
void abort_child(ker::mod::sched::task::Task&);
}
namespace ker::syscall::vmem {
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;
struct log {template<class...Args>static void warn(const char*,Args...) {}};
auto sync_file_mmap_range_quiesced(ker::mod::mm::paging::PageTable*r,uint64_t start,uint64_t length)->int {
 EXPECT_EQ(start,0U);EXPECT_EQ(length,uint64_t{1}<<47);host_fork_public::file_sync(r);return 0;
}
""" + self.owner.body("syscalls_impl/vmem/sys_vmem.cpp", "void release_file_mmap_ranges_for_pagemap(") + """
}
namespace ker::mod::mm::virt {
void destroy_user_space(PageTable*r,uint64_t,const char*,const char*){host_fork_public::destroy_root(r);}
void release_pagemap(PageTable*r,uint64_t){host_fork_public::release_root(r);}
}
namespace ker::syscall::process {
void release_cloned_fd_table_refs(ker::mod::sched::task::Task*t){host_fork_public::release_fds(t);}
namespace child_events {void abort_publication(ker::mod::sched::task::Task&t){host_fork_public::abort_child(t);}}
}
"""

    def caller_source(self):
        prelude = """
#include <cstdint>
#include <cerrno>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/syscall_preparation.hpp>
namespace ker::syscall::shm {void shm_cleanup_for_task(ker::mod::sched::task::Task*);}
namespace ker::syscall::vmem {void release_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable*);}
namespace ker::mod::sched::task {void release_lazy_vmem_ranges(Task&);}
namespace ker::mod::mm::virt {
void destroy_user_space(ker::mod::mm::paging::PageTable*,uint64_t,const char*,const char*);
void release_pagemap(ker::mod::mm::paging::PageTable*,uint64_t);
}
namespace ker::mod::mm::phys {void page_free(void*);}
namespace host_fork_alloc {auto finish(int64_t,uint64_t,const char*)->uint64_t;void adopt_task(ker::mod::sched::task::Task*);void adopt_thread(ker::mod::sched::task::Task*);void adopt_percpu(void*);}
namespace host_fork_public {uint64_t finish(int64_t,uint64_t,const char*);void vm_success(ker::mod::sched::task::Task*);}
namespace ker::syscall::process {
void release_cloned_fd_table_refs(ker::mod::sched::task::Task*);
namespace child_events {void abort_publication(ker::mod::sched::task::Task&);}
namespace detail {
"""
        declarations = self.body("enum class ForkVmFailurePoint :")
        translator = self.body("auto public_fork_vm_error(")
        declaration = """
auto prepare_fork_vm_snapshot(ker::mod::sched::task::Task&,ker::mod::sched::task::Task&,
 const ker::mod::sys::SyscallPreparationScope&,ker::mod::mm::numa::AllocationContext*,
 ker::mod::mm::numa::AllocationResult*,ForkVmFailurePoint*)->int;
}
"""
        start = self.fork.index("    // Preparation and every unpublished VM rollback remain ordinary.")
        stop = self.fork.index("    (void)sched::resource::observe_address_space(*child);", start)
        block = self.fragment("ordinary VM caller and rollback", self.fork[start:stop])
        caller = """
auto host_public_vm_stage(ker::mod::sched::task::Task*parent,ker::mod::sched::task::Task*child,
 bool EAGER_COPY,ker::mod::mm::numa::AllocationContext&fork_placement,
 ker::mod::mm::numa::AllocationResult&fork_page,uint64_t KERNEL_STACK_BASE)->uint64_t {
 using namespace ker::mod;
 auto finish_fork=[](int64_t status,uint64_t bytes,const char*stage){return host_fork_public::finish(status,bytes,stage);};
""" + block + "host_fork_public::vm_success(child);return 0;\n}\n"
        anchors = [
            ("thread", "if (child_thread == nullptr) {"),
            ("tcb", "if (!child->pid_namespace()->is_host() &&"),
            ("fd", "if (!clone_fd_table_shared_checked(parent, child)) {"),
            ("publication", "if (!child_events::begin_publication(*parent, *child)) {"),
            ("scheduler", "if (!PUBLICATION_REF || !process_reservation.attach(*child) || !sched::post_task_balanced_inherited(child, inherited_placement)) {"),
        ]
        later = []
        for name, anchor in anchors:
            whole = self.owner.definition(self.fork, anchor)
            body = self.fragment(name + " existing failure cleanup", whole[whole.index("{") + 1 : -1])
            later.append("""
auto host_public_cleanup_""" + name + """(ker::mod::sched::task::Task*parent,ker::mod::sched::task::Task*child,
 uint64_t KERNEL_STACK_BASE,bool PUBLICATION_REF)->uint64_t {
 using namespace ker::mod;
 auto finish_fork=[](int64_t status,uint64_t bytes,const char*stage){return host_fork_public::finish(status,bytes,stage);};
""" + body + "}\n")
        return prelude + declarations + translator + declaration + caller + "\n".join(later) + self.allocations_source() + "}\n"

    def controls(self, source):
        vectors = [
            ("public-bool-errno", "return -ENOMEM;", "return status;", "PublicForkError.CompletePhaseStatusTable"),
            ("public-bypass-translation", "detail::public_fork_vm_error(point, FORK_VM_ERROR)", "FORK_VM_ERROR", "PublicForkWiring.NativeLazyAndGlobalRegistryKeepDifferentPublicErrors"),
            ("public-publishes-refusal", "if (FORK_VM_ERROR != 0)", "if (false)", "PublicForkWiring.NativeLazyAndGlobalRegistryKeepDifferentPublicErrors"),
            ("public-missing-root-rollback", 'mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-vm-snapshot-fail");', "/* actual caller root rollback omitted */", "PublicForkWiring.LateMixedRefusalRunsEveryAdoptedPrefixCleanup"),
            ("public-failure-if-off", "sys::SyscallPreparationScope rollback(*parent);", "/* ordinary cleanup omitted */", "PublicForkWiring.ThreadFailureCleansOrdinarily"),
        ]
        for label, old, new, test in vectors:
            assert source.count(old) >= 1, label
            yield label, source.replace(old, new, 1), test
        for name,spelling,test in [
            ("task","sched::task::Task{}","AllocationTaskDenialSkipsInitialization"),
            ("name","char[NAME_LEN + 1]","AllocationNameDenialDropsOnlyEarlyPrefix"),
            ("thread","sched::threading::Thread()","AllocationThreadDenialDropsMixedVmPrefix"),
            ("percpu","cpu::PerCpu()","AllocationPerCpuDenialDropsMixedVmAndThreadPrefix")]:
            old="new (std::nothrow) "+spelling
            assert source.count(old)==1
            yield "allocation-plain-"+name,source.replace(old,"new "+spelling,1),"PublicForkWiring."+test
        for name,old,new,test in [
            ("name-task",'delete child;\n            return finish_fork(-ENOMEM, 0, "name");','/* task prefix lost */\n            return finish_fork(-ENOMEM, 0, "name");',"AllocationNameDenialDropsOnlyEarlyPrefix"),
            ("percpu-root",'mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-percpu-alloc-fail");','/* root prefix lost */',"AllocationPerCpuDenialDropsMixedVmAndThreadPrefix")]:
            assert source.count(old)==1
            yield "allocation-omits-"+name,source.replace(old,new,1),"PublicForkWiring."+test

    def allocations_source(self):
        """Real expression-to-refusal slices; successful continuation is explicit."""
        fork=self.fork
        fragments=[]
        bounds=[
            ("task", "    auto* child = new (std::nothrow) sched::task::Task{};", "    child->publish_memory_policy_snapshot(MEMORY_POLICY);"),
            ("name", "    if (parent->name != nullptr) {", "    child->pid = sched::task::get_next_pid();"),
            ("thread", "        auto* child_thread = new (std::nothrow) sched::threading::Thread();", "        sched::threading::record_stack_usage(*child);"),
            ("percpu", "    auto* per_cpu = new (std::nothrow) cpu::PerCpu();", "    // Resume from the immutable syscall-entry state"),
        ]
        for name,start,stop in bounds:
            a=fork.index(start);b=fork.index(stop,a)
            fragment=self.fragment("allocation "+name,fork[a:b])
            child_arg="" if name=="task" else "ker::mod::sched::task::Task*child,"
            success={"task":"host_fork_alloc::adopt_task(child);", "name":"", "thread":"host_fork_alloc::adopt_thread(child);", "percpu":"host_fork_alloc::adopt_percpu(per_cpu);"}[name]
            fragments.append("""
auto host_allocation_"""+name+"""(ker::mod::sched::task::Task*parent,"""+child_arg+"""
 uint64_t KERNEL_STACK_BASE,uint64_t KERNEL_RSP)->uint64_t {
 using namespace ker::mod;
 auto finish_fork=[](int64_t status,uint64_t bytes,const char*stage){return host_fork_alloc::finish(status,bytes,stage);};
"""+fragment+success+"return 0;\n}\n")
        return "\n".join(fragments)
