"""Extend the maintained managed-huge fixture with actual fork metadata bodies.

No owner, survey, original-exclusive copier or coordinator is reimplemented.
Host Task/IRQ/VFS/physical/PUB and deferred callback ports remain explicit seams.
"""

from pathlib import Path


class Builder:
    test_count = 20

    def __init__(self, kernel_file, support, definition):
        self.path = kernel_file
        self.support = support
        self.definition = definition
        self.inputs = [Path(__file__), support / "fork_vm_coordinator_cases.inc"]
        self.sources = {}
        self.extractions = []
        for name in [
            "platform/sched/task.hpp",
            "platform/sched/task.cpp",
            "platform/sched/lazy_vmem.hpp",
            "platform/sched/lazy_vmem.cpp",
            "platform/mm/file_mapping_lifetime.hpp",
            "platform/mm/file_mapping_lifetime.cpp",
            "platform/mm/virt.hpp",
            "platform/mm/virt.opt.cpp",
            "syscalls_impl/vmem/sys_vmem.hpp",
            "syscalls_impl/vmem/sys_vmem.cpp",
            "syscalls_impl/shm/shm.hpp",
            "syscalls_impl/shm/shm.cpp",
            "syscalls_impl/process/process.cpp",
            "vfs/core.cpp",
            "util/smallvec.hpp",
        ]:
            p = kernel_file(name)
            self.inputs.append(p)
            self.sources[name] = p.read_text()
        from fork_public_wiring_fixture import PublicWiring

        self.public = PublicWiring(self)
        self.inputs.extend(self.public.inputs)
        self.test_count += self.public.test_count

    def body(self, file, anchor):
        import hashlib

        value = self.definition(self.sources[file], anchor)
        self.extractions.append(
            dict(
                file=file,
                anchor=anchor,
                bytes=len(value.encode()),
                sha256=hashlib.sha256(value.encode()).hexdigest(),
            )
        )
        return value + "\n"

    def span(self, file, start, stop):
        import hashlib

        source = self.sources[file]
        assert source.count(start) == source.count(stop) == 1, (file, start, stop)
        value = source[source.index(start) : source.index(stop)]
        self.extractions.append(
            dict(
                file=file,
                start=start,
                stop=stop,
                bytes=len(value.encode()),
                sha256=hashlib.sha256(value.encode()).hexdigest(),
            )
        )
        return value + "\n"

    def extend_context(self, overlay):
        task = overlay / "platform/sched/task.hpp"
        source = task.read_text()
        source = source.replace(
            "#include <platform/mm/paging.hpp>",
            """#include <platform/mm/paging.hpp>
#include <util/smallvec.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
namespace ker::vfs { struct File; }
namespace host_fork_vm {
uint64_t lock_irq(unsigned=0); void unlock_irq(uint64_t); void retain_task(); void release_task();
void enter_legacy();
struct Spin { unsigned tag{};auto lock_irqsave()->uint64_t{return lock_irq(tag);} void unlock_irqrestore(uint64_t f){unlock_irq(f);} };
}
""",
        )
        ranges = self.span(
            "platform/sched/task.hpp",
            "enum class LazyVmemKind :",
            "class LazyVmemClone;",
        )
        source = source.replace(
            "struct LazyVmemRange;", ranges + "class LazyVmemClone;\n"
        )
        source = source.replace(
            " mm::paging::PageTable* pagemap{};",
            """ mm::paging::PageTable* pagemap{};
 uint64_t pid{1}; bool has_run{}, is_thread{}; Task* owner{};
 host_fork_vm::Spin lazy_vmem_lock; LazyVmemRangeVec lazy_vmem_ranges;
 std::atomic<void*> native_file_mapping_retirement{};
 unsigned lifetime_refs{1};
 auto try_acquire_lifetime_ref()->bool {host_fork_vm::retain_task(); ++lifetime_refs; return true;}
 void release(){host_fork_vm::release_task(); --lifetime_refs;}
 uint64_t entry{}, program_header_addr{}, elf_header_addr{}, image_load_base{}, image_vaddr_start{}, image_vaddr_end{};
 uint64_t program_header_count{}, program_header_ent_size{}, interp_base{}, interp_vaddr_start{}, interp_vaddr_end{}, at_random_addr{};
 std::atomic<uint64_t> mmap_next{};
""",
        )
        source += "\nnamespace ker::mod::sched::task { inline auto process_pid(const Task& t)->uint64_t{return t.owner?t.owner->pid:t.pid;} }\n"
        task.write_text(source)
        p = overlay / "platform/sys/syscall_preparation.hpp"
        s = p.read_text()
        s = s.replace(
            "host_range::interrupts = false;",
            "host_range::interrupts = false; host_fork_vm::enter_legacy();",
            1,
        )
        p.write_text(s)
        self.public.extend_context(overlay)
        # Original SmallVec implementation, with only its allocator declaration
        # and fatal provider headers replaced by ordinary Host endpoints.
        for name, content in [
            ("util/smallvec.hpp", self.sources["util/smallvec.hpp"]),
            ("platform/mm/dyn/kmalloc.hpp", "#pragma once\n"),
            (
                "util/hcf.hpp",
                "#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf(){std::abort();}\n",
            ),
        ]:
            p = overlay / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
        header = self.sources["platform/mm/file_mapping_lifetime.hpp"]
        # The types and methods are exact. Access control alone is relaxed for
        # genuine fixture seed/count inspection; no production layout is copied.
        header = header.replace("   private:", "   public:")
        p = overlay / "platform/mm/file_mapping_lifetime.hpp"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(header)
        (overlay / "platform/sched/lazy_vmem.hpp").write_text(
            self.sources["platform/sched/lazy_vmem.hpp"]
        )
        p = overlay / "syscalls_impl/vmem/sys_vmem.hpp"
        s = p.read_text()
        s += (
            "\nnamespace ker::syscall::vmem {\n"
            + self.body(
                "syscalls_impl/vmem/sys_vmem.hpp", "class PublicFileForkPreparation {"
            )
            + self.body(
                "syscalls_impl/vmem/sys_vmem.hpp", "class LegacyFileForkPreparation {"
            )
            + "}\n"
        )
        p.write_text(s)

    def components(self):
        d = self.body
        out = [
            """
#include <climits>
#include <platform/sched/lazy_vmem.hpp>
namespace ker::mod::mm::virt {
auto capture_user_root_incarnation(paging::PageTable*,uint64_t&)noexcept->bool;
auto get_kernel_pagemap()->paging::PageTable*;
auto query_current_file_binding(sched::task::Task&,uint64_t,uint64_t,file_mapping::FileMappingBinding*&,bool&)->bool;
auto attach_unpublished_file_binding(file_mapping::FileMappingBinding*&,paging::PageTable*,uint64_t,uint64_t,uint64_t)->bool;
}
namespace host_fork_vm {
unsigned locks{}, retained_tasks{}, returned_tasks{}, vfs_puts{}, binding_reserves{}, binding_returns{};
unsigned registry_tries{}, registry_fail_try{};
bool enabled{}, fail_attach{}, fail_shm{}, fail_lazy{}, fail_binding_reserve{};
unsigned child_attaches{}, queries{};
unsigned pubs{};void(*on_pub)(){};
void(*on_pin)(){},(*on_shm)(){},(*on_legacy)(){};
void publication_acquire(){++pubs;if(on_pub)on_pub();}
void pin_acquire(){if(enabled&&on_pin)on_pin();}
void enter_legacy(){if(enabled&&on_legacy)on_legacy();}
auto ordinary()->bool {return host_range::interrupts && !publication_depth && !host_range::exclusive && !root_readers && !cow_depth && !pin_depth && !backend().preempt_depth && !locks;}
uint64_t lock_irq(unsigned tag){host_fork_public::lazy_lock(tag);if(enabled&&tag==1&&on_shm)on_shm();auto f=host_range::interrupts;host_range::interrupts=false;++locks;return f;}
void unlock_irq(uint64_t f){EXPECT_GT(locks,0U);--locks;host_range::interrupts=f;}
void retain_task(){EXPECT_TRUE(ordinary());++retained_tasks;}
void release_task(){EXPECT_TRUE(ordinary());++returned_tasks;}
struct RegistryMutex {
 bool held{};
 void lock(){EXPECT_FALSE(held);held=true;}
 bool try_lock(){++registry_tries;if(enabled && registry_fail_try==registry_tries)return false;lock();return true;}
 void unlock(){EXPECT_TRUE(held);held=false;}
};
}
namespace ker::vfs {struct File {std::atomic<int> refcount{1};};
""",
            d("vfs/core.cpp", "auto vfs_try_retain_file("),
            """
void vfs_put_file(File* f){if(!f)return;EXPECT_TRUE(host_fork_vm::ordinary());EXPECT_GT(f->refcount.fetch_sub(1),0);++host_fork_vm::vfs_puts;}
void vfs_retain_file(File*){std::abort();} // Whole-child unregister never splits a registry row.
auto vfs_put_file_result(File*f)->int{vfs_put_file(f);return 0;}
namespace tmpfs {class TmpfsReadSharedBacking{};class TmpfsDemandSharedBacking{};}
}
namespace ker::mod::sys::usercopy {constexpr uint64_t USER_ADDR_LIMIT=uint64_t{1}<<47;}
namespace ker::mod::mm::file_mapping {
struct LifetimeRetirementTicket {LifetimeRetirementRun run{};void* arg{};bool pending{};};
namespace {
host_fork_vm::Spin owner_count_lock;
""",
        ]
        out += [d("platform/mm/file_mapping_lifetime.cpp", "class OwnerCountGuard {")]
        for anchor in [
            "auto counts_valid(",
            "auto acquire_owner(",
            "auto release_owner(",
            "auto reclassify_owner(",
            "auto same_owners(",
        ]:
            out += [
                (
                    "template<typename Counts>\n"
                    if anchor in ["auto counts_valid(", "auto same_owners("]
                    else "template<typename Counts,typename Role>\n"
                )
                + d("platform/mm/file_mapping_lifetime.cpp", anchor)
            ]
        out += [
            """}
std::array<LifetimeRetirementTicket,256> host_tickets;
auto host_reserve(void*,LifetimeRetirementRun run,void*arg)noexcept->LifetimeRetirementTicket*{
 EXPECT_TRUE(host_fork_vm::ordinary());++host_fork_vm::binding_reserves;
 if(host_fork_vm::fail_binding_reserve)return nullptr;
 for(auto&t:host_tickets)if(!t.run){t={run,arg,false};return &t;}return nullptr;
}
void host_submit(void*,LifetimeRetirementTicket&t)noexcept{EXPECT_FALSE(t.pending);t.pending=true;}
bool host_observe(void*,LifetimeRetirementTicket&,LifetimeRetirementRun,void*,RetirementCompletion&)noexcept{return false;}
LifetimeRetirementPort host_port{nullptr,host_reserve,host_submit,host_observe,nullptr};
void host_drain(){EXPECT_TRUE(host_fork_vm::ordinary());for(bool again=true;again;){again=false;for(auto&t:host_tickets)if(t.pending){auto run=t.run;auto*arg=t.arg;t={};(void)run(arg);++host_fork_vm::binding_returns;again=true;}}}
""",
            d(
                "platform/mm/file_mapping_lifetime.cpp",
                "FileMappingLifetime::FileMappingLifetime(",
            ),
            "FileMappingLifetime::~FileMappingLifetime() = default;\n",
        ]
        for anchor in [
            "auto FileMappingLifetime::try_acquire(",
            "void FileMappingLifetime::release(",
            "auto FileMappingLifetime::reclassify(",
            "auto FileMappingLifetime::owner_counts(",
            "auto FileMappingLifetime::finish(",
            "FileMappingBinding::FileMappingBinding(",
            "auto FileMappingBinding::prepare_unbound(",
            "auto FileMappingBinding::configure_unpublished(",
            "auto FileMappingBinding::try_acquire(",
            "void FileMappingBinding::release(",
            "auto FileMappingBinding::reclassify(",
            "auto FileMappingBinding::owner_counts(",
            "auto FileMappingBinding::finish(",
        ]:
            out += [d("platform/mm/file_mapping_lifetime.cpp", anchor)]
        out += [
            self.span(
                "platform/mm/file_mapping_lifetime.cpp",
                "FileMappingTlbRetirement::~FileMappingTlbRetirement()",
                "#ifdef WOS_SELFTEST\nauto FileMappingIndex::selftest_count_all_locked",
            ),
            "}\n",
        ]
        out += [
            "namespace ker::mod::sched::task {\n",
            d("platform/sched/task.cpp", "void release_lazy_vmem_ranges(Task& task)"),
            "}\n",
        ]
        # Ordinary lazy clone, TMPFS fork binding, global File and SHM algorithms.
        out += [
            self.span(
                "platform/sched/lazy_vmem.cpp",
                "namespace ker::mod::sched::task {",
                "}  // namespace ker::mod::sched::task",
            ),
            "}\n",
        ]
        out += [
            "namespace ker::mod::mm::virt {\n",
            d("platform/mm/virt.opt.cpp", "auto capture_user_root_incarnation("),
            d("platform/mm/virt.opt.cpp", "auto current_process_root("),
            d("platform/mm/virt.opt.cpp", "class DemandRootAccess {"),
            d("platform/mm/virt.opt.cpp", "auto query_current_file_binding("),
            d("platform/mm/virt.opt.cpp", "auto attach_unpublished_file_binding("),
            "}\n",
        ]
        out += [
            "namespace ker::syscall::vmem {\n",
            d("syscalls_impl/vmem/sys_vmem.cpp", "struct FileMmapRange {"),
            """ker::util::SmallVec<FileMmapRange,32>g_file_mmap_ranges;host_fork_vm::RegistryMutex g_file_mmap_ranges_lock;struct NativeMappingEntry{ker::mod::mm::paging::PageTable*root{};NativeMappingEntry*registry_next{};bool registered{};ker::mod::mm::file_mapping::FileMappingLifetime*lifetime{};};NativeMappingEntry*g_native_file_mmap_ranges{};
""",
            self.span(
                "syscalls_impl/vmem/sys_vmem.cpp",
                "namespace {\nauto prepared_fork_actor(",
                "auto validate_native_unpublished_mapping_owners(",
            ),
            d("syscalls_impl/vmem/sys_vmem.cpp", "auto ranges_overlap("),
            d("syscalls_impl/vmem/sys_vmem.cpp", "auto checked_range_end("),
            d("syscalls_impl/vmem/sys_vmem.cpp", "auto file_mmap_range_end("),
            d(
                "syscalls_impl/vmem/sys_vmem.cpp",
                "auto unregister_one_file_mmap_overlap(",
            ),
            d("syscalls_impl/vmem/sys_vmem.cpp", "auto unregister_file_mmap_range("),
            "}\n",
        ]
        out += [
            """namespace ker::mod::security{struct IpcOwnership{};}namespace ker::mod::sched::ipc{struct NamespaceRef{};}namespace ker::abi::shm{constexpr int IPC_PRIVATE=0;}
namespace ker::syscall::shm {using ker::mod::sched::ipc::NamespaceRef;constexpr uint64_t USER_SPACE_START=0x400000,USER_SPACE_END=0x7fffffffffff;
""",
            d("syscalls_impl/shm/shm.cpp", "struct ShmSegment {"),
            d("syscalls_impl/shm/shm.cpp", "struct ShmAttachment {"),
            """host_fork_vm::Spin g_lock{1};std::array<ShmSegment,64>g_segments{};std::array<ShmAttachment,256>g_attachments{};
""",
            d("syscalls_impl/shm/shm.cpp", "auto process_pid_for_task("),
            d("syscalls_impl/shm/shm.cpp", "auto find_retained_segment_by_id("),
            d("syscalls_impl/shm/shm.cpp", "auto find_free_attachment("),
            d("syscalls_impl/shm/shm.hpp", "class ShmForkPreparation {"),
            self.span(
                "syscalls_impl/shm/shm.cpp",
                "namespace {\nauto prepared_shm_actor(",
                "void shm_cleanup_for_task(",
            ),
            d("syscalls_impl/shm/shm.cpp", "auto backing_page("),
            d("syscalls_impl/shm/shm.cpp", "void release_segment("),
            d("syscalls_impl/shm/shm.cpp", "void maybe_release_destroyed_segment("),
            d("syscalls_impl/shm/shm.cpp", "void detach_attachments_for_pid_locked("),
            d("syscalls_impl/shm/shm.cpp", "void shm_cleanup_for_task("),
            "}\n",
        ]
        out += [
            "namespace ker::mod::mm::virt {namespace detail{struct PreparedSwapForkAccess;}\n",
            d("platform/mm/virt.hpp", "class PreparedAnonymousSwapFork {"),
            """namespace swap{struct SwapSlot{uint64_t id{};};unsigned frees{};auto free_slot(SwapSlot)->int{EXPECT_TRUE(host_fork_vm::ordinary());++frees;return 0;}}
""",
        ]
        for anchor in [
            "enum class AnonymousSwapRuntimeState",
            "struct AnonymousSwapObject {",
            "enum class AnonymousPageinAllocation",
            "struct AnonymousSwapMapping {",
        ]:
            out += [d("platform/mm/virt.opt.cpp", anchor)]
        out += [
            """AnonymousSwapMapping*anonymous_swap_mappings{};struct{std::atomic<uint64_t>swapped_pages{};}anonymous_swap_stats;
auto get_kernel_pagemap()->PageTable*{return kernel_pagemap;}
"""
        ]
        for anchor in [
            "auto is_reserved_leaf(const PageTableEntry& entry) -> bool {",
            "auto leaf_entry(PageTable* root, vaddr_t vaddr) -> PageTableEntry* {",
            "auto valid_anonymous_placement(",
            "void anonymous_swap_add(",
            "void insert_anonymous_swap_mapping_locked(",
            "void remove_anonymous_swap_mapping_locked(",
            "auto detach_anonymous_swap_object_locked(",
            "void release_anonymous_swap_object(",
            "auto anonymous_swap_marker_raw(",
        ]:
            out += [d("platform/mm/virt.opt.cpp", anchor)]
        out += [
            self.span(
                "platform/mm/virt.opt.cpp",
                "struct PreparedAnonymousSwapFork::Storage",
                "\nnamespace {\nauto clone_anonymous_swap_mappings",
            ),
            d("platform/mm/virt.opt.cpp", "void purge_anonymous_swap_mappings("),
        ]
        header = self.sources["platform/mm/virt.hpp"]
        a = header.index(
            "[[nodiscard]] auto deep_copy_current_user_pagemap_prepared(sched::task::Task&,"
        )
        out += [header[a : header.index(";", a) + 1], "}\n"]
        return "\n".join(out)

    def compose(
        self,
        fixture,
        prelude,
        context,
        support,
        user_pin,
        exclusive,
        epoch,
        index,
        resolver,
        private,
        copier,
        commit,
        adapters,
        cases,
        range_cases,
        fork_cases,
    ):
        # Replace the previously explicit *unreachable* prepared-swap port by
        # actual metadata. Existing54 tests still use the null-plan overload.
        support = support[: support.index("// Existing 54 positive vectors")]
        support = support.replace(
            "if (!fork_mode) return;",
            "if (!fork_mode || !host_range::exclusive) return;",
        )
        support = support.replace(
            "void page_free(void* pointer) {",
            "void page_free(void* pointer) {\n    if(host_fork_alloc::free_stack(pointer)||host_fork_public::free_stack(pointer))return;",
        )
        start = prelude.index("struct HostBindingIndex {")
        stop = prelude.index("// Actual PageTable/PageTableEntry", start)
        prelude = (
            prelude[:start]
            + """// Existing54 pin vectors retain their explicit binding-match refusal port.
struct HostBindingIndex: file_mapping::FileMappingIndex {
 auto matches_pin_locked(PageTable*r,uint64_t serial,uint64_t address,const file_mapping::FileMappingBinding*b)const noexcept->bool{
 return binding_matches && file_mapping::FileMappingIndex::matches_pin_locked(r,serial,address,b);
 }
}file_mapping_index;
"""
            + prelude[stop:]
        )
        # Extend the existing nothrow endpoint once, retaining its old plan
        # failure vectors in the new fixture's allocation ledger.
        original_new = self.definition(
            fixture,
            "void* operator new(std::size_t bytes, const std::nothrow_t&) noexcept {",
        )
        fixture = fixture.replace(
            original_new,
            "/* The extended nothrow endpoint is defined with the coordinator cases below. */",
        )
        # New actual TMPFS query uses its real short PreemptGuard outside the
        # copier. Existing physical hook distinguishes that from COW entry.
        prelude = prelude.replace(
            "void Mutex::lock() {",
            "void Mutex::lock() {\n    if(host_fork_vm::enabled){ EXPECT_TRUE(host_range::interrupts);host_fork_vm::publication_acquire();}",
        )
        prelude = prelude.replace(
            "void Mutex::clear_owner() {",
            "void Mutex::clear_owner() {\n    if(host_fork_vm::enabled) EXPECT_TRUE(host_range::interrupts);",
        )
        prelude = prelude.replace(
            "auto lock_irqsave() -> uint64_t {\n        EXPECT_EQ(depth, 0U);",
            "auto lock_irqsave() -> uint64_t {\n        if (&depth==&pin_depth) host_fork_vm::pin_acquire();\n        EXPECT_EQ(depth, 0U);",
        )
        # The namespace is declared before the fixture's header includes.
        declaration = "#include <cstddef>\nnamespace host_fork_alloc {bool refuse(std::size_t,bool);void* null_result();void note_delete(void*);bool free_stack(void*);void guard();}\nnamespace host_fork_vm {extern bool enabled;void publication_acquire();void pin_acquire();}\n"
        coordinator = self.body(
            "syscalls_impl/process/process.cpp", "auto prepare_fork_vm_snapshot("
        )
        return "\n".join(
            [
                declaration,
                fixture,
                prelude,
                context,
                support,
                "namespace ker::mod::mm::virt {",
                user_pin,
                exclusive,
                epoch,
                index,
                resolver,
                private,
                "}",
                self.components(),
                self.public.components(),
                "namespace ker::mod::mm::virt {",
                copier,
                commit,
                "} namespace ker::mod::mm::huge::detail {",
                adapters,
                "}",
                "namespace ker::syscall::process::detail {",
                self.public.body("enum class ForkVmFailurePoint :"),
                "auto public_fork_vm_error(ForkVmFailurePoint,int)->int;",
                coordinator,
                "}",
                "namespace ker::mod::mm::virt {",
                cases,
                range_cases,
                fork_cases,
                (self.support / "fork_vm_coordinator_cases.inc").read_text().replace(
                    "void drop(void* p) {", "void drop(void* p) {\n    host_fork_alloc::note_delete(p);\n    host_fork_public::note_delete(p);", 1
                ).replace("void* operator new(std::size_t n) {", "void* operator new(std::size_t n) {\n if(host_fork_alloc::refuse(n,false))return host_fork_alloc::null_result();", 1)
                 .replace("void* operator new[](std::size_t n) { return ::operator new(n); }", "void* operator new[](std::size_t n) { if(host_fork_alloc::refuse(n,true))return host_fork_alloc::null_result(); return ::operator new(n); }", 1)
                 .replace("void* operator new(std::size_t n, const std::nothrow_t&) noexcept {", "void* operator new(std::size_t n, const std::nothrow_t&) noexcept {\n if(host_fork_alloc::refuse(n,false))return nullptr;", 1)
                 .replace("noexcept { return host_fork_vm::take(n); }", "noexcept { if(host_fork_alloc::refuse(n,true))return nullptr; return host_fork_vm::take(n); }", 1),
                self.public.path.read_text(),
            ]
        )

    def controls(self, source):
        vectors = [
            (
                "coordinator-missing-swap",
                "eager_context, empty_result, swap);",
                "eager_context, empty_result);",
                "ForkVmCoordinator.SwappedAndBothFailedStatesTransferActualMarkers",
            ),
            (
                "coordinator-missing-tmpfs",
                "if (!tmpfs.attach(child)) return -EAGAIN;",
                "if (false) return -EAGAIN;",
                "ForkVmCoordinator.MixedFileMetadataKeepDistinctOwners",
            ),
            (
                "coordinator-missing-shm",
                "status = shm.commit(actor, child, preparation, publication);",
                "status = 0;",
                "ForkVmCoordinator.ShmWholeTableCommitAndLegacyRows",
            ),
            (
                "coordinator-double-match",
                "if (!lazy.matches(actor) || !tmpfs.matches(actor)) return -EAGAIN;",
                "if (!lazy.matches(actor) || !tmpfs.matches(actor) || !tmpfs.matches(actor)) return -EAGAIN;",
                "ForkVmCoordinator.MixedFileMetadataKeepDistinctOwners",
            ),
        ]
        for label, old, new, test in vectors:
            assert source.count(old) == 1, (label, source.count(old))
            yield label, source.replace(old, new), test
        old = (
            "sys::SyscallPreparationScope::LegacyInterval legacy_context(preparation);"
        )
        assert source.count(old) == 1
        nested = source.replace(
            old,
            old
            + "\nmm::huge::CurrentRangeSplitAdmission nested(actor,preparation,publication);",
        )
        yield "coordinator-nested-reader", nested, "ForkVmCoordinator.CountZeroUsesWholeSurveyAndOneOriginalExclusive"
        anchor = "ker::syscall::vmem::SharedVmemPublicationGuard publication;\n        if (!lazy.matches(actor)"
        assert source.count(anchor) == 1
        bad = source.replace(
            old, "/* Legacy moved too early by compiled control. */"
        ).replace(anchor, old + "\n        " + anchor)
        yield "coordinator-irq-before-pub", bad, "ForkVmCoordinator.CountZeroUsesWholeSurveyAndOneOriginalExclusive"
        old = "rollback();\n    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);"
        # New public rollback vectors may use the same assertion; mutate only
        # the original maintained coordinator case selected by this control.
        begin = source.index("TEST_F(ForkVmCoordinator, LaterLegacyCommitRefusalRetainsEveryAdoptedPrefix)")
        end = source.find("TEST_F(", begin + 1)
        if end < 0:
            end = len(source)
        case = source[begin:end]
        assert case.count(old) == 1
        yield (
            "coordinator-caller-omits-rollback",
            source[:begin] + case.replace(
                old,
                "/* Omitted caller rollback. */\n    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);",
            ) + source[end:],
            "ForkVmCoordinator.LaterLegacyCommitRefusalRetainsEveryAdoptedPrefix",
        )
