"""Compose managed split, real sibling staging and actual unmap on existing ports.

The AS recount is an explicit observation endpoint; it checks destructor order,
not quota enforcement. Physical/ref/TLB endpoints retain the existing limits.
"""
from pathlib import Path

from fork_vm_coordinator_fixture import Builder as ForkBuilder


class Builder(ForkBuilder):
    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        self.case_path = support / "managed_anonymous_remove_cases.inc"
        self.inputs.extend([Path(__file__), self.case_path])
        self.test_count += self.case_path.read_text().count("TEST_F(ManagedAnonymousRemove,")
        for name in ["platform/sched/scheduler.cpp", "platform/sched/threading.cpp",
                     "platform/sched/resource_as.hpp", "platform/sched/resource_as.cpp",
                     "platform/mm/paging.opt.cpp", "platform/perf/perf_events.hpp"]:
            path = kernel_file(name)
            self.inputs.append(path)
            self.sources[name] = path.read_text()

    def extend_context(self, overlay):
        super().extend_context(overlay)
        path = overlay / "platform/sched/task.hpp"
        text = path.read_text()
        old = "host_fork_vm::retain_task(); ++lifetime_refs; return true;"
        assert text.count(old) == 1
        text = text.replace(old, "if(host_managed_remove::refuse_task(this))return false; host_managed_remove::retain_task(); ++lifetime_refs; return true;")
        old = "host_fork_vm::release_task(); --lifetime_refs;"
        assert text.count(old) == 1
        text = text.replace(old, "host_managed_remove::release_task(lifetime_refs); --lifetime_refs;")
        text = text.replace("namespace host_fork_vm {", "namespace host_managed_remove {bool refuse_task(const void*);void retain_task();void release_task(unsigned);}\nnamespace host_fork_vm {", 1)
        path.write_text(text)
        path = overlay / "platform/sched/scheduler.hpp"
        path.write_text(path.read_text() + """
namespace ker::mod::sched {
using ActiveTaskPredicate=bool(*)(task::Task*,void*);
auto get_active_task_count()->uint32_t;
auto snapshot_active_task_lifetime_refs_if(ActiveTaskPredicate,void*,task::Task**,size_t)->size_t;
auto find_active_task_lifetime_ref_if(ActiveTaskPredicate,void*)->task::Task*;
}
""")
        path = overlay / "platform/sched/resource_as.hpp"
        path.write_text(self.sources["platform/sched/resource_as.hpp"])

    def additions(self):
        d = self.body
        out = ["""
#include <abi/callnums/vmem.h>
#include <platform/sched/resource_as.hpp>
namespace ker::mod::dbg {[[noreturn]] void panic_handler(const char*){std::abort();}} // Fatal invariant endpoint.
namespace host_managed_remove {
using Task=ker::mod::sched::task::Task;
bool enabled{}, invoking{};
unsigned registry_depth{}, observations{}, unmap_acks{}, unmap_drops{};
const void* refused_task{};
std::array<Task*,16> registry{};
uint32_t count{};
std::vector<std::pair<uint64_t,void*>> expected_unmaps;
bool refuse_task(const void* task){return enabled&&registry_depth&&task==refused_task;}
void retain_task(){
 if(!enabled||!registry_depth){host_fork_vm::retain_task();return;}
 EXPECT_EQ(registry_depth,1U);EXPECT_EQ(publication_depth,1U);EXPECT_EQ(cow_depth+pin_depth,0U);EXPECT_LE(root_readers,1U);
 ++host_fork_vm::retained_tasks;
}
void release_task(unsigned refs){
 if(!enabled||(!registry_depth&&!publication_depth)){host_fork_vm::release_task();return;}
 EXPECT_GT(refs,1U);EXPECT_EQ(cow_depth+pin_depth+root_readers,0U);++host_fork_vm::returned_tasks;
}
struct RegistryLock {
 auto lock_irqsave()->uint64_t{EXPECT_EQ(registry_depth,0U);++registry_depth;return host_fork_vm::lock_irq();}
 void unlock_irqrestore(uint64_t f){EXPECT_EQ(registry_depth,1U);host_fork_vm::unlock_irq(f);--registry_depth;}
};
void before_decrement(void* pointer){
 if(!enabled||!invoking)return;
 auto found=std::find_if(expected_unmaps.begin(),expected_unmaps.end(),[&](auto const& p){return p.second==pointer;});
 if(found==expected_unmaps.end())return;
 EXPECT_EQ(cow_depth+pin_depth,0U);EXPECT_GT(unmap_acks,unmap_drops);++unmap_drops;
}
void unmap_ack(ker::mod::mm::paging::PageTable*root,uint64_t address){
 EXPECT_EQ(root,runtime_root);EXPECT_EQ(publication_depth,1U);EXPECT_FALSE(host_range::interrupts);
 EXPECT_EQ(root_readers,1U);EXPECT_EQ(cow_depth+pin_depth,0U);
 ASSERT_LT(unmap_acks,expected_unmaps.size());EXPECT_EQ(address,expected_unmaps[unmap_acks].first);++unmap_acks;
}
}
namespace ker::mod::sched {
using host_managed_remove::count;
host_managed_remove::RegistryLock global_task_registry_lock;
uint32_t& active_task_count=host_managed_remove::count;
auto active_task_slot(uint32_t i)->task::Task*{return host_managed_remove::registry.at(i);}
""", d("platform/sched/scheduler.cpp", "auto get_active_task_count("),
               d("platform/sched/scheduler.cpp", "auto snapshot_active_task_lifetime_refs_if("),
               d("platform/sched/scheduler.cpp", "auto find_active_task_lifetime_ref_if("), "}\n",
               "namespace ker::mod::sched::threading {namespace {\n",
               d("platform/sched/threading.cpp", "auto checked_add("), "}\n",
               d("platform/sched/threading.cpp", "auto range_overlaps_initial_layout("), "}\n",
               "namespace ker::mod::mm::paging {\n",
               d("platform/mm/paging.opt.cpp", "auto create_page_table_entry("),
               d("platform/mm/paging.opt.cpp", "auto purge_page_table_entry("), "}\n",
               "namespace ker::mod::mm::virt {\nconstexpr bool OWNED_FRAME_TRACKING_ENABLED=false; // Explicit Normal telemetry port.\n",
               d("platform/mm/virt.opt.cpp", "auto find_anonymous_swap_mapping_locked(PageTable* pagemap, vaddr_t vaddr) -> AnonymousSwapMapping* {"),
               d("platform/mm/virt.opt.cpp", "void owned_frame_untrack_leaf(PageTable* pagemap, vaddr_t vaddr, const PageTableEntry& entry) {"),
               d("platform/mm/virt.opt.cpp", "void drop_present_leaf_ref("),
               "void selftest_require_mutable_pagemap_root(PageTable*r,const char*s,uint64_t){selftest_require_mutable_pagemap_root(r,s);}\n",
               d("platform/mm/virt.opt.cpp", "void unmap_page("), "}\n",
               "namespace ker::mod::perf {\n"]
        for name in ["WkiPerfScope", "WkiPerfPhase", "WkiPerfLocalVmemOp"]:
            out.append(d("platform/perf/perf_events.hpp", "enum class " + name + " :"))
        out.extend(["""
constexpr uint64_t PERF_ANY_SESSION=UINT64_MAX;
auto current_wki_capture_session(WkiPerfScope,uint8_t)->uint64_t{return PERF_ANY_SESSION;}
auto next_wki_trace_correlation()->uint32_t{std::abort();}
template<class...Args>void record_wki_event(Args...){std::abort();}
template<class...Args>void record_wki_summary(Args...){std::abort();}
}
namespace ker::mod::time {auto get_us()->uint64_t{std::abort();}}
namespace ker::mod::sched::resource {
auto observe_mapping_count(task::Task& subject,mm::paging::PageTable*,uint64_t)->bool{
 EXPECT_TRUE(host_range::interrupts);EXPECT_EQ(publication_depth,1U);EXPECT_EQ(root_readers,0U);
 EXPECT_EQ(subject.usercopy_pagemap_accesses.load(),0U);EXPECT_EQ(cow_depth+pin_depth+backend().preempt_depth,0U);
 ++host_managed_remove::observations;return true;
}
""", d("platform/sched/resource_as.cpp", "auto AddressSpaceObservation::observe("),
                    d("platform/sched/resource_as.cpp", "AddressSpaceObservation::~AddressSpaceObservation()"), "}\n",
                    "namespace ker::syscall::vmem {\nusing ker::mod::sched::get_current_task;\n",
                    "using LazyVmemRange=ker::mod::sched::task::LazyVmemRange;using LazyVmemKind=ker::mod::sched::task::LazyVmemKind;using LazyVmemRangeVec=ker::mod::sched::task::LazyVmemRangeVec;\n",
                    "constexpr uint64_t USER_SPACE_START=0x400000;\nvoid advance_mmap_cursor(ker::mod::sched::task::Task*,uint64_t,uint64_t);\n",
                    d("syscalls_impl/vmem/sys_vmem.cpp", "struct KernelManagedLayoutOverlapQuery {"),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "auto task_has_overlapping_kernel_managed_layout("),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "auto overlaps_kernel_managed_user_layout_locked("),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "auto slice_lazy_vmem_range("),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "void release_lazy_vmem_range_file("),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "class SharedLazyVmemUpdate {"),
                    "namespace detail {\n",
                    d("syscalls_impl/vmem/sys_vmem.cpp", "auto managed_range_error("),
                    d("syscalls_impl/vmem/sys_vmem.cpp", "auto remove_current_anonymous_range("), "}\n}\n"])
        return "\n".join(out)

    def compose(self, fixture, prelude, context, support, *rest):
        anchor = "auto page_ref_dec(void* pointer) -> uint32_t {"
        assert fixture.count(anchor) == 1
        fixture = fixture.replace(anchor, anchor + "\n    host_managed_remove::before_decrement(pointer);", 1)
        anchor = "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {"
        assert prelude.count(anchor) == 1
        prelude = prelude.replace(anchor, anchor + "\n    if(host_managed_remove::enabled&&host_managed_remove::invoking&&!full){host_managed_remove::unmap_ack(root,va);return;}", 1)
        anchor = "auto acquire_user_pagemap_access(PageTable* root, bool, size_t& index) -> bool {"
        assert prelude.count(anchor) == 1
        prelude = prelude.replace(anchor, anchor + "\n    if(host_managed_remove::enabled&&host_managed_remove::invoking&&root_readers){ADD_FAILURE()<<\"nested removal reader\";return false;}", 1)
        anchor = "    static void warn(const char*, Args...) {}"
        assert support.count(anchor) == 1
        support = support.replace(anchor, anchor + "\n    template<class...Args>static void critical(const char*,Args...){ADD_FAILURE()<<\"unexpected unmap fatal root port\";}", 1)
        declaration = """
#include <cstdint>
namespace ker::mod::mm::paging {struct PageTable;}
namespace host_managed_remove {extern bool enabled,invoking;void before_decrement(void*);void unmap_ack(ker::mod::mm::paging::PageTable*,uint64_t);}
"""
        return declaration + super().compose(fixture, prelude, context, support, *rest) + "\n" + self.additions() + self.case_path.read_text()

    def controls(self, source):
        for label, old, new, test in [
            ("remove-skips-unmap", "mm::virt::unmap_page(ROOT, page);", "(void)page;", "PartialRemovalPreservesUnrequestedFramesAndSiblingRows"),
            ("remove-double-match", "if (!metadata.commit(admission, actor, start, end))", "if (!metadata.matches(actor) || !metadata.commit(admission, actor, start, end))", "PartialRemovalPreservesUnrequestedFramesAndSiblingRows"),
            ("remove-omits-private-profile", "status = metadata.stage_anonymous_remove(start, end);", "status = metadata.stage(SharedLazyVmemUpdate::Change::REMOVE,start,end);", "SharedAndFileLookingAnonymousRowsRefuseBeforeWrites"),
            ("remove-omits-legacy-owner", "if (legacy_overlap) return -EOPNOTSUPP;", "if (false) return -EOPNOTSUPP;", "IndependentLegacyAndTypedOwnersRefuseBeforeSplit"),
            ("remove-nests-query-reader", "if (!metadata.commit(admission, actor, start, end))", "fm::FileMappingBinding* extra=nullptr;bool exact_extra=false;(void)mm::virt::query_current_file_binding(actor,start,end,extra,exact_extra);if (!metadata.commit(admission, actor, start, end))", "PartialRemovalPreservesUnrequestedFramesAndSiblingRows"),
        ]:
            assert source.count(old) == 1, label
            yield label, source.replace(old, new), "ManagedAnonymousRemove." + test
        actual = self.body("platform/mm/virt.opt.cpp", "void unmap_page(")
        old = "    flush_pagemap_after_update(page_table, vaddr, false);\n    drop_present_leaf_ref(OLD_ENTRY);"
        assert actual.count(old) == 1
        bad = actual.replace(old, "    drop_present_leaf_ref(OLD_ENTRY);\n    flush_pagemap_after_update(page_table, vaddr, false);")
        assert source.count(actual.strip()) == 1
        yield "remove-drops-before-ack", source.replace(actual.strip(), bad.strip()), "ManagedAnonymousRemove.PartialRemovalPreservesUnrequestedFramesAndSiblingRows"
