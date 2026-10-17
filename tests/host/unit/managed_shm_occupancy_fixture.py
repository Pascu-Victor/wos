"""Actual SHM attach/rollback/detach on the maintained MM fixture.

IPC/credential/AS and direct-publisher scheduling are explicit finite ports.
Actual PTE walkers, pin/unpin and unmap bodies retain the existing Host limits;
this is neither native namespace/quota enforcement nor SMP reservation proof.
"""
from pathlib import Path

from managed_anonymous_remove_fixture import Builder as AnonymousBuilder


class Builder(AnonymousBuilder):
    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        self.shm_cases = support / "managed_shm_occupancy_cases.inc"
        self.inputs.extend([Path(__file__), self.shm_cases])
        self.test_count += self.shm_cases.read_text().count("TEST_F(ManagedShmOccupancy,")
        for name in ["platform/security/authorization.hpp", "platform/sys/usercopy.hpp", "platform/sys/usercopy.cpp"]:
            path = kernel_file(name)
            self.inputs.append(path)
            self.sources[name] = path.read_text()

    def extend_context(self, overlay):
        super().extend_context(overlay)
        path = overlay / "platform/sched/task.hpp"
        source = path.read_text()
        anchor = "namespace ker::mod::sched::task {"
        assert source.count(anchor) == 2  # Task declaration and existing process_pid endpoint.
        declarations = """
#include <abi/callnums/shm.h>
namespace ker::mod::sched::ipc {
// Value-only namespace port; no native namespace retention/retirement claim.
struct NamespaceRef {
 uint64_t token{1}; bool retired{};
 auto closed() const -> bool {return retired;}
 auto is_host() const -> bool {return true;}
 auto generation() const -> uint64_t {return token;}
 auto operator==(const NamespaceRef&) const -> bool = default;
};
}
namespace ker::mod::security {
struct Snapshot {};
""" + self.body("platform/security/authorization.hpp", "struct IpcOwnership {") + """
}
namespace host_shm {
auto ipc_scope()->const ker::mod::sched::ipc::NamespaceRef*;
auto credentials()->ker::mod::security::Snapshot;
}
"""
        source = source.replace(anchor, declarations + anchor, 1)
        anchor = " mm::paging::PageTable* pagemap{};"
        assert source.count(anchor) == 1
        source = source.replace(anchor, anchor + """
 auto ipc_namespace()const->const ipc::NamespaceRef* {return host_shm::ipc_scope();}
 auto pid_namespace()const->const ipc::NamespaceRef* {return host_shm::ipc_scope();}
 auto credentials()const->security::Snapshot {return host_shm::credentials();}
""", 1)
        source += "\nnamespace ker::mod::sched::task {inline auto namespace_process_pid(const Task&t)->uint64_t{return process_pid(t);}}\n"
        path.write_text(source)

    def shm_additions(self):
        d = self.body
        out = [r'''
namespace host_shm {
using Task=ker::mod::sched::task::Task;
ker::mod::sched::ipc::NamespaceRef scope;
bool enabled{}, missing_namespace{}, allowed_access{true}, allowed_as{true};
unsigned access_calls{}, as_calls{}, maps{}, acks{}, drops{}, new_tables{};
uint32_t requested_access{};
void(*on_admit)(){},(*after_map)(uint64_t){};
void* pending_drop{};
std::vector<std::pair<uint64_t,void*>> published;
auto ipc_scope()->const ker::mod::sched::ipc::NamespaceRef* {return missing_namespace?nullptr:&scope;}
auto credentials()->ker::mod::security::Snapshot {
 EXPECT_EQ(publication_depth,1U);EXPECT_EQ(host_fork_vm::locks,0U);return {};
}
void ordinary() {EXPECT_TRUE(host_range::interrupts);EXPECT_EQ(publication_depth,1U);EXPECT_EQ(host_fork_vm::locks,0U);}
void before_decrement(void* page) {
 if(enabled&&pending_drop==page){EXPECT_GT(acks,drops);++drops;pending_drop=nullptr;}
}
void ack(ker::mod::mm::paging::PageTable*root,uint64_t address) {
 ordinary();EXPECT_EQ(root,runtime_root);EXPECT_EQ(cow_depth+pin_depth,0U);
 auto found=std::find_if(published.begin(),published.end(),[&](auto const&row){return row.first==address;});
 ASSERT_NE(found,published.end());EXPECT_EQ(pending_drop,nullptr);pending_drop=found->second;++acks;
}
}
namespace ker::mod::security {
auto may_access_ipc(const Snapshot&,const IpcOwnership&,uint32_t requested)->bool {
 EXPECT_EQ(publication_depth,1U);EXPECT_EQ(host_fork_vm::locks,1U);
 ++host_shm::access_calls;host_shm::requested_access=requested;return host_shm::allowed_access;
}
}
namespace ker::mod::sched::resource {
auto admit_address_space(task::Task*actor,mm::paging::PageTable*root,uint64_t added,uint64_t,uint64_t)->bool {
 host_shm::ordinary();EXPECT_EQ(actor,host_range::current);EXPECT_EQ(root,runtime_root);EXPECT_GT(added,0U);
 ++host_shm::as_calls;if(host_shm::on_admit)host_shm::on_admit();return host_shm::allowed_as;
}
}
namespace ker::mod::mm::phys {
// Lookup-hint caching is not modeled; the checked scalar backend owns the
// actual finite reference count, once. No native cached-hint claim.
auto page_ref_dec(void* page,PageLookupHint*)->uint32_t{return page_ref_dec(page);}
void page_ref_inc(void* page){EXPECT_TRUE(page_ref_try_inc(page,nullptr));}
}
namespace ker::mod::mm::virt {
''']
        for anchor in ["auto entry_at(const PageTable* table, size_t index) -> const PageTableEntry& {",
                       "auto present_mapping_entry(", "auto is_page_mapped(", "auto is_page_mapped_or_reserved(",
                       "auto snapshot_user_page_mapping_locked(", "auto try_pin_current_user_page_mapping("]:
            out.append(d("platform/mm/virt.opt.cpp", anchor))
        out.append(r'''
// No new fault-in or I/O scenario is supplied by this profile.
auto ensure_user_page_writable(sched::task::Task*,vaddr_t)->bool{std::abort();}
auto ensure_user_page_mapped(sched::task::Task*,vaddr_t)->bool{std::abort();}
''')
        for anchor in ["auto pin_user_page_common(", "auto pin_user_page_for_task(", "auto user_page_pin_still_mapped(", "void unpin_user_page("]:
            out.append(d("platform/mm/virt.opt.cpp", anchor))
        out.append(r'''
enum class PublicReadPrepareResult:uint8_t {NOT_TRACKED,READY,REFUSED};
auto prepare_tmpfs_read_shared_page(sched::task::Task&,uint64_t,bool)->PublicReadPrepareResult{std::abort();}
struct UserFutexIdentity;
// Checked finite 4K hardware publication endpoint. It never follows/writes PS
// or replaces an occupied PTE, even when an intentionally broken guard calls it.
void map_page(PageTable*root,const vaddr_t va,const paddr_t pa,const uint64_t flags) {
 host_shm::ordinary();EXPECT_EQ(root,runtime_root);
 auto*table=root;
 for(int level=4;level>1;--level){
  auto&entry=entry_at(table,index_of(va,level));
  if(entry.present&&entry.pagesize){ADD_FAILURE()<<"SHM reached an occupied PS mutator";return;}
  if(!entry.present){
   auto*next=static_cast<PageTable*>(std::aligned_alloc(PAGE,PAGE));if(!next)std::abort();std::memset(next,0,PAGE);
   table_live.push_back(next);++host_shm::new_tables;entry=pte_from_raw(reinterpret_cast<uint64_t>(next)|7);
  }
  table=reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame<<paging::PAGE_SHIFT));
 }
 auto&entry=entry_at(table,index_of(va,1));
 if(entry.present||is_reserved_leaf(entry)){ADD_FAILURE()<<"SHM replaced occupied4K leaf";return;}
 entry=pte_from_raw(pa|flags);++host_shm::maps;
 host_shm::published.push_back({va,reinterpret_cast<void*>(addr::get_virt_pointer(pa))});
 if(host_shm::after_map)host_shm::after_map(va);
}
}
namespace ker::mod::sys::usercopy {
class UserPageFrameRef;
''')
        out.append(d("platform/sys/usercopy.hpp", "class StableUserPage {"))
        for anchor in ["StableUserPage::~StableUserPage()", "auto StableUserPage::valid()", "auto StableUserPage::physical_address()",
                       "auto StableUserPage::still_mapped()", "void StableUserPage::reset()", "auto range_valid(", "auto pin_task_user_page("]:
            out.append(d("platform/sys/usercopy.cpp", anchor))
        out += ["auto mapped_physical_address(sched::task::Task&,uint64_t,bool=false)->uint64_t;\n",
                d("platform/sys/usercopy.cpp", "auto mapped_physical_address("), "}\nnamespace ker::syscall::shm {\n"]
        shm = self.sources["syscalls_impl/shm/shm.cpp"]
        out.append(next(line for line in shm.splitlines() if line.startswith("constexpr uint64_t SHM_SEARCH_START")))
        for anchor in ["auto to_errno(", "struct ShmPidIdentity {", "auto pid_identity_for_task(", "auto visible_pid(",
                       "auto backing_paddr(", "auto find_visible_segment_by_id(", "auto find_attachment(",
                       "struct SharedLayoutOverlapQuery {", "auto task_has_overlapping_initial_layout(",
                       "auto range_overlaps_shared_initial_layout(", "auto range_overlaps_lazy_vmem(", "auto range_is_free(",
                       "auto find_free_range(", "void abandon_attachment_publication(", "void rollback_attachment_pages(",
                       "auto shmat_impl(", "void detach_attachment_locked(", "auto shmdt_impl("]:
            out.append(d("syscalls_impl/shm/shm.cpp", anchor))
        out.append("}\n")
        return "\n".join(out)

    def compose(self, fixture, prelude, context, support, *rest):
        anchor = "auto page_ref_dec(void* pointer) -> uint32_t {"
        assert fixture.count(anchor) == 1
        fixture = fixture.replace(anchor, anchor + "\n    host_shm::before_decrement(pointer);", 1)
        anchor = "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {"
        assert prelude.count(anchor) == 1
        prelude = prelude.replace(anchor, anchor + "\n    if(host_shm::enabled&&!full){host_shm::ack(root,va);return;}", 1)
        source = super().compose(fixture, prelude, context, support, *rest)
        old = "namespace ker::mod::security{struct IpcOwnership{};}namespace ker::mod::sched::ipc{struct NamespaceRef{};}namespace ker::abi::shm{constexpr int IPC_PRIVATE=0;}"
        assert source.count(old) == 1
        source = source.replace(old, "// Finite namespace/security declarations are in the generated Task header.", 1)
        declarations = "#include <cstdint>\nnamespace ker::mod::mm::paging{struct PageTable;}\nnamespace host_shm {extern bool enabled;void before_decrement(void*);void ack(ker::mod::mm::paging::PageTable*,uint64_t);}\n"
        return declarations + source + self.shm_additions() + self.shm_cases.read_text()

    def controls(self, source):
        for address, name, case in [
            ("current", "shm-range-omits-ps", "RegisteredTwoMegabyteFirstInteriorLastAndCrossBoundaryAreOccupied"),
            ("PAGE_ADDR", "shm-final-omits-ps", "FinalPsGuardRollsBackMappedPrefixBeforeAbandoning"),
        ]:
            term = " ||\n            ker::mod::mm::virt::is_page_mapped(task->pagemap, " + address + ")"
            assert source.count(term) == 1, name
            yield name, source.replace(term, "", 1), "ManagedShmOccupancy." + case
