"""Actual ordinary removal after prepared demotion, on maintained Host ports.

The supplied snapshot/pwrite endpoints do not execute native usercopy/VFS/SMAP.
They check ordinary reader/IF/PUB boundaries. Real registry, lazy metadata,
geometry, split/ACK, unmap and scalar File retain bodies are extracted below.
"""
from pathlib import Path

from managed_anonymous_remove_fixture import Builder as AnonymousBuilder


class Builder(AnonymousBuilder):
    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        self.mixed_cases = support / "managed_mixed_geometry_cases.inc"
        self.inputs.extend([Path(__file__), self.mixed_cases])
        count = self.case_source().count("TEST_F(ManagedMixedGeometry,")
        if not 0 < count <= 24:
            raise ValueError("mixed geometry profile must add1..24 actual cases")
        self.test_count += count
        for name in ("platform/sys/usercopy.hpp", "platform/sys/usercopy.cpp", "platform/security/file_policy.hpp"):
            path = kernel_file(name)
            self.inputs.append(path)
            self.sources[name] = path.read_text()

    def case_source(self):
        # Protection has its own profile; old removal keeps its exact135 bodies.
        return self.mixed_cases.read_text().partition("// BEGIN MIXED PROTECTION CASES\n")[0]

    def extend_context(self, overlay):
        super().extend_context(overlay)
        path = overlay / "platform/sched/task.hpp"
        text = path.read_text()
        anchor = " std::atomic<void*> native_file_mapping_retirement{};"
        assert text.count(anchor) == 1
        path.write_text(text.replace(anchor, anchor + "\n std::atomic<bool> shares_user_pagemap{true};", 1))

    def mixed_additions(self):
        d = self.body
        out = [r'''
namespace host_mixed {
bool active{};
unsigned expected_pubs{}, backend_calls{}, snapshots_taken{}, typed_calls{}, final_closes{};
int backend_error{}, typed_result{};
bool dirty{true}, snapshot_matches{true};
struct Snapshot {uint64_t address{};void* page{};};
std::array<Snapshot,16> snapshots;
std::vector<uint64_t> written_offsets;
std::vector<uint8_t> written_bytes;
auto allocation_allowed()->bool {
 if(!publication_depth)return host_fork_vm::ordinary();
 // These are the old generic4KiB allocation contexts, including lazy-row
 // IRQ-save rewriting. No split reader/COW/pin owner may reach them.
 return !root_readers&&!cow_depth&&!pin_depth&&!backend().preempt_depth&&!host_range::exclusive;
}
void ordinary_boundary(){
 EXPECT_TRUE(host_range::interrupts);EXPECT_EQ(publication_depth,1U);
 EXPECT_EQ(root_readers+cow_depth+pin_depth+backend().preempt_depth,0U);
 EXPECT_EQ(host_fork_vm::pubs,expected_pubs);
 EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(),0U);
}
void nested_root_probe(){
 ordinary_boundary();
 ker::mod::mm::huge::detail::CurrentRangeRootAccess access;
 EXPECT_EQ(ker::mod::mm::huge::detail::acquire_current_range_root(*host_range::current,access),ker::mod::mm::huge::Error::None);
 ker::mod::mm::huge::detail::release_current_range_root(access);
 ordinary_boundary();
}
void unmap_ack(ker::mod::mm::paging::PageTable*root,uint64_t address){
 ordinary_boundary();EXPECT_EQ(root,runtime_root);
 ASSERT_LT(host_managed_remove::unmap_acks,host_managed_remove::expected_unmaps.size());
 EXPECT_EQ(address,host_managed_remove::expected_unmaps[host_managed_remove::unmap_acks].first);
 ++host_managed_remove::unmap_acks;
}
void put_file(ker::vfs::File*f){
 EXPECT_EQ(root_readers+cow_depth+pin_depth+backend().preempt_depth,0U);
 int old=f->refcount.fetch_sub(1);EXPECT_GT(old,0);++host_fork_vm::vfs_puts;
 if(old==1){EXPECT_FALSE(ker::syscall::vmem::g_file_mmap_ranges_lock.held);++final_closes;}
}
void before_decrement(void*page){
 if(root_readers){EXPECT_EQ(root_readers,1U);EXPECT_TRUE(host_range::interrupts);return;}
 host_managed_remove::before_decrement(page);
}
}
// Finite Host endpoint: native lookup-hint caching is not modeled. The
// existing checked scalar backend owns reference/accounting/ACK checks once.
namespace ker::mod::mm::phys {
auto page_ref_dec(void* page, PageLookupHint*) -> uint32_t { return page_ref_dec(page); }
}
namespace ker::vfs {
auto file_mount_read_only(const File&f)->bool{return f.read_only;}
auto vfs_pwrite_file(File*,const void*bytes,size_t count,off_t offset)->ssize_t {
 host_mixed::nested_root_probe();++host_mixed::backend_calls;
 if(host_mixed::backend_error)return host_mixed::backend_error;
 host_mixed::written_offsets.push_back(static_cast<uint64_t>(offset));
 auto*p=static_cast<const uint8_t*>(bytes);host_mixed::written_bytes.insert(host_mixed::written_bytes.end(),p,p+count);
 return static_cast<ssize_t>(count);
}
}
namespace ker::mod::security {
''', d("platform/security/file_policy.hpp", "[[nodiscard]] constexpr auto authorize_file_description_access("), r'''
}
namespace ker::mod::mm::virt {
''']
        out.extend(d("platform/mm/virt.opt.cpp", anchor) for anchor in (
            "auto entry_at(const PageTable* table, size_t index) -> const PageTableEntry& {",
            "auto present_mapping_entry(", "auto is_page_mapped(", "auto is_page_reserved(", "void unpin_user_page("))
        out.append(r'''
// The current snapshot inputs below are finite endpoint observations. An
// unexpected quiesced path aborts; it is not a native-root qualification.
auto pin_user_page(PageTable*,vaddr_t,bool,UserPagePin&,bool)->bool{std::abort();}
auto user_page_pin_still_mapped(const UserPagePin&)->bool{return host_mixed::snapshot_matches;}
struct UserFutexIdentity;
}
namespace ker::mod::sys::usercopy {
class UserPageFrameRef;
class UserAccessGuard {public:UserAccessGuard(){EXPECT_TRUE(host_mixed::active);}~UserAccessGuard()=default;};
''')
        out.append(d("platform/sys/usercopy.hpp", "class StableUserPage {"))
        for anchor in ("StableUserPage::~StableUserPage()", "auto StableUserPage::valid()", "auto StableUserPage::kernel_address()",
                       "auto StableUserPage::physical_address()", "auto StableUserPage::was_dirty()", "auto StableUserPage::still_mapped()",
                       "void StableUserPage::reset()"):
            out.append(d("platform/sys/usercopy.cpp", anchor))
        out.append(r'''
auto pin_task_user_page(sched::task::Task&task,uint64_t address,bool writable,bool fault_in,StableUserPage&out)->bool{
 out.reset();host_mixed::ordinary_boundary();EXPECT_FALSE(writable);EXPECT_FALSE(fault_in);
 for(auto const&given:host_mixed::snapshots)if(given.page&&given.address==address){
  mm::paging::PageTable*root=nullptr;if(!task.try_acquire_usercopy_pagemap(root))return false;
  out.m_task=&task;
  if(!mm::virt::acquire_user_pagemap_access(root,false,out.m_pin.access_gate_index)){out.reset();return false;}
  uint32_t page=0;auto*block=backend().containing(reinterpret_cast<uint64_t>(given.page),page);
  if(!block||!block->refs[page]){ADD_FAILURE()<<"snapshot endpoint lacks a live supplied frame";out.reset();return false;}
  ++block->refs[page];out.m_pin.pagemap=root;out.m_pin.hhdm_page=given.page;out.m_pin.user_page=address;
  out.m_pin.physical_page=block->physical+uint64_t{page}*mm::paging::PAGE_SIZE;out.m_pin.dirty=host_mixed::dirty;
  out.m_user_address=address;++host_mixed::snapshots_taken;return true;
 }
 return false;
}
auto mapped_physical_address(sched::task::Task&,uint64_t,bool=false)->uint64_t;
''')
        out.append(d("platform/sys/usercopy.cpp", "auto mapped_physical_address("))
        out.extend(["}\nnamespace ker::syscall::vmem {\n",
                    "namespace detail {int managed_range_error(ker::mod::mm::huge::Error);}\n",
                    "constexpr bool ENABLE_WATCHED_MMAP_LOGS=false;constexpr uint64_t WATCH_MMAP_VADDR=0;\n",
                    "using log_mixed_task=ker::mod::sched::task::Task;\n"])
        for anchor in (
            "inline auto is_watched_mmap_vaddr(", "auto align_user_vmem_size(", "struct OccupiedVmemRange {",
            "auto table_from_entry(", "auto page_table_index(", "auto next_table_boundary(",
            "auto pte_raw(const ker::mod::mm::paging::PageTableEntry& entry) -> uint64_t {", "auto is_reserved_leaf(", "auto first_pagemap_occupied_range(",
            "void release_lazy_vmem_range_files(", "auto push_lazy_vmem_range(", "auto push_lazy_vmem_range_copy(",
            "auto remove_lazy_vmem_range("):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        out.append("template<typename Fn>\n" + d("syscalls_impl/vmem/sys_vmem.cpp", "auto update_shared_vmem_ranges_locked("))
        for anchor in ("auto remove_shared_vmem_range_locked(", "struct FileMmapSyncRange {", "auto collect_file_mmap_sync_ranges(",
                       "auto authorize_file_mapping_writeback(", "auto write_file_mapping_bytes(", "void release_file_mmap_sync_ranges(",
                       "enum class FileMmapPageSnapshot :", "auto snapshot_file_mmap_page(", "auto preflight_file_mmap_sync_ranges(",
                       "auto sync_file_mmap_range_impl(", "auto sync_file_mmap_range(ker::mod::sched::task::Task&",
                       "auto register_file_mmap_range(",
                       "auto public_shared_file_overlap(ker::mod::sched::task::Task& task, uint64_t start, uint64_t length) -> bool {"):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        out.append(r'''
// Preserve the actual dispatch and unchanged production typed checks by source
// guards. This finite return endpoint does not execute a binding mutation.
auto change_public_shared_file(log_mixed_task&,uint64_t,uint64_t,bool protect,uint64_t=0)->uint64_t{
 EXPECT_FALSE(protect);++host_mixed::typed_calls;return static_cast<uint64_t>(host_mixed::typed_result);
}
''')
        for anchor in ("class PreparedRangeDemotion {", "\nauto demote_prepared_range_locked(", "auto anon_free("):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        out.extend(["namespace detail {\n", d("syscalls_impl/vmem/sys_vmem.cpp", "auto remove_current_mixed_range("), "}\n}\n"])
        return "\n".join(out)

    def compose(self, *arguments):
        source = super().compose(*arguments)
        declarations = r'''
#include <climits>
#include <sys/types.h>
namespace ker::vfs {struct File;}
namespace ker::mod::mm::paging {struct PageTable;}
namespace host_mixed {extern bool active;bool allocation_allowed();void put_file(ker::vfs::File*);void before_decrement(void*);void unmap_ack(ker::mod::mm::paging::PageTable*,unsigned long);}
'''
        replacements = [
            ("struct File {std::atomic<int> refcount{1};};",
             "struct FileOps{void(*vfs_write)(){};};struct File {std::atomic<int> refcount{1};FileOps*fops{};unsigned open_flags{2};bool read_only{};};"),
            ("void vfs_retain_file(File*){std::abort();} // Whole-child unregister never splits a registry row.",
             self.body("vfs/core.cpp", "void vfs_retain_file(")),
            ("void vfs_put_file(File* f){if(!f)return;EXPECT_TRUE(host_fork_vm::ordinary());",
             "void vfs_put_file(File* f){if(!f)return;if(host_mixed::active){host_mixed::put_file(f);return;}EXPECT_TRUE(host_fork_vm::ordinary());"),
            ("host_managed_remove::unmap_ack(root,va);return;",
             "if(host_mixed::active)host_mixed::unmap_ack(root,va);else host_managed_remove::unmap_ack(root,va);return;"),
            ("host_managed_remove::before_decrement(pointer);",
             "if(host_mixed::active)host_mixed::before_decrement(pointer);else host_managed_remove::before_decrement(pointer);"),
            ("struct log {template<class...Args>static void warn(const char*,Args...) {}};",
             "struct log {template<class...Args>static void warn(const char*,Args...) {} template<class...Args>static void error(const char*,Args...) {}};"),
        ]
        # The actual ordinary continuation adds an error log. This declared
        # logger endpoint retains the old warning behavior for all111 cases.
        for old, new in replacements:
            assert source.count(old) == 1, old
            source = source.replace(old, new, 1)
        # Existing global allocator ledger assumed only outside-PUB prepared
        # metadata. New mode also executes the real legacy under-PUB rewrites.
        for anchor in ("auto take(std::size_t n) -> void* {", "void drop(void* p) {"):
            body = self.definition(source, anchor)
            old = "EXPECT_TRUE(ordinary());"
            assert body.count(old) == 1
            source = source.replace(body, body.replace(old, "EXPECT_TRUE(host_mixed::active?host_mixed::allocation_allowed():ordinary());"), 1)
        return declarations + source + "\n" + self.mixed_additions() + self.case_source()

    def controls(self, source):
        yield from super().controls(source)
        vectors = [
            ("mixed-omits-geometry-validation",
             "return detail::managed_range_error(admission.validate_split_and_arm(prepared.survey_, prepared.tables_));",
             "return 0;", "MixedHugeFourKFileHoleAndNoneUsesOrdinaryContinuation"),
            ("mixed-keeps-reader-in-continuation", "int const SYNC_RET = sync_file_mmap_range(*task, addr, size);",
             "ker::mod::mm::huge::detail::CurrentRangeRootAccess extra; (void)ker::mod::mm::huge::detail::acquire_current_range_root(*task,extra); int const SYNC_RET=sync_file_mmap_range(*task,addr,size); ker::mod::mm::huge::detail::release_current_range_root(extra);",
             "BackendRunsAfterReaderDestructionUnderSamePublication"),
            ("mixed-reacquires-publication", "int const SYNC_RET = sync_file_mmap_range(*task, addr, size);",
             "publication_guard.~SharedVmemPublicationGuard(); ::new(&publication_guard) SharedVmemPublicationGuard; int const SYNC_RET=sync_file_mmap_range(*task,addr,size);",
             "BackendRunsAfterReaderDestructionUnderSamePublication"),
        ]
        for label, old, new, test in vectors:
            assert source.count(old) == 1, label
            yield label, source.replace(old, new, 1), "ManagedMixedGeometry." + test
        actual = self.body("platform/mm/virt.opt.cpp", "auto commit_managed_split_owned(")
        old = "for (uint16_t i = 0; i < storage->descriptor_count; ++i) {"
        assert actual.count(old) == 2
        bad = actual.replace(old, "for (uint16_t i = 0; i + 1 < storage->descriptor_count; ++i) {", 1)
        assert source.count(actual.strip()) == 1
        yield "mixed-skips-descriptor", source.replace(actual.strip(), bad.strip(), 1), "ManagedMixedGeometry.CrossLeafDescriptorsAllDemoteBeforeOrdinaryUnmap"
        actual = self.body("platform/mm/virt.opt.cpp", "void unmap_page(")
        old = "    flush_pagemap_after_update(page_table, vaddr, false);\n    drop_present_leaf_ref(OLD_ENTRY);"
        assert actual.count(old) == 1
        bad = actual.replace(old, "    drop_present_leaf_ref(OLD_ENTRY);\n    flush_pagemap_after_update(page_table, vaddr, false);")
        assert source.count(actual.strip()) == 1
        yield "mixed-drops-before-ack", source.replace(actual.strip(), bad.strip(), 1), "ManagedMixedGeometry.MixedHugeFourKFileHoleAndNoneUsesOrdinaryContinuation"


class ProtectBuilder(Builder):
    """Actual protection continuation; explicit Task/hardware publication ports."""

    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        count = self.protect_cases().count("TEST_F(ManagedMixedProtect,")
        if not 0 < count <= 18:
            raise ValueError("mixed protection profile must add1..18 actual cases")
        self.test_count += count

    def protect_cases(self):
        before, marker, after = self.mixed_cases.read_text().partition("// BEGIN MIXED PROTECTION CASES\n")
        if not marker or not before:
            raise ValueError("separate protection case boundary absent")
        return after

    def extend_context(self, overlay):
        super().extend_context(overlay)
        path = overlay / "platform/sched/task.hpp"
        source = path.read_text()
        anchor = " auto try_acquire_usercopy_pagemap(mm::paging::PageTable*&) -> bool;"
        assert source.count(anchor) == 1
        source = source.replace("#include <atomic>", "#include <atomic>\n#include <platform/mm/numa_policy.hpp>", 1)
        path.write_text(source.replace(anchor, " auto memory_policy_snapshot() const -> mm::numa::Policy;\n" + anchor, 1))

    def protection_additions(self):
        d = self.body
        out = [r'''
namespace host_protect {
bool active{}, zero_refused{};
unsigned acks{}, policy_reads{}, map_calls{}, cr3_writes{};
ker::mod::mm::numa::Policy copied_policy{};
void* supplied_zero{};
void acknowledge(ker::mod::mm::paging::PageTable* root,uint64_t,bool){
 host_mixed::ordinary_boundary();EXPECT_EQ(root,runtime_root);++acks;
}
}
// Only this new profile declares copied Task-policy input and hardware/root
// publication endpoints. The production Task policy reader is not modeled.
// Finite live-frame retain port; native allocator lookup/atomics are not
// modeled. Real materialize_reserved_page retains exactly its supplied frame.
namespace ker::mod::mm::phys {
void page_ref_inc(void* pointer) {
 uint32_t page = 0;
 auto* block = backend().containing(reinterpret_cast<uint64_t>(pointer), page);
 ASSERT_NE(block, nullptr);
 ASSERT_EQ(reinterpret_cast<uint64_t>(pointer) % paging::PAGE_SIZE, 0U);
 ASSERT_GT(block->refs[page], 0U);
 ASSERT_LT(block->refs[page], UINT32_MAX);
 ++block->refs[page];
}
}
namespace ker::mod::sched::task {
auto Task::memory_policy_snapshot() const -> mm::numa::Policy {
 EXPECT_TRUE(host_protect::active);EXPECT_EQ(this,host_range::current);
 EXPECT_EQ(publication_depth+root_readers+cow_depth+pin_depth+backend().preempt_depth,0U);
 EXPECT_TRUE(host_range::interrupts);++host_protect::policy_reads;return host_protect::copied_policy;
}
}
namespace ker::mod::security {
''', d("platform/security/file_policy.hpp", "[[nodiscard]] constexpr auto authorize_file_mapping("), r'''
}
namespace ker::mod::mm::virt {
// Finite CR3 and optional allocation-provenance endpoints. No native TLB or
// provenance-history qualification follows from these calls.
auto rdcr3()->uint64_t{return 0;}
void wrcr3(uint64_t){host_mixed::ordinary_boundary();++host_protect::cr3_writes;}
void owned_frame_refresh_leaf(PageTable*,vaddr_t,const PageTableEntry&){}
''']
        for anchor in ("auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted) -> paging::PageTable* {", "auto is_page_write_restricted(",
                       "auto protect_anonymous_swap_page(", "void unify_page_flags("):
            out.append(d("platform/mm/virt.opt.cpp", anchor))
        out.append(r'''
// This finite mapping endpoint owns the exact supplied allocation reference.
// It checks context and writes the supplied root; it is not actual map_page.
void map_page(PageTable*root,vaddr_t address,paddr_t physical,uint64_t flags){
 host_mixed::ordinary_boundary();EXPECT_EQ(root,runtime_root);
 auto* leaf=leaf_entry(root,address);ASSERT_NE(leaf,nullptr);
 EXPECT_EQ(leaf->present,0U);*leaf=paging::create_page_table_entry(physical,flags);
 ++host_protect::map_calls;host_protect::acknowledge(root,address,false);
}
}
namespace ker::syscall::vmem {
// The shared-zero cache is supplied explicitly. The real materializer below
// still performs its own NULL refusal, frame retain and adoption.
auto get_anon_zero_page()->void*{
 host_mixed::ordinary_boundary();return host_protect::zero_refused?nullptr:host_protect::supplied_zero;
}
''')
        for anchor in ("auto is_prot_none(", "auto prot_to_page_flags(", "auto anon_zero_page_flags(",
                       "auto materialize_reserved_page(", "auto protect_lazy_vmem_range(",
                       "auto protect_shared_vmem_range_locked(", "auto authorize_shared_file_protection_locked(",
                       "auto protect_range("):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        out.extend(["namespace detail {\n", d("syscalls_impl/vmem/sys_vmem.cpp", "auto protect_current_mixed_range("), "}\n}\n"])
        return "\n".join(out)

    def compose(self, *arguments):
        source = super().compose(*arguments)
        declarations = r'''
#include <cstdint>
namespace ker::mod::mm::paging {struct PageTable;}
namespace host_protect {extern bool active;void acknowledge(ker::mod::mm::paging::PageTable*,uint64_t,bool);}
'''
        anchor = "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {"
        assert source.count(anchor) == 1
        source = source.replace(anchor, anchor + "\n if(host_protect::active&&!root_readers){host_protect::acknowledge(root,va,full);return;}", 1)
        anchor = "EXPECT_FALSE(protect);++host_mixed::typed_calls;"
        assert source.count(anchor) == 1
        source = source.replace(anchor, "EXPECT_EQ(protect,host_protect::active);++host_mixed::typed_calls;", 1)
        return declarations + source + self.protection_additions() + self.protect_cases()

    def controls(self, source):
        yield from super().controls(source)
        actual = self.body("syscalls_impl/vmem/sys_vmem.cpp", "auto protect_range(")
        vectors = [
            ("protect-skips-demotion",
             "int const DEMOTION = demote_prepared_range_locked(*task, ADDR, ADDR + size, publication_guard, *prepared);",
             "int const DEMOTION = 0;", "CrossLeafDescriptorsFinishBeforeProtection"),
            ("protect-keeps-reader",
             "if (!protect_shared_vmem_range_locked(task, ADDR, size, PROT)) {",
             "ker::mod::mm::huge::detail::CurrentRangeRootAccess extra; (void)ker::mod::mm::huge::detail::acquire_current_range_root(*task,extra); if (!protect_shared_vmem_range_locked(task, ADDR, size, PROT)) {",
             "MixedReadPreservesDataOutsideFlagsAndFileOwners"),
            ("protect-reacquires-publication",
             "if (!protect_shared_vmem_range_locked(task, ADDR, size, PROT)) {",
             "publication_guard.~SharedVmemPublicationGuard(); ::new(&publication_guard) SharedVmemPublicationGuard; if (!protect_shared_vmem_range_locked(task, ADDR, size, PROT)) {",
             "MixedReadPreservesDataOutsideFlagsAndFileOwners"),
            ("protect-skips-readonly-preflight",
             "if (ker::mod::mm::virt::is_page_write_restricted(task->pagemap, page)) {",
             "if (false) {", "ReadonlySharedTailRefusesBeforeAnySplit"),
        ]
        for label, old, new, test in vectors:
            assert actual.count(old) == 1, label
            bad = actual.replace(old, new, 1)
            assert source.count(actual.strip()) == 1
            yield label, source.replace(actual.strip(), bad.strip(), 1), "ManagedMixedProtect." + test


class FixedBuilder(ProtectBuilder):
    """Actual private anonymous replacement; finite address/AS/backing ports.

    All projections below apply only to this new composite. Existing profiles
    retain their original generator and endpoint source byte for byte.
    """

    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        self.fixed_cases = support / "managed_anonymous_fixed_cases.inc"
        count = self.fixed_cases.read_text().count("TEST_F(ManagedAnonymousFixed,")
        if not 0 < count <= 16:
            raise ValueError("anonymous fixed profile must add1..16 actual cases")
        self.inputs.append(self.fixed_cases)
        self.test_count += count

    def fixed_additions(self):
        d = self.body
        out = [r'''
namespace host_fixed {
bool active{}, valid_hhdm{true}, admit{true}, lazy{}, zero_cow{true};
int fail_after{-1};
unsigned allocation_calls{}, admissions{}, bulk_maps{}, clock_reads{}, rollback_acks{};
std::vector<uint64_t> replacement_frames;
std::vector<std::pair<uint64_t,uint64_t>> new_mappings;
void record_mapping(uint64_t address,uint64_t physical){
 ASSERT_LT(new_mappings.size(),64U);new_mappings.push_back({address,physical});
}
auto hhdm_address(ker::mod::mm::paging::PageTable* root)->uintptr_t {
 EXPECT_EQ(root,runtime_root);
 return valid_hhdm?0xffff800000001000ULL:0x1234ULL;
}
void allocation_boundary(){host_mixed::ordinary_boundary();}
// A pages1 failure notification is legitimate on this new ordinary path.
// NONE records no reclaim progress; the old inactive512-only refusal remains.
void notify_order0_failure(uint16_t node){
 host_mixed::ordinary_boundary();EXPECT_LT(node,backend().topology.node_count);
}
// Finite PA ledger, not a PMM: scan at most64 in-range order0 candidates.
// A page conflicts with ANY retained live page, including managed owners.
auto fresh_physical(uint64_t first,uint64_t end,uint64_t alignment)->uint64_t {
 if(replacement_frames.size()>=64)return UINT64_MAX;
 uint64_t const start=(first+32*LARGE+alignment-1)&~(alignment-1);
 for(unsigned n=0;n<64;++n){
  uint64_t const pa=start+uint64_t{n}*alignment;
  if(pa<first||pa>end||end-pa<PAGE)break;
  bool live=false;
  for(auto const& block:backend().blocks)
   for(uint32_t i=0;i<block.pages;++i)
    live|=block.refs[i]&&pa<block.physical+uint64_t{i+1}*PAGE&&block.physical+uint64_t{i}*PAGE<pa+PAGE;
  if(!live){replacement_frames.push_back(pa);return pa;}
 }
 return UINT64_MAX;
}
void rollback_ack(ker::mod::mm::paging::PageTable*root,uint64_t address){
 host_mixed::ordinary_boundary();EXPECT_EQ(root,runtime_root);
 auto*entry=ker::mod::mm::virt::leaf_entry(root,address);ASSERT_NE(entry,nullptr);
 EXPECT_EQ(entry->present,0U);  // actual unmap clears the PTE before ACK.
 auto const found=std::find_if(new_mappings.begin(),new_mappings.end(),[&](auto const& row){return row.first==address;});
 ASSERT_NE(found,new_mappings.end());
 EXPECT_NE(std::find(replacement_frames.begin(),replacement_frames.end(),found->second),replacement_frames.end());
 ++rollback_acks;
}
}
namespace ker::mod::dbg {template<class...Args>void error(const char*,Args...){}}
namespace ker::mod::perf {auto is_wki_recording_enabled()->bool{return false;}}
namespace ker::mod::mm::phys {
void page_ref_add(void* page,uint64_t count){
 host_mixed::ordinary_boundary();
 ASSERT_LE(count,64U);
 for(uint64_t i=0;i<count;++i)page_ref_inc(page);
}
}
namespace ker::mod::mm::virt {
''', d("platform/mm/virt.opt.cpp", "auto is_page_mapped_or_reserved("), r'''
void map_same_page_range(PageTable*root,vaddr_t address,paddr_t physical,uint64_t count,uint64_t flags){
 host_mixed::ordinary_boundary();ASSERT_LE(count,64U);++host_fixed::bulk_maps;
 for(uint64_t i=0;i<count;++i)map_page(root,address+i*paging::PAGE_SIZE,physical,flags);
}
}
namespace ker::mod::sched::resource {
// Decision endpoint only: actual policy/accounting walks are not modeled.
// The actual observation methods below retain attempt/commit/recount ordering.
auto admit_mapping_count(task::Task* subject,mm::paging::PageTable* root,uint64_t added,
 uint64_t replaced_start,uint64_t replaced_size,uint64_t* admitted_bytes)->bool {
 host_mixed::ordinary_boundary();EXPECT_EQ(subject,host_range::current);EXPECT_EQ(root,runtime_root);
 EXPECT_GT(added,0U);EXPECT_EQ(added%PAGE,0U);
 EXPECT_TRUE(replaced_size==0||(replaced_start%PAGE==0&&replaced_size==added));
 ++host_fixed::admissions;
 if(!host_fixed::admit)return false;
 if(admitted_bytes)*admitted_bytes=added;
 return true;
}
using ker::mod::sched::get_current_task;
''', d("platform/sched/resource_as.cpp", "auto can_reuse_mapping_count("),
               d("platform/sched/resource_as.cpp", "auto AddressSpaceObservation::admit_mapping("),
               d("platform/sched/resource_as.cpp", "void AddressSpaceObservation::commit_mapping()"), r'''
}
namespace ker::syscall::vmem {
constexpr uint64_t MMAP_START=0x0000200000000000ULL;
auto lazy_anon_mmap_enabled()->bool{return host_fixed::lazy;}
auto anon_zero_cow_enabled()->bool{return host_fixed::zero_cow;}
''']
        # Definitions remain actual source bodies. These forward declarations
        # are only extraction order; the production class is not moved.
        out.append("void rollback_mapped_pages(ker::mod::sched::task::Task*,uint64_t,uint64_t);\n")
        for anchor in (
            "auto clamp_perf_u16(", "auto clamp_perf_u32(", "auto vmem_latency_since(",
            "auto perf_status_from_vmem_result(", "void record_local_vmem_event(",
            "auto first_lazy_vmem_overlap(", "auto first_occupied_range(",
            "auto mmap_range_is_free(", "auto find_free_range_from(",
            "void advance_mmap_cursor(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {",
            "void advance_shared_mmap_cursor_locked(", "auto find_free_range(",
            "auto add_lazy_vmem_range_entry(", "auto add_lazy_vmem_range(",
            "auto add_shared_vmem_range_locked(", "auto reserve_free_mmap_range_locked(",
            "void release_mmap_reservation_locked(", "void release_fixed_mmap_range_locked(",
            "auto private_anon_allocate_locked(",
            "void rollback_mapped_pages(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t mapped_pages) {",
            "auto validate_prepared_anon_fixed(ker::mod::sched::task::Task& actor,",
        ):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        actual = d("syscalls_impl/vmem/sys_vmem.cpp", "auto anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, PreparedRangeDemotion* prepared)")
        old = "auto pm_addr = reinterpret_cast<uintptr_t>(task->pagemap);"
        assert actual.count(old) == 1
        # Numerical HHDM input only. Preserve the real guard and refusal.
        out.append(actual.replace(old, "auto pm_addr = host_fixed::hhdm_address(task->pagemap);", 1))
        out.append(d("syscalls_impl/vmem/sys_vmem.cpp", "auto anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags) -> uint64_t {"))
        out.extend(["namespace detail {\n", d("syscalls_impl/vmem/sys_vmem.cpp", "auto replace_current_anonymous_fixed("), "}\n}\n"])
        return "\n".join(out)

    def compose(self, *arguments):
        source = super().compose(*arguments)
        declarations = r'''
#include <cstdint>
namespace ker::mod::mm::paging {struct PageTable;}
namespace host_fixed {
extern bool active;extern int fail_after;extern unsigned allocation_calls;
void allocation_boundary();auto fresh_physical(uint64_t,uint64_t,uint64_t)->uint64_t;
extern unsigned clock_reads;
void rollback_ack(ker::mod::mm::paging::PageTable*,uint64_t);
void record_mapping(uint64_t,uint64_t);
void notify_order0_failure(uint16_t);
}
'''
        old = "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {"
        assert source.count(old) == 1
        source = source.replace(old, old + "\n if(host_fixed::active&&!full&&!root_readers){host_mixed::unmap_ack(root,va);return;}", 1)
        old = "auto notify_order0_node_failure(uint16_t) noexcept -> NodeReclaimStatus {"
        assert source.count(old) == 1
        source = source.replace(old, "auto notify_order0_node_failure(uint16_t node) noexcept -> NodeReclaimStatus {\n    if(host_fixed::active){host_fixed::notify_order0_failure(node);return NodeReclaimStatus::NONE;}", 1)
        old = "return allocate(first, pages, alignment, initialization);"
        assert source.count(old) == 1
        new = r'''
if(host_fixed::active&&owner==mm::PhysicalPageOwner::USER_PRIVATE_MAPPING&&pages==1){
 host_fixed::allocation_boundary();++host_fixed::allocation_calls;
 if(host_fixed::fail_after==0)return nullptr;
 if(host_fixed::fail_after>0)--host_fixed::fail_after;
 auto const pa=host_fixed::fresh_physical(first,end,alignment);
 if(pa==UINT64_MAX)return nullptr;
 return allocate(pa,pages,alignment,initialization);
}
return allocate(first, pages, alignment, initialization);'''
        source = source.replace(old, new, 1)
        old = "EXPECT_EQ(leaf->present,0U);*leaf=paging::create_page_table_entry(physical,flags);"
        assert source.count(old) == 1
        source = source.replace(old, "if(host_fixed::active)host_fixed::record_mapping(address,physical);\n " + old, 1)
        old = "ASSERT_LT(host_managed_remove::unmap_acks,host_managed_remove::expected_unmaps.size());"
        assert source.count(old) == 1
        source = source.replace(old, "if(host_fixed::active&&host_managed_remove::unmap_acks==host_managed_remove::expected_unmaps.size()){host_fixed::rollback_ack(root,address);return;}\n " + old, 1)
        old = "namespace ker::mod::time {auto get_us()->uint64_t{std::abort();}}"
        assert source.count(old) == 1
        source = source.replace(old, "namespace ker::mod::time {auto get_us()->uint64_t{if(host_fixed::active)return ++host_fixed::clock_reads;std::abort();}}", 1)
        return declarations + source + self.fixed_additions() + self.fixed_cases.read_text()

    def controls(self, source):
        # Four new controls only; all old modes/control vectors stay exact.
        actual = self.body("syscalls_impl/vmem/sys_vmem.cpp", "auto anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, PreparedRangeDemotion* prepared)")
        actual = actual.replace("auto pm_addr = reinterpret_cast<uintptr_t>(task->pagemap);", "auto pm_addr = host_fixed::hhdm_address(task->pagemap);", 1)
        vectors = [
            ("fixed-skips-demotion", "int const DEMOTION = demote_prepared_range_locked(*task, vaddr, vaddr + size, publication_guard, *prepared);", "int const DEMOTION = 0;", "CrossLeafReplacementDemotesAllBeforeUnmap"),
            ("fixed-keeps-reader", "release_fixed_mmap_range_locked(task, vaddr, size);", "ker::mod::mm::huge::detail::CurrentRangeRootAccess extra; (void)ker::mod::mm::huge::detail::acquire_current_range_root(*task,extra); release_fixed_mmap_range_locked(task,vaddr,size); ker::mod::mm::huge::detail::release_current_range_root(extra);", "PrivateReadUsesSamePublicationAfterReaderDestruction"),
            ("fixed-reacquires-publication", "release_fixed_mmap_range_locked(task, vaddr, size);", "publication_guard.~SharedVmemPublicationGuard(); ::new(&publication_guard) SharedVmemPublicationGuard; release_fixed_mmap_range_locked(task,vaddr,size);", "PrivateReadUsesSamePublicationAfterReaderDestruction"),
            ("fixed-bypasses-as-refusal", "if (!observation.admit_mapping(task->pagemap, size, hint, size)) {", "if (false) {", "AsRefusalKeepsManagedLeafAndMetadataExact"),
        ]
        for label, old, new, test in vectors:
            assert actual.count(old) == 1, label
            assert source.count(actual.strip()) == 1, label
            yield label, source.replace(actual.strip(), actual.replace(old, new, 1).strip(), 1), "ManagedAnonymousFixed." + test


class FileFixedBuilder(FixedBuilder):
    """Actual File fixed body on explicit FD/stat/backend/cache decision ports.

    This subclass alone expands the File declaration/ports. All prior Builder,
    ProtectBuilder and FixedBuilder bytes/profiles remain exact249/255.
    """

    def __init__(self, kernel_file, support, definition):
        super().__init__(kernel_file, support, definition)
        self.file_cases = support / "managed_file_fixed_cases.inc"
        count = self.file_cases.read_text().count("TEST_F(ManagedFileFixed,")
        if not 0 < count <= 16:
            raise ValueError("File fixed profile must add1..16 actual cases")
        self.inputs.append(self.file_cases)
        self.test_count += count

    def file_additions(self):
        d = self.body
        out = [r'''
namespace host_file {
bool active{}, in_operation{}, lazy{}, cache{}, reuse_fd{};
int stat_error{}, read_error{}, fail_read_after{-1};
unsigned fd_lookups{}, stats{}, reads{}, outer_closes{}, typed_calls{}, cache_hits{}, batch_flushes{};
size_t max_read=PAGE;
ker::vfs::File* fd_file{};
ker::vfs::File* expected_file{};
ker::vfs::File* replacement{};
std::vector<uint64_t> read_offsets;
std::vector<void*> cache_pages;
void final_close(ker::vfs::File* file){
 if(active&&in_operation&&file==expected_file){
  EXPECT_EQ(publication_depth,0U);EXPECT_TRUE(host_range::interrupts);
  EXPECT_EQ(root_readers+cow_depth+pin_depth+backend().preempt_depth,0U);++outer_closes;
 }
}
}
namespace ker::vfs {
namespace tmpfs {auto get_tmpfs_fops()->FileOps*{static FileOps ops;return &ops;}}
auto vfs_get_file_retain_checked(ker::mod::sched::task::Task* task,int fd,File*& empty)->int {
 EXPECT_TRUE(host_fork_vm::ordinary());EXPECT_EQ(task,host_range::current);++host_file::fd_lookups;
 if(empty)return -EINVAL;
 if(fd!=7||!host_file::fd_file)return -EBADF;
 if(!vfs_try_retain_file(host_file::fd_file))return -EOVERFLOW;
 empty=host_file::fd_file;return 0;
}
auto vfs_fstat_file(File* file,Stat* output)->int {
 EXPECT_TRUE(host_fork_vm::ordinary());++host_file::stats;
 if(!file||!output)return -EINVAL;
 if(host_file::stat_error)return host_file::stat_error;
 *output=file->stat;return 0;
}
auto vfs_pread_file_direct(File* file,void* output,size_t length,off_t offset)->ssize_t {
 host_mixed::nested_root_probe();EXPECT_EQ(file,host_file::expected_file);EXPECT_GE(offset,0);
 if(host_file::reuse_fd){
  host_file::reuse_fd=false;EXPECT_EQ(host_file::fd_file,file);
  host_file::fd_file=host_file::replacement;vfs_put_file(file);
 }
 ++host_file::reads;host_file::read_offsets.push_back(static_cast<uint64_t>(offset));
 if(host_file::read_error)return host_file::read_error;
 if(host_file::fail_read_after==0)return -EIO;
 if(host_file::fail_read_after>0)--host_file::fail_read_after;
 size_t const count=std::min(length,host_file::max_read);
 for(size_t i=0;i<count;++i)static_cast<uint8_t*>(output)[i]=static_cast<uint8_t>(0x31+(offset+i)%113);
 return static_cast<ssize_t>(count);
}
}
namespace ker::mod::mm::virt {
''', d("platform/mm/virt.hpp", "struct PageMapBatch {"), r'''
void init_page_map_batch(PageMapBatch* batch,PageTable* root,uint64_t flags){
 host_mixed::ordinary_boundary();batch->root=root;batch->table_flags=flags;
}
void map_page_batched(PageMapBatch* batch,vaddr_t address,paddr_t physical,uint64_t flags){
 host_mixed::ordinary_boundary();EXPECT_EQ(flags,batch->table_flags);map_page(batch->root,address,physical,flags);batch->dirty=true;
}
void flush_page_map_batch(PageMapBatch*){host_mixed::ordinary_boundary();++host_file::batch_flushes;}
}
namespace ker::mod::security {
''', r'''
}
namespace ker::syscall::vmem {
auto lazy_file_mmap_enabled()->bool{return host_file::lazy;}
auto file_mmap_page_cache_enabled()->bool{return host_file::cache;}
// The cache lookup/loan is a supplied reference, not native cache coherence.
auto file_mmap_cached_page_for_file(ker::vfs::File* file,const ker::vfs::Stat&,uint64_t offset,void** page)->bool{
 host_mixed::ordinary_boundary();EXPECT_EQ(file,host_file::expected_file);
 auto const index=offset/PAGE;
 if(index>=host_file::cache_pages.size()||!host_file::cache_pages[index])return false;
 *page=host_file::cache_pages[index];ker::mod::mm::phys::page_ref_inc(*page);++host_file::cache_hits;return true;
}
// Typed backend binding transaction remains an explicit unsupported endpoint;
// actual File dispatch/old occupied restriction are separately source-bound.
auto allocate_public_shared_tmpfs(ker::mod::sched::task::Task&,ker::vfs::File&,const ker::vfs::Stat&,
 uint64_t,uint64_t,uint64_t,uint64_t,uint64_t)->uint64_t {
 EXPECT_TRUE(host_fork_vm::ordinary());++host_file::typed_calls;return static_cast<uint64_t>(-EOPNOTSUPP);
}
''']
        for anchor in (
            "auto file_mapping_offset_range_valid(", "auto file_mmap_can_share(",
            "auto file_mapping_page_flags(", "auto add_lazy_file_vmem_range(",
            "auto add_shared_lazy_file_vmem_range_locked(", "auto read_file_mapping_exact(",
            "auto map_file_mmap_cached_page(",
            "auto file_allocate(ker::mod::sched::task::Task* task, uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset,",
            "auto file_allocate(ker::mod::sched::task::Task* task, uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset)\n",
        ):
            out.append(d("syscalls_impl/vmem/sys_vmem.cpp", anchor))
        out.extend(["namespace detail {\n", d("syscalls_impl/vmem/sys_vmem.cpp", "auto replace_current_file_fixed("), "}\n}\n"])
        return "\n".join(out)

    def compose(self, *arguments):
        source = super().compose(*arguments)
        # Only the File profile extends the real registered-backend lifetime.
        # Parent metadata/PTE/table cleanup precedes NumaAllocation::TearDown.
        owner = "    Backend model{};\n"
        hook = "    virtual void release_derived_descriptor_owners() {}\n"
        assert source.count(owner) == 1
        source = source.replace(owner, owner + hook, 1)
        teardown = "    void TearDown() override {\n        EXPECT_EQ(model.live_refs(), 0U);"
        assert source.count(teardown) == 1
        source = source.replace(teardown, teardown.replace("        EXPECT_EQ", "        release_derived_descriptor_owners();\n        EXPECT_EQ", 1), 1)
        old = "struct FileOps{void(*vfs_write)(){};};struct File {std::atomic<int> refcount{1};FileOps*fops{};unsigned open_flags{2};bool read_only{};};"
        new = r'''
// GTest's host sys/stat header may expose macros with the WOS names.
#ifdef S_IFMT
#undef S_IFMT
#endif
#ifdef S_IFREG
#undef S_IFREG
#endif
enum class FSType { SOCKET, TMPFS };
constexpr unsigned S_IFMT=0170000,S_IFREG=0100000;
struct Stat {uint64_t st_mode=S_IFREG,st_dev=1,st_ino=1;off_t st_size=64*4096;struct Time{long tv_sec{},tv_nsec{};}st_mtim,st_ctim;};
struct FileOps{void(*vfs_write)(){};};
struct File {std::atomic<int>refcount{1};FileOps*fops{};unsigned open_flags{2};bool read_only{};
 FSType fs_type{FSType::SOCKET};void*mount_owner{};off_t pos{17};const char*vfs_path{};Stat stat{};};
'''
        assert source.count(old) == 1
        source = source.replace(old, new, 1)
        old = "if(old==1){EXPECT_FALSE(ker::syscall::vmem::g_file_mmap_ranges_lock.held);++final_closes;}"
        assert source.count(old) == 1
        source = source.replace(old, old[:-1] + "host_file::final_close(f);}", 1)
        old = "owner==mm::PhysicalPageOwner::USER_PRIVATE_MAPPING&&pages==1"
        assert source.count(old) == 1
        source = source.replace(old, "(owner==mm::PhysicalPageOwner::USER_PRIVATE_MAPPING||(host_file::active&&owner==mm::PhysicalPageOwner::USER_FILE_MAPPING))&&pages==1", 1)
        declarations = r'''
#include <optional>
namespace ker::vfs {struct File;}
namespace host_file {extern bool active;void final_close(ker::vfs::File*);}
'''
        return declarations + source + self.file_additions() + self.file_cases.read_text()

    def controls(self, source):
        actual = self.body("syscalls_impl/vmem/sys_vmem.cpp", "auto file_allocate(ker::mod::sched::task::Task* task, uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset,")
        vectors = [
            ("file-fixed-skips-demotion", "int const DEMOTION = demote_prepared_range_locked(*task, vaddr, vaddr + size, publication_guard, *prepared);", "int const DEMOTION = 0;", "CrossLeafReadReplacesOnlyRequestedFrames"),
            ("file-fixed-keeps-reader", "release_fixed_mmap_range_locked(task, vaddr, size);", "ker::mod::mm::huge::detail::CurrentRangeRootAccess extra; (void)ker::mod::mm::huge::detail::acquire_current_range_root(*task,extra); release_fixed_mmap_range_locked(task,vaddr,size); ker::mod::mm::huge::detail::release_current_range_root(extra);", "ReadAndFinalOuterCloseUseOrdinaryContinuousPublication"),
            ("file-fixed-reacquires-publication", "release_fixed_mmap_range_locked(task, vaddr, size);", "publication_guard.~SharedVmemPublicationGuard(); ::new(&publication_guard) SharedVmemPublicationGuard; release_fixed_mmap_range_locked(task,vaddr,size);", "ReadAndFinalOuterCloseUseOrdinaryContinuousPublication"),
            ("file-fixed-bypasses-as", "if (!observation.admit_mapping(task->pagemap, size, hint, size)) {", "if (false) {", "AsDenialKeepsOldLeafAndDoesNotReadBacking"),
        ]
        for label, old, new, test in vectors:
            assert actual.count(old) == source.count(actual.strip()) == 1, label
            yield label, source.replace(actual.strip(), actual.replace(old, new, 1).strip(), 1), "ManagedFileFixed." + test
