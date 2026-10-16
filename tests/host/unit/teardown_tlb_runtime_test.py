#!/usr/bin/env python3
"""Execute actual raw/budget teardown with controlled physical/refcount/TLB seams.

Aligned Host buffers stand in for HHDM/physical tables; CPU TLB completion, root
access gates, file index, allocator metadata, clock and logging are controlled.
This checks detach-before-release and exact-root ACK ordering, including the
immediate capacity flush and partial disposal, not hardware IPI/SMP behavior.
"""
import argparse
import hashlib
import json
import os
import signal
import time
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules/kern/src/platform/mm/virt.opt.cpp"


def extract(source, token, semicolon=False):
    start = source.index(token)
    brace = source.index("{", start)
    i, depth = brace, 0
    while i < len(source):
        if source.startswith("//", i):
            i = source.index("\n", i) + 1
            continue
        if source.startswith("/*", i):
            i = source.index("*/", i + 2) + 2
            continue
        if source[i] in "\"'":
            if source[i] == "'" and i and source[i - 1].isalnum() and source[i + 1].isalnum():
                i += 1
                continue
            quote = source[i]
            i += 1
            while i < len(source):
                if source[i] == "\\":
                    i += 2
                    continue
                if source[i] == quote:
                    i += 1
                    break
                i += 1
            continue
        depth += source[i] == "{"
        depth -= source[i] == "}"
        i += 1
        if depth == 0:
            if semicolon:
                assert source[i] == ";"
                i += 1
            return source[start:i] + "\n"
    raise AssertionError(token)


PREFIX = r'''
#include <platform/mm/managed_huge_core.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>
namespace test_kernel {
using vaddr_t = uint64_t;
namespace huge = ker::mod::mm::huge;
struct PageTableEntry {
    uint64_t present:1 = 0, writable:1 = 0, user:1 = 0, rest:4 = 0, pagesize:1 = 0;
    uint64_t pad:4 = 0, frame:40 = 0, high:12 = 0;
};
static_assert(sizeof(PageTableEntry) == 8);
struct alignas(4096) PageTable { std::array<PageTableEntry,512> entries{}; };
static_assert(sizeof(PageTable) == 4096);
namespace paging {
constexpr uint64_t PAGE_SIZE=4096, PAGE_SHIFT=12, PAGE_TABLE_ENTRIES=512, PAGE_PRESENT=1;
auto purge_page_table_entry() { return PageTableEntry{}; }
}
namespace addr {
constexpr uint64_t get_hhdm_offset() { return 0; }
auto get_phys_pointer(uint64_t p) { return p; }
auto get_virt_pointer(uint64_t p) { return reinterpret_cast<uint64_t*>(p); }
}
// Disclosed Host physical-address seam: real aligned userspace buffers are the
// test's physical/HHDM tables; no actual kernel-half address is dereferenced.
constexpr unsigned CANONICAL_SIGN_BIT=63;
constexpr uint64_t CANONICAL_KERNEL_UPPER=0;
enum class PageKind { UNKNOWN,SLAB,MEDIUM,KMALLOC_LARGE,FREE,RESERVED,NORMAL,PAGE_TABLE };
struct FrameProbeCache {};
std::map<uint64_t,PageKind> kinds;
std::map<void*,unsigned> refs;
PageTable* expected_root=nullptr;
std::map<size_t,PageTable*> held_gates;
unsigned failures=0, checks=0, ref_batches=0, tlb_calls=0, released=0;
unsigned reachable_failures=0, ack_failures=0;
unsigned cow_depth=0, pin_depth=0;
uint64_t root_serial=1;
std::map<void*,unsigned> managed_extra;
std::map<void*,uint16_t> managed_index;
uint16_t observed_handoff=0;
constexpr uint64_t PTE_FRAME_MASK=0x000ffffffffff000ULL;
void observe_cap_completion();
struct HostRegistry : huge::RegistryCore {
    auto advance_retired_handoff(huge::LeafHandle handle,uint16_t next,uint16_t count) -> huge::Error {
        expect_pin();
        auto error=huge::RegistryCore::advance_retired_handoff(handle,next,count);
        if(error==huge::Error::None) observed_handoff=next+count;
        return error;
    }
    auto complete_retired_after_ack(huge::LeafHandle handle) -> huge::Error {
        expect_pin();
        observe_cap_completion();
        return huge::RegistryCore::complete_retired_after_ack(handle);
    }
    static void expect_pin();
} managed_huge_registry;
bool last_ack=false;
PageTable* last_ack_root=nullptr;
void expect(bool ok,const char* message) {
    ++checks;
    if(!ok) { ++failures; std::fprintf(stderr,"FAIL %s\n",message); }
}
void HostRegistry::expect_pin(){expect(pin_depth==1,"actual registry handoff/completion serialized by pin");}
void observe_cap_completion(){
    expect(observed_handoff==512,"cap completion requires all512 actual registry handoffs");
    for(auto const& [p,extra]:managed_extra) expect(refs[p]==extra,"cap credit after last actual mapping ref decrement");
}
bool reachable(PageTable* table,int level,uint64_t target,std::set<PageTable*>& seen) {
    if(!seen.insert(table).second) return false;
    size_t max=level==4?256:512;
    for(size_t i=0;i<max;++i) {
        auto e=table->entries[i]; if(!e.present) continue;
        uint64_t p=e.frame<<12;
        if(p==target) return true;
        if(level>1 && !e.pagesize && kinds[p]==PageKind::PAGE_TABLE &&
           reachable(reinterpret_cast<PageTable*>(p),level-1,target,seen)) return true;
    }
    return false;
}
bool reachable(uint64_t target) { std::set<PageTable*> seen; return reachable(expected_root,4,target,seen); }
namespace dbg { [[noreturn]] void panic_handler(const char* what) { throw std::runtime_error(what); } }
struct Log { template<class... T> static void warn(T...) {} template<class... T> static void trace(T...) {} };
using log = Log;
namespace phys {
struct PageLookupHint {};
struct PageRefBatchStats { uint64_t refs_decremented{},pages_freed{}; };
auto page_ref_dec_batch(std::span<void*const> pages,PageLookupHint*) -> PageRefBatchStats {
    ++ref_batches;
    bool ack=last_ack && last_ack_root==expected_root;
    if(!ack) ++ack_failures;
    expect(ack,"each nonempty refdec batch follows exact-root completed ACK");
    last_ack=false;
    PageRefBatchStats out;
    expect(cow_depth+pin_depth==0,"ref finalization outside all COW/pin locks");
    for(void* page:pages) {
        if(managed_index.contains(page)) {
            huge::Snapshot snapshot{};managed_huge_registry.snapshot(snapshot);
            if(snapshot.retired_pages==512) {
                expect(observed_handoff>managed_index.at(page),"every queued ref handed off before possible64th append flush");
                expect(snapshot.charged_pages==512,"cap retained through every actual mapping ref drop");
            } else {
                expect(snapshot.mapped_pages==512 && refs[page]>1,"independent4K alias may retire before the still-mapped managed owner");
            }
        }
        expect(refs[page]>0,"real fixture owns every queued reference");
        if(refs[page]==1) {
            bool detached=!reachable(reinterpret_cast<uint64_t>(page));
            if(!detached) ++reachable_failures;
            expect(detached,"final ref is unreachable before immediate capacity/slice release");
        }
        if(refs[page]) { --refs[page]; ++out.refs_decremented; }
        if(!refs[page]) { ++out.pages_freed; ++released; kinds[reinterpret_cast<uint64_t>(page)]=PageKind::FREE; }
    }
    return out;
}
}
void flush_pagemap_after_update(PageTable* root,uint64_t address,bool full) {
    ++tlb_calls;
    expect(cow_depth+pin_depth==0,"real-flush seam called after unlocking COW/pin");
    expect(root==expected_root,"TLB operation uses exact actual fixture root");
    expect(address==0 && full,"teardown requires a full-root invalidation");
    expect(std::any_of(held_gates.begin(),held_gates.end(),[&](auto x){return x.second==root;}),"original root gate held across barrier");
    last_ack=true;last_ack_root=root;
}
namespace util {
template<class T,size_t N> struct SmallVec {
    std::vector<T> storage;
    auto size() const { return storage.size(); }
    bool contains(T value) const { return std::find(storage.begin(),storage.end(),value)!=storage.end(); }
    bool push_back(T value) { storage.push_back(value); return true; }
};
}
auto frame_page_kind(uint64_t p,FrameProbeCache* =nullptr) { return kinds[p]; }
auto phys_to_hhdm_checked(uint64_t p,FrameProbeCache* =nullptr) -> void* { return reinterpret_cast<void*>(p); }
auto entry_at(PageTable* p,size_t i) -> PageTableEntry& { return p->entries[i]; }
void log_user_phys_mappings(uint64_t,const char*,uint64_t,const char*,bool) {}
void debug_log_user_phys_mappings(uint64_t,const char*,uint64_t,const char*,bool) {}
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE=static_cast<size_t>(-1);
size_t begin_user_pagemap_exclusive(PageTable* root) {
    size_t n=reinterpret_cast<uint64_t>(root)>>12; expect(!held_gates.contains(n),"exclusive fixture gate not reentered"); held_gates[n]=root;return n;
}
size_t try_begin_user_pagemap_exclusive(PageTable* root) { return begin_user_pagemap_exclusive(root); }
void end_user_pagemap_exclusive(size_t n) { held_gates.erase(n); }
struct UserPagemapExclusiveScope {
    size_t n;
    explicit UserPagemapExclusiveScope(PageTable* r):n(begin_user_pagemap_exclusive(r)){}
    void finish(){if(n!=USER_PAGEMAP_ACCESS_GATE_NONE){end_user_pagemap_exclusive(n);n=USER_PAGEMAP_ACCESS_GATE_NONE;}}
    ~UserPagemapExclusiveScope(){finish();}
};
void purge_anonymous_swap_mappings(PageTable*) {}
void owned_frame_purge_pagemap(PageTable*) {}
namespace file_mapping { struct FileMappingTlbRetirement { void release_after_completed_tlb() {} }; }
struct RootControl { uint64_t serial{}; };
auto find_user_root_control_locked(PageTable* root) -> RootControl* {
    static RootControl control;
    if(root!=expected_root || root==nullptr) return nullptr;
    control.serial=root_serial;return &control;
}
struct PinLock {
    unsigned& depth;
    uint64_t lock_irqsave(){expect(depth==0,"nonrecursive Host lock");++depth;return 0;}
    void unlock_irqrestore(uint64_t){expect(depth==1,"exact Host lock release");--depth;}
} user_mapping_pin_lock{pin_depth},cow_pte_lock{cow_depth};
struct Index { bool root_empty_locked(PageTable*){return true;}bool detach_root_locked(PageTable*,uint64_t,file_mapping::FileMappingTlbRetirement&){return true;} } file_mapping_index;
namespace time { uint64_t get_us(){static uint64_t t=0;return ++t;} }
auto elapsed_us_since(uint64_t a,uint64_t b){return b>=a?b-a:0;}
'''

HELPERS = r'''
void note_destroy_refdec_batch_stats(DestroyUserSpaceCallStats* s,phys::PageRefBatchStats const& b) {
    if(s){s->data_pages_ref_decremented+=b.refs_decremented;s->data_pages_freed+=b.pages_freed;}
}
void note_destroy_page_table_refdec_batch_stats(DestroyUserSpaceCallStats* s,phys::PageRefBatchStats const& b) {
    if(s){s->page_table_pages_ref_decremented+=b.refs_decremented;s->page_table_pages_freed+=b.pages_freed;}
}
void note_destroy_huge_skip(DestroyUserSpaceCallStats* s){if(s)++s->skipped_huge_pages;}
void note_destroy_corrupt_skip(DestroyUserSpaceCallStats* s){if(s)++s->skipped_corrupt_entries;}
void note_destroy_page_table_alias_skip(DestroyUserSpaceCallStats* s){if(s)++s->skipped_page_table_aliases;}
void note_destroy_kind_skip(DestroyUserSpaceCallStats*,PageKind){}
void note_destroy_user_space_phase_time(DestroyUserSpaceCallStats&,DestroyUserSpacePhase,uint64_t){}
void note_destroy_user_space_stats(DestroyUserSpaceCallStats const&){}
'''

TESTS = r'''
struct Fixture {
    std::vector<void*> allocations;
    PageTable* root{};
    std::vector<PageTable*> leaves;
    std::vector<void*> data;
    void* alloc(PageKind kind) {
        void* p=std::aligned_alloc(4096,4096);if(!p)throw std::bad_alloc();
        std::fill_n(static_cast<unsigned char*>(p),4096,0);allocations.push_back(p);
        kinds[reinterpret_cast<uint64_t>(p)]=kind;refs[p]=1;return p;
    }
    PageTable* table(){return new(alloc(PageKind::PAGE_TABLE)) PageTable{};}
    static PageTableEntry entry(void* p){PageTableEntry e{};e.present=1;e.user=1;e.writable=1;e.frame=reinterpret_cast<uint64_t>(p)>>12;return e;}
    Fixture(size_t pages,bool many_tables=false) {
        root=table();auto* l3=table();auto* l2=table();root->entries[0]=entry(l3);l3->entries[0]=entry(l2);
        size_t n=many_tables?pages:1;
        for(size_t i=0;i<n;++i){auto* l1=table();leaves.push_back(l1);l2->entries[i]=entry(l1);}
        for(size_t i=0;i<pages;++i){void* p=alloc(PageKind::NORMAL);data.push_back(p);leaves[many_tables?i:0]->entries[many_tables?0:i]=entry(p);}
        expected_root=root;last_ack=false;last_ack_root=nullptr;
    }
    ~Fixture(){for(auto p:allocations){kinds.erase(reinterpret_cast<uint64_t>(p));refs.erase(p);std::free(p);}held_gates.clear();expected_root=nullptr;}
    void all_retired(){for(void* p:allocations)if(p!=root)expect(refs[p]==0,"all detached data/PT fixture refs retired exactly once");expect(refs[root]==1,"root original allocator owner retained");}
};
void bind_for_direct_test(DestroyRefdecBatch& batch,PageTable* root) {
    batch.root=root;
}
void raw_case(size_t pages,bool many_tables){Fixture f(pages,many_tables);unsigned t=tlb_calls,b=ref_batches;destroy_user_space(f.root,1,"host","raw");f.all_retired();expect(tlb_calls>t,"raw final flush retained");expect(ref_batches>b,"raw owner batches exercised");expect(held_gates.empty(),"raw gate released after final flush");}
void budget_case(unsigned step,bool partial,bool pt_partial) {
    Fixture f(65,pt_partial);auto* state=new DestroyUserSpaceBudgetState{};
    expect(begin_prepared_destroy_user_space(*state,f.root,1,"host","budget",true),"actual begin admits exact fixture root");
    expect(state->refdec_batch.root==f.root && state->page_table_refdec_batch.root==f.root,"actual begin binds both batches");
    if(partial){
        DestroyUserSpaceCallStats stats{};unsigned bound=0;
        while(state->phase==DestroyUserSpacePhase::COLLECT_FRAMES && ++bound<10000)advance_collect_frames_budgeted(*state,stats);
        if(pt_partial){while(state->phase==DestroyUserSpacePhase::FREE_DATA && ++bound<20000)advance_free_data_budgeted(*state,stats);}
        while(++bound<30000){
            if(pt_partial){advance_free_page_tables_budgeted(*state,stats);if(state->page_table_refdec_batch.count)break;}
            else{advance_free_data_budgeted(*state,stats);if(state->refdec_batch.count)break;}
        }
        expect(bound<30000,"actual partial walk reaches queued detached owner");unsigned before=ref_batches;
        destroy_user_space_budget_state_destroy(state);expect(ref_batches==before+1,"partial destructor flushes its one nonempty batch");expect(held_gates.empty(),"partial disposal releases gate after batch");return;
    }
    unsigned t=tlb_calls,b=ref_batches;expect(!destroy_user_space_budgeted(state,0),"max_steps0 makes no progress");expect(t==tlb_calls && b==ref_batches,"max_steps0 neither ACKs nor drops refs");
    unsigned rounds=0;while(!destroy_user_space_budgeted(state,step) && ++rounds<20000){}
    expect(rounds<20000,"actual budget walk completes");f.all_retired();expect(state->phase==DestroyUserSpacePhase::DONE,"budget reaches existing final phase");unsigned final=tlb_calls;destroy_user_space_budget_state_destroy(state);expect(tlb_calls==final,"DONE empty destructor adds no redundant barrier");expect(held_gates.empty(),"DONE destructor releases held gate");
}
int run_tests(){
    for(size_t n:{63U,64U,65U})raw_case(n,false);
    raw_case(65,true);
    budget_case(1,false,false);budget_case(64,false,false);budget_case(8192,false,true);
    budget_case(1,true,false);budget_case(1,true,true);
    {Fixture f(0);auto* s=new DestroyUserSpaceBudgetState{};unsigned t=tlb_calls,b=ref_batches;destroy_user_space_budget_state_destroy(s);expect(t==tlb_calls && b==ref_batches,"unstarted empty state permits null batch root");}
    {Fixture f(1);UserPagemapExclusiveScope gate(f.root);DestroyRefdecBatch b{};bind_for_direct_test(b,f.root);unsigned t=tlb_calls,r=ref_batches;destroy_refdec_batch_flush(b,nullptr,DestroyRefdecBatchKind::DATA);expect(t==tlb_calls && r==ref_batches,"empty flush performs no barrier or release");}
    {Fixture f(1);DestroyRefdecBatch b{};b.pages[0]=f.data[0];b.count=1;unsigned r=ref_batches;bool refused=false;try{destroy_refdec_batch_flush(b,nullptr,DestroyRefdecBatchKind::DATA);}catch(std::runtime_error const&){refused=true;}expect(refused && r==ref_batches && refs[f.data[0]]==1,"nonempty missing-root refuses before any ref decrement");}
    std::printf("checks=%u failures=%u ref_batches=%u tlb_calls=%u reachable_final_failures=%u missing_ack_failures=%u\n",checks,failures,ref_batches,tlb_calls,reachable_failures,ack_failures);
    return failures?3:0;
}
'''


EXTRA_TESTS = (Path(__file__).resolve().with_name("managed_huge_retirement_cases.inc")).read_text()


def generate(source):
    out = [PREFIX]
    for token in ["struct DestroyUserSpaceCallStats {", "enum class DestroyUserSpacePhase", "struct DestroyWalkFrame {",
                  "enum class LiveAllocatorFrameKind"]:
        out.append(extract(source, token, True))
    out.append(HELPERS)
    for token in ["auto live_allocator_kind_to_page_kind(", "auto classify_live_allocator_frame(",
                  "auto page_table_tree_contains_frame("]:
        out.append(extract(source, token))
    out.append(extract(source, "struct PageTableFrameSet {", True))
    constant = re.findall(r"^constexpr size_t DESTROY_REFDEC_BATCH_CAP = [^;]+;", source, re.M)
    assert len(constant) == 1, "unique actual batch capacity required"
    out.append(constant[0] + "\n")
    out.append(extract(source, "struct DestroyManagedCursor {", True))
    out.append(extract(source, "struct DestroyUserSpaceBudgetState {", True))
    out.append("using DestroyRefdecBatch=DestroyUserSpaceBudgetState::RefdecBatch;\n")
    out.append(extract(source, "enum class DestroyRefdecBatchKind", True))
    out.append(extract(source, "auto index_of("))
    out.append(extract(source, "class MutableUserEntrySlot {", True))
    out.append(extract(source, "enum class Pml2Walk", True))
    out.append(extract(source, "auto managed_pml2_slot_locked("))
    out.append(extract(source, "auto managed_root_matches_locked("))
    for token in ["void note_destroy_refdec_batch_stats(DestroyUserSpaceCallStats* stats, phys::PageRefBatchStats const& batch_stats,",
                  "void destroy_refdec_batch_flush(",
                  "class DestroyManagedCursorStorage {", "void destroy_refdec_batch_append(", "auto destroy_managed_leaf(",
                  "void destroy_validate_managed_leaf(", "void destroy_managed_handoff(", "void destroy_managed_drain(",
                  "void destroy_require_no_managed_owners(", "void destroy_refdec_batch_queue(", "void destroy_state_reset_walk(",
                  "auto destroy_state_push(", "auto destroy_state_top(", "void destroy_state_pop(",
                  "void destroy_state_flush_refdec_batch(", "void destroy_state_flush_page_table_refdec_batch(",
                  "void destroy_state_queue_refdec(", "void destroy_state_queue_page_table_refdec(",
                  "void collect_page_table_frames(", "auto advance_collect_frames_budgeted(", "auto advance_free_data_budgeted(",
                  "auto advance_free_page_tables_budgeted(", "void free_user_data_pages(", "void free_page_table_pages(",
                  "auto begin_prepared_destroy_user_space(", "auto destroy_user_space_budgeted(",
                  "void destroy_user_space_budget_state_destroy(", "void destroy_user_space(PageTable*"]:
        out.append(extract(source, token, token.startswith("class ")))
    out.append("void run_managed_tests();\n")
    out.append(TESTS.replace("    std::printf(\"checks=", "    run_managed_tests();\n    std::printf(\"checks="))
    out.append(EXTRA_TESTS)
    out.append("} // namespace test_kernel\nint main(){return test_kernel::run_tests();}\n")
    return "\n".join(out)


def pin(path):
    path = Path(path).resolve()
    first = path.stat()
    data = path.read_bytes()
    last = path.stat()
    identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
    if identity(first) != identity(last):
        raise RuntimeError(f"input changed while reading: {path}")
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                device=last.st_dev, inode=last.st_ino, mtime_ns=last.st_mtime_ns, ctime_ns=last.st_ctime_ns)


def mutated(source, kind):
    def once(old, new):
        pattern = r"\s+".join(re.escape(part) for part in old.split())
        if len(re.findall(pattern, source)) != 1:
            raise RuntimeError("actual mutation boundary changed: " + kind)
        return re.sub(pattern, lambda _match: new, source)
    if kind == "missing-handoff":
        return once("if (managed_huge_registry.advance_retired_handoff(cursor.leaf.handle, cursor.next, 1) != huge::Error::None) __builtin_trap();",
                    "/* negative control: omit actual registry handoff */")
    if kind == "duplicate-cursor":
        return once("++cursor.next;", "/* negative control: leave the same mapping ref in the cursor */")
    if kind == "flush-before-transfer":
        return once("destroy_refdec_batch_append(pages, batch.count, page);",
                    "destroy_refdec_batch_append(pages, batch.count, page);\n"
                    "        if (batch.count >= batch.pages.size()) destroy_refdec_batch_flush(batch, stats, DestroyRefdecBatchKind::DATA);")
    if kind == "early-cap":
        helper = extract(source, "void destroy_managed_handoff(")
        final = extract(helper, "if (cursor.next == huge::LEAF_PAGES)").rstrip()
        final_start = helper.index(final)
        call = r"destroy_refdec_batch_flush\(batch,\s*stats,\s*DestroyRefdecBatchKind::DATA\);"
        flush = r"if\s*\(batch.count\s*>=\s*batch.pages.size\(\)\)\s*" + call
        matches = list(re.finditer(flush, helper[:final_start]))
        if len(matches) != 2 or len(re.findall(call, final)) != 1:
            raise RuntimeError("early-cap actual completion boundary changed")
        before = matches[-1]
        moved = re.sub(call, "/* negative control: credit before final ACK/ref batch */", final, count=1)
        changed = helper[:before.start()] + moved + "\n    " + before.group() + helper[final_start+len(final):]
        if source.count(helper.rstrip()) != 1:
            raise RuntimeError("unique actual handoff helper required")
        return source.replace(helper.rstrip(), changed.rstrip())
    raise RuntimeError("unknown mutation")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--virt-source", type=Path)
    parser.add_argument("--kernel-overlay", type=Path)
    parser.add_argument("--cxx", default=os.environ.get("HOST_CXX"))
    parser.add_argument("--output", type=Path, help="fresh optional result directory; all compiler scratch is removed")
    parser.add_argument("--negative-controls", action="store_true")
    args = parser.parse_args()
    root = args.source_root.resolve()
    source_path = args.virt_source.resolve() if args.virt_source else root / "modules/kern/src/platform/mm/virt.opt.cpp"
    overlay = args.kernel_overlay.resolve() if args.kernel_overlay else root / "modules/kern/src"
    compiler = args.cxx or shutil.which("g++", path=os.defpath) or shutil.which("clang++", path=os.defpath)
    if not compiler:
        parser.error("existing Host compiler missing")
    compiler = Path(shutil.which(compiler) or compiler).resolve()
    if args.output:
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
    else:
        output = Path(tempfile.mkdtemp(prefix="wos-teardown-result-"))
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=output))
    commands, errors, emitted = [], [], []
    active = {}
    spawning = False
    pending_signal = None
    started = time.monotonic()
    selected = [Path(__file__).resolve(), Path(__file__).resolve().with_name("managed_huge_retirement_cases.inc"),
                source_path, overlay / "platform/mm/managed_huge_core.hpp", compiler]
    before = [pin(p) for p in selected]
    env = dict(os.environ, TMPDIR=str(scratch), TMP=str(scratch), TEMP=str(scratch),
               ASAN_OPTIONS="detect_leaks=0:halt_on_error=1", UBSAN_OPTIONS="halt_on_error=1")

    def interrupt(number, _frame):
        nonlocal pending_signal
        if spawning:
            pending_signal = number
        else:
            raise KeyboardInterrupt(f"signal {number}")

    previous_handlers = {number: signal.signal(number, interrupt) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def absent(group):
        try:
            os.killpg(group, 0)
            return False
        except ProcessLookupError:
            return True

    def stop(process):
        if not absent(process.pid):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        if not absent(process.pid):
            raise RuntimeError("owned compiler group survived cleanup")
        active.pop(process.pid, None)

    def run(label, argv, success=True):
        nonlocal spawning, pending_signal
        remaining = 150 - (time.monotonic() - started)
        used = sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file())
        if remaining <= 0 or used > 128 * 1024**2:
            raise RuntimeError("bounded compiler time/scratch exhausted")
        log = output / (label + ".log")
        when = time.monotonic()
        with log.open("wb") as stream:
            spawning = True
            try:
                process = subprocess.Popen([str(x) for x in argv], cwd=root, env=env,
                                           stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                active[process.pid] = process
            finally:
                spawning = False
            if pending_signal is not None:
                raise KeyboardInterrupt(f"signal {pending_signal}")
            try:
                code = process.wait(timeout=min(60, remaining))
            except BaseException:
                stop(process)
                raise
        group_absent = absent(process.pid)
        if group_absent:
            active.pop(process.pid, None)
        else:
            stop(process)
        data = log.read_text(errors="replace")
        commands.append(dict(label=label, argv=[str(x) for x in argv], exit=code,
                             seconds=time.monotonic()-when, leader_reaped=True, owned_group_absent=group_absent,
                             log=pin(log)))
        if not group_absent or (code == 0) != success:
            raise RuntimeError(f"{label}: unexpected exit {code}")
        if not success and ("ERROR: AddressSanitizer" in data or "runtime error:" in data):
            raise RuntimeError("negative control hit a sanitizer defect instead of the ownership oracle")
        if success and label == "actual-runtime":
            print(data, end="")

    try:
        source = source_path.read_text()
        common = [compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Wno-unused-parameter",
                  "-Wno-unused-function", "-fno-pie", "-no-pie", "-fsanitize=address,undefined",
                  "-fno-omit-frame-pointer", "-fno-strict-aliasing", "-I", overlay]
        variants = [("actual", source)]
        if args.negative_controls:
            variants += [(name, mutated(source, name)) for name in
                         ("early-cap", "duplicate-cursor", "missing-handoff", "flush-before-transfer")]
        for name, actual_source in variants:
            cpp, binary = scratch / (name + ".cpp"), scratch / name
            cpp.write_text(generate(actual_source))
            if name == "actual":
                dep = scratch / "actual.d"
                run("actual-dependencies", common + ["-M", "-MF", dep, "-MT", "unit", cpp])
                import shlex
                dependencies = [Path(x).resolve() for x in shlex.split(dep.read_text().replace("\\\n", " ").split(":", 1)[1])]
                emitted = [pin(p) for p in dict.fromkeys(dependencies) if p != cpp]
            run(name + "-compile", common + [cpp, "-o", binary])
            run(name + "-runtime", [binary], name == "actual")
    except BaseException as error:
        errors.append(type(error).__name__ + ": " + str(error))
    finally:
        # Cancellation must finish owned cleanup even if another signal arrives.
        for number in previous_handlers:
            signal.signal(number, signal.SIG_IGN)
        for process in list(active.values()):
            try:
                stop(process)
            except BaseException as error:
                errors.append("cleanup: " + str(error))
        after, emitted_after = [], []
        try:
            after = [pin(row["path"]) for row in before]
            emitted_after = [pin(row["path"]) for row in emitted]
            if before != after or emitted != emitted_after:
                errors.append("actual source/emitted dependencies changed")
        except BaseException as error:
            errors.append("input recheck: " + str(error))
        removed_bytes = 0
        preserved_scratch = None
        try:
            if active:
                errors.append("unproven owned group remains; preserve its scratch")
                preserved_scratch = str(scratch)
            else:
                removed_bytes = sum(p.stat().st_blocks * 512 for p in scratch.rglob("*") if p.is_file())
                shutil.rmtree(scratch)
        finally:
            for number, handler in previous_handlers.items():
                signal.signal(number, handler)
        result = dict(qualification="PASS_ACTUAL_EXTRACTED_TEARDOWN_ONLY" if not errors else "FAIL_EXTRACTED_TEARDOWN",
                      errors=errors, seconds=time.monotonic()-started, commands=commands,
                      selected_before=before, selected_after=after, emitted_inputs=emitted,
                      emitted_inputs_after=emitted_after, scratch_removed=not scratch.exists(),
                      scratch_allocated_bytes_removed=removed_bytes, owned_groups_remaining=list(active),
                      preserved_scratch=preserved_scratch, output_directory=str(output),
                      limits=["Explicit Host buffers/physical metadata/locks/root gates/ACK endpoints; no actual hardware or SMP ACK.",
                              "Real registry and actual raw/budget/dispose helpers; no public managed huge admission."])
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({key: result[key] for key in ("qualification", "errors", "seconds", "scratch_removed")}))
        if not args.output and not active:
            shutil.rmtree(output)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
