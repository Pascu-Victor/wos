#!/usr/bin/env python3
"""Run actual KASAN poison/unpoison policy with finite mapping/IRQ/write ports.

Actual helper, atomic flag, poison and unpoison decisions execute. IRQ asm,
translation, shadow layout and virtual writes are explicit Host endpoints.
Materializations count finite demand-zero publications, never physical credit.
No x86 shadow ABI, native IRQ/NMI/TLB or allocator accounting is qualified.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time
from file_mmap_registry_runtime_test import command, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
SOURCE = ROOT / 'modules/kern/src/sanitizer/kasan.cpp'
HEADER = ROOT / 'modules/kern/src/sanitizer/kasan.hpp'
VIRT_SOURCE = ROOT / 'modules/kern/src/platform/mm/virt.opt.cpp'
MM_HEADER = ROOT / 'modules/kern/src/platform/mm/mm.hpp'
ORIGINAL_UNPOISON = r'''void unpoison_range(const void* ptr, size_t size) {
    if (size == 0) {
        return;
    }

    auto const START = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t const END = START + size;
    // Whole granules need exactly one zero shadow byte per eight data bytes.
    // Kernel resume clears a large retired-stack extent with interrupts off;
    // use the excluded scalar/rep-stos runtime instead of an O0 per-granule walk.
    if ((START & 7U) == 0 && (size & 7U) == 0 && END > START) {
        std::memset(addr_to_shadow(START), SHADOW_ACCESSIBLE, size / 8);
        return;
    }
    for (uintptr_t granule = START & ~0x7ULL; granule < END; granule += 8) {
        uintptr_t const ACCESS_START = START > granule ? START : granule;
        uintptr_t const GRANULE_END = granule + 8;
        uintptr_t const ACCESS_END = END < GRANULE_END ? END : GRANULE_END;
        int8_t* shadow = addr_to_shadow(granule);
        if (ACCESS_START != granule || ACCESS_END == GRANULE_END) {
            *shadow = SHADOW_ACCESSIBLE;
        } else {
            *shadow = static_cast<int8_t>(ACCESS_END - granule);
        }
    }
}'''
ORIGINAL_UNPOISON_SHA256 = '4065325f14dc1428fa42be06100df43025a84ceba5d4203b7645c9e351683a7f'

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <utility>
namespace fixture {
constexpr size_t PAGE=4096,PAGES=8;
constexpr uintptr_t APP=0xffff800040000000ULL;
alignas(PAGE) std::array<int8_t,PAGE*PAGES>shadow{};
std::array<bool,PAGES>present{};
unsigned failures{},assertions{},cases{},queries{},zero_writes{},byte_writes{},materializations{},saves{},restores{},unknown_writes{},pml4_syncs{};
bool irq=true,external_owner{},early_null{},current_root_stale{};
std::function<void()>after_restore;
void check(bool v,const char*why){++assertions;if(!v&&failures++<24)std::fprintf(stderr,"FAIL %s\n",why);}
uintptr_t base(){return reinterpret_cast<uintptr_t>(shadow.data());}
uintptr_t app(size_t offset){return APP+offset*8;}
}
namespace ker::mod::mm {
namespace paging {constexpr uint64_t PAGE_SIZE=4096;struct PageTable{};}
namespace virt {constexpr uint64_t PADDR_INVALID=UINT64_MAX;paging::PageTable root;
auto get_kernel_pagemap()->paging::PageTable*{return fixture::early_null?nullptr:&root;}
auto translate(paging::PageTable*,uint64_t)->uint64_t;
}
}
static_assert(ker::mod::mm::paging::PAGE_SIZE == fixture::PAGE);
namespace ker::mod::kasan {
// Finite logical-to-Host layout only. Genuine addr_to_shadow formula is used;
// these endpoint constants are deliberately not the real kernel ABI addresses.
const uintptr_t SHADOW_OFFSET=fixture::base()-(fixture::APP>>3);
const uintptr_t SHADOW_REGION_BASE=fixture::base();
const uintptr_t SHADOW_REGION_END=fixture::base()+fixture::shadow.size();
// ACTUAL_SENTINELS
// ACTUAL_ADDR_TO_SHADOW
// ACTUAL_MAP_FLAG
void unpoison_range(const void*,size_t);
void poison_range(const void*,size_t,int8_t);
}
namespace fixture {
namespace kasan=ker::mod::kasan;
uint64_t irq_save_disable(){++saves;uint64_t f=irq?0x200:0;irq=false;return f;}
void irq_restore_enable(){
 check(!irq,"IRQ remains disabled until exact restore");
 check(!kasan::s_shadow_map_lock.test(std::memory_order_acquire)||external_owner,"own mapping flag released before IF restore");
 ++restores;irq=true;auto callback=std::move(after_restore);after_restore={};if(callback)callback();
}
void materialize(size_t page){
 check(page<PAGES,"bounded shadow write endpoint");if(page>=PAGES)return;
 if(!present[page]){std::memset(shadow.data()+page*PAGE,0,PAGE);present[page]=true;++materializations;}
}
void zero_shadow(void*ptr,int value,size_t size){
 ++zero_writes;
 check(!kasan::s_shadow_map_lock.test(std::memory_order_acquire)||external_owner,"virtual zero after own mapping flag release");
 check(value==0,"unpoison writes accessible zero");
 auto a=reinterpret_cast<uintptr_t>(ptr);
 if(a<base()||a>=base()+shadow.size()||size>base()+shadow.size()-a){++unknown_writes;return;}
 if(current_root_stale){
  bool const held=kasan::s_shadow_map_lock.test_and_set(std::memory_order_acquire);
  check(!held,"present kernel shadow can fault only after own map flag release");
  if(!held)kasan::s_shadow_map_lock.clear(std::memory_order_release);
  ++pml4_syncs;current_root_stale=false;  // Finite existing virtual-fault/PML4 endpoint.
 }
 if(size){size_t first=(a-base())/PAGE,last=(a-base()+size-1)/PAGE;
  for(size_t p=first;p<=last;++p)materialize(p);}
 std::memset(ptr,value,size);
}
void store_shadow(int8_t*ptr,int8_t value){
 ++byte_writes;
 check(!kasan::s_shadow_map_lock.test(std::memory_order_acquire)||external_owner,"virtual byte store after own mapping flag release");
 auto a=reinterpret_cast<uintptr_t>(ptr);
 if(a<base()||a>=base()+shadow.size()){++unknown_writes;return;}
 materialize((a-base())/PAGE);*ptr=value;
}
void reset(){
 ++cases;check(!kasan::s_shadow_map_lock.test(std::memory_order_acquire),"prior call returned mapping flag");
 kasan::s_shadow_map_lock.clear(std::memory_order_release);shadow.fill(0);present.fill(false);
 queries=zero_writes=byte_writes=materializations=saves=restores=unknown_writes=pml4_syncs=0;
 irq=true;external_owner=early_null=current_root_stale=false;after_restore={};
}
void set_present(size_t p,int8_t value){present[p]=true;std::memset(shadow.data()+p*PAGE,value,PAGE);}
void clear(size_t offset,size_t bytes){kasan::unpoison_range(reinterpret_cast<void*>(app(offset)),bytes*8);}
bool bytes_equal(size_t begin,size_t end,int8_t value){
 for(size_t i=begin;i<end;++i)if(shadow[i]!=value)return false;return true;
}
void publish_and_poison(size_t offset){
 check(!kasan::s_shadow_map_lock.test_and_set(std::memory_order_acquire),"finite publisher owns same mapping flag");
 materialize(offset/PAGE);kasan::s_shadow_map_lock.clear(std::memory_order_release);
 kasan::poison_range(reinterpret_cast<void*>(app(offset)),8,kasan::SHADOW_HEAP_FREED);
}
}
namespace ker::mod::mm::virt {
auto translate(paging::PageTable*pt,uint64_t address)->uint64_t{
 fixture::check(pt==&root&&!fixture::irq,"presence query uses kernel root with IRQ masked");
 fixture::check(ker::mod::kasan::s_shadow_map_lock.test(std::memory_order_acquire)&&!fixture::external_owner,
                "presence query owns existing map flag");
 ++fixture::queries;
 if(address<fixture::base()||address>=fixture::base()+fixture::shadow.size())return PADDR_INVALID;
 auto index=(address-fixture::base())/fixture::PAGE;
 return fixture::present[index]?0x1000+index*fixture::PAGE:PADDR_INVALID;
}
}
namespace ker::mod::kasan {
// ACTUAL_HELPER
// ACTUAL_POISON
// ACTUAL_UNPOISON
}
'''

CASES = r'''
namespace fixture {
void run(){
 // 1: absent whole granule never writes or materializes zero-only shadow.
 reset();clear(19,1);
 check(queries==1&&saves==1&&restores==1,"absent page presence checked once");
 check(!present[0]&&materializations==0&&zero_writes==0,"absent zero keeps shadow unmapped");
 check(irq,"original enabled IF restored");
 // 2: split four clipped shadow pages without touching any absent page.
 reset();clear(PAGE-2,2*PAGE+5);
 check(queries==4&&zero_writes==0&&materializations==0,"absent multi-page chunks are bounded and skipped");
 // 3: present poison cleared only within clipped span, neighboring poison kept.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);clear(13,11);
 check(bytes_equal(13,24,0),"present poison is cleared");
 check(bytes_equal(0,13,kasan::SHADOW_HEAP_FREED)&&bytes_equal(24,PAGE,kasan::SHADOW_HEAP_FREED),
       "clipped shadow neighbors retain poison");
 check(queries==1&&zero_writes==1&&materializations==0,"present page uses original virtual zero");
 // 4: absent middle page stays absent between two clipped poisoned pages.
 reset();set_present(0,kasan::SHADOW_POISONED);set_present(2,kasan::SHADOW_POISONED);clear(PAGE-3,PAGE+12);
 check(queries==3&&zero_writes==2&&materializations==0&&!present[1],"mixed range skips only absent shadow pages");
 check(bytes_equal(PAGE-3,PAGE,0)&&bytes_equal(2*PAGE,2*PAGE+9,0)&&shadow[PAGE-4]==kasan::SHADOW_POISONED&&
       shadow[2*PAGE+9]==kasan::SHADOW_POISONED,"mixed clipped spans preserve poison neighbors");
 // 5: exact last byte of a shadow page never reaches the next page.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);clear(PAGE-1,1);
 check(queries==1&&shadow[PAGE-1]==0&&shadow[PAGE-2]==kasan::SHADOW_HEAP_FREED&&!present[1],"page-edge clipping is exact");
 // 6: busy TRY returns immediately to original write with external flag intact.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);external_owner=true;(void)kasan::s_shadow_map_lock.test_and_set();clear(31,1);
 check(queries==0&&zero_writes==1&&materializations==0&&shadow[31]==0&&kasan::s_shadow_map_lock.test(),"busy map flag falls back without query or ownership theft");
 check(irq&&saves==1&&restores==1,"busy path restores enabled IF");kasan::s_shadow_map_lock.clear();external_owner=false;
 // 7: busy fallback also preserves disabled IF.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);irq=false;external_owner=true;(void)kasan::s_shadow_map_lock.test_and_set();clear(31,1);
 check(!irq&&saves==1&&restores==0&&queries==0,"busy path never enables original disabled IF");
 kasan::s_shadow_map_lock.clear();external_owner=false;
 // 8: successful present query preserves disabled IF while zeroing after unlock.
 reset();irq=false;set_present(0,kasan::SHADOW_HEAP_FREED);clear(7,1);
 check(!irq&&saves==1&&restores==0&&queries==1&&shadow[7]==0,"present path preserves disabled IF");
 // 9: another publication/poison after locked absence must remain poisoned.
 reset();bool raced=false;after_restore=[&]{raced=true;publish_and_poison(21);};clear(21,1);
 check(raced&&queries==1&&materializations==1&&zero_writes==0&&shadow[21]==kasan::SHADOW_HEAP_FREED,
       "absence linearizes before later mapping and poison");after_restore={};
 // 10: real nonzero poison stores demand-materialize and retain their sentinels.
 reset();kasan::poison_range(reinterpret_cast<void*>(app(64)),12*8,kasan::SHADOW_HEAP_FREED);
 check(materializations==1&&byte_writes==12&&bytes_equal(64,76,kasan::SHADOW_HEAP_FREED),"actual freed poison is retained");
 clear(65,10);check(shadow[64]==kasan::SHADOW_HEAP_FREED&&bytes_equal(65,75,0)&&shadow[75]==kasan::SHADOW_HEAP_FREED,
                    "reuse clears exact old poison and retains surrounding UAF guards");
 // 11: partial trailing granule keeps the existing first-N encoding.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);
 kasan::unpoison_range(reinterpret_cast<void*>(app(200)),17);
 check(queries==0&&zero_writes==0&&byte_writes==3&&shadow[200]==0&&shadow[201]==0&&shadow[202]==1&&
       shadow[203]==kasan::SHADOW_HEAP_FREED,"partial trailing redzone encoding unchanged");
 // 12: preserve the existing unaligned-prefix behavior exactly.
 reset();set_present(0,kasan::SHADOW_POISONED);
 kasan::unpoison_range(reinterpret_cast<void*>(app(200)+1),7);
 check(queries==0&&byte_writes==1&&shadow[200]==0&&shadow[201]==kasan::SHADOW_POISONED,"partial prefix stays on original store path");
 // 13: partial poison preserves accessible prefix encoding.
 reset();kasan::unpoison_range(reinterpret_cast<void*>(app(200)),24);
 kasan::poison_range(reinterpret_cast<void*>(app(200)+17),7,kasan::SHADOW_HEAP_RREDZONE);
 check(shadow[200]==0&&shadow[201]==0&&shadow[202]==1,"actual partial poison retains first17 accessible bytes");
 // 14: zero-valued poison delegates to the actual implicit-zero unpoison path.
 reset();kasan::poison_range(reinterpret_cast<void*>(app(33)),8,kasan::SHADOW_ACCESSIBLE);
 check(queries==1&&materializations==0&&zero_writes==0&&!present[0],"zero poison uses actual unpoison decision");
 // 15: early null kernel root keeps the legacy virtual write path.
 reset();early_null=true;clear(9,1);
 check(queries==0&&saves==0&&zero_writes==1&&materializations==1,"early null root falls back to original write");
 // 16: nonkernel address keeps original fallback attempt, not absence skipping.
 reset();kasan::unpoison_range(reinterpret_cast<void*>(uintptr_t{0x10000000000}),8);
 check(queries==0&&saves==0&&zero_writes==1&&unknown_writes==1,"nonkernel address preserves original write attempt");
 // 17: finite out-of-shadow layout span keeps original fallback attempt.
 reset();clear(PAGE*PAGES,1);
 check(queries==0&&saves==0&&zero_writes==1&&unknown_writes==1,"out-of-shadow span preserves original write attempt");
 // 18: application overflow never enters the new aligned branch.
 reset();kasan::unpoison_range(reinterpret_cast<void*>(UINTPTR_MAX-7),16);
 check(queries==0&&saves==0&&zero_writes==0&&byte_writes==0,"overflow retains complete original behavior");
 // 19: zero length is still a complete no-op.
 reset();kasan::unpoison_range(reinterpret_cast<void*>(app(3)),0);
 check(queries==0&&saves==0&&zero_writes==0&&byte_writes==0,"zero size retains complete original behavior");
 // 20: whole shadow page clearing does not clear adjacent present pages.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);set_present(1,kasan::SHADOW_HEAP_FREED);set_present(2,kasan::SHADOW_HEAP_FREED);
 clear(PAGE,PAGE);
 check(bytes_equal(PAGE,2*PAGE,0)&&bytes_equal(0,PAGE,kasan::SHADOW_HEAP_FREED)&&
       bytes_equal(2*PAGE,3*PAGE,kasan::SHADOW_HEAP_FREED),"whole shadow page preserves both neighboring poisoned pages");
 // 21: kernel-present/current-root-stale must retain the virtual fault endpoint.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);current_root_stale=true;clear(71,1);
 check(queries==1&&zero_writes==1&&pml4_syncs==1&&materializations==0&&!current_root_stale&&shadow[71]==0,
       "present kernel shadow keeps current-PML4 virtual fault synchronization");
 // 22: preserve every existing nonzero sentinel through actual poison stores.
 reset();for(int8_t value:{kasan::SHADOW_HEAP_LREDZONE,kasan::SHADOW_HEAP_RREDZONE,kasan::SHADOW_HEAP_FREED,
                          kasan::SHADOW_GLOBAL_REDZONE,kasan::SHADOW_POISONED}){
  kasan::poison_range(reinterpret_cast<void*>(app(300)),8,value);
  check(shadow[300]==value,"all original nonzero shadow sentinels remain writable");
  clear(300,1);check(shadow[300]==0,"full-granule reuse clears every prior nonzero sentinel");
 }
 check(!kasan::s_shadow_map_lock.test(),"all calls release their own mapping flag");
 std::printf("%s kasan_shadow cases=%u assertions=%u failures=%u\n",failures?"FAIL":"PASS",cases,assertions,failures);
}
}
int main(){fixture::run();return fixture::failures?1:0;}
'''


def definition(source: str, anchor: str) -> str:
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda m: ''.join('\n' if c == '\n' else ' ' for c in m[0]), source)
    start = source.index(anchor)
    opening = clean.index('{', start)
    depth = 0
    for end in range(opening, len(clean)):
        if clean[end] == '{': depth += 1
        elif clean[end] == '}':
            depth -= 1
            if depth == 0:
                return source[start:end + 1]
    raise ValueError('unterminated complete actual definition: ' + anchor)


def generate(source: str, header: str, control: str) -> tuple[str, dict]:
    if hashlib.sha256(ORIGINAL_UNPOISON.encode()).hexdigest() != ORIGINAL_UNPOISON_SHA256:
        raise ValueError('retained complete original zero-fill body hash changed')
    helper = definition(source, 'static auto clear_aligned_shadow_without_materializing(')
    poison = definition(source, 'void poison_range(')
    unpoison = definition(source, 'void unpoison_range(')
    actual = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
              [('helper', helper), ('poison', poison), ('unpoison', unpoison)]}
    if control == 'original-zero-fill':
        helper = ''
        unpoison = ORIGINAL_UNPOISON
    elif control == 'whole-page-clear':
        original = 'std::memset(reinterpret_cast<void*>(shadow), SHADOW_ACCESSIBLE, CHUNK);'
        wrong = 'std::memset(reinterpret_cast<void*>(shadow & ~(mm::paging::PAGE_SIZE - 1)), SHADOW_ACCESSIBLE, mm::paging::PAGE_SIZE);'
        if helper.count(original) != 1:
            raise ValueError('whole-page mutation must change one exact original zero primitive')
        helper = helper.replace(original, wrong)
    elif control != 'current':
        raise ValueError('unknown control')
    # Finite privileged-instruction primitives only; no branch/atomic edits.
    save = 'asm volatile("pushfq; popq %0; cli" : "=r"(irq_flags) : : "memory");'
    restore = 'asm volatile("sti" : : : "memory");'
    if helper:
        if helper.count(save) != 1 or helper.count(restore) != 1:
            raise ValueError('exact privileged IRQ endpoint selection changed')
        helper = helper.replace(save, 'irq_flags = fixture::irq_save_disable();')
        helper = helper.replace(restore, 'fixture::irq_restore_enable();')
    # Virtual writes are finite endpoints. Keep each actual value expression and
    # decision intact, including every nonzero/partial byte store.
    expected_stores = {
        '*shadow = value;': 1,
        '*shadow = static_cast<int8_t>(POISON_START - granule);': 1,
        '*shadow = SHADOW_ACCESSIBLE;': 1,
        '*shadow = static_cast<int8_t>(ACCESS_END - granule);': 1,
    }
    bodies = [helper, poison, unpoison]
    joined = '\n'.join(bodies)
    for store, count in expected_stores.items():
        if joined.count(store) != count:
            raise ValueError('exact actual byte-store endpoint count changed: ' + store)
        value = store[len('*shadow = '):-1]
        bodies = [body.replace(store, 'fixture::store_shadow(shadow, ' + value + ');') for body in bodies]
    bodies = [body.replace('std::memset(', 'fixture::zero_shadow(') for body in bodies]
    flag = re.findall(r'^static std::atomic_flag s_shadow_map_lock = ATOMIC_FLAG_INIT;[^\n]*', source, re.M)
    formula = re.findall(r'^inline auto addr_to_shadow\([^\n]+', header, re.M)
    sentinels = re.findall(r'^constexpr int8_t SHADOW_[^\n]+', header, re.M)
    if len(flag) != 1 or len(formula) != 1 or len(sentinels) != 6:
        raise ValueError('actual map flag/address formula/sentinels selection changed')
    text = PRELUDE
    for name, body in {'ACTUAL_SENTINELS': '\n'.join(sentinels), 'ACTUAL_ADDR_TO_SHADOW': formula[0],
                       'ACTUAL_MAP_FLAG': flag[0], 'ACTUAL_HELPER': bodies[0],
                       'ACTUAL_POISON': bodies[1], 'ACTUAL_UNPOISON': bodies[2]}.items():
        marker = '// ' + name
        if text.count(marker) != 1:
            raise ValueError('finite source seam count changed: ' + name)
        text = text.replace(marker, body)
    actual['ported_bodies'] = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
                               zip(('helper', 'poison', 'unpoison'), bodies)}
    return text + CASES, actual



# Separate group: the new backing policy and complete actual pool publication
# body execute with finite page-mapping, physical-address and IRQ endpoints.
# These endpoints do not claim privileged instructions, real PTEs or owner charge.
BACKING_PRELUDE = r'''
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#define WOS_KASAN 1
#define WOS_SELFTEST 1
namespace fixture {
constexpr size_t PAGE=4096,PAGES=80;
unsigned failures{},assertions{},cases{},calls{},materializations{},saves{},restores{},data_pages{},register_calls{},fail_call{};
bool irq=true,early_null{},flushed{},expect_unpublished{};
std::array<bool,PAGES>present{};
std::array<std::array<int8_t,PAGE>,PAGES>shadow{};
std::array<bool,520>data_present{};
std::array<uint64_t,256>addresses{};
void check(bool v,const char*why){++assertions;if(!v&&failures++<32)std::fprintf(stderr,"FAIL %s\n",why);}
uint64_t irq_save_disable(){++saves;uint64_t f=irq?0x200:0;irq=false;return f;}
void irq_restore_enable(){check(!irq,"backing IF masked until exact restore");++restores;irq=true;}
void clear_existing(uint64_t);
}
namespace ker::mod::mm {
// ACTUAL_MM_CONSTANTS
namespace addr {using vaddr_t=uint64_t;using paddr_t=uint64_t;
auto get_phys_pointer(vaddr_t p)->void*{return reinterpret_cast<void*>(p);}}
namespace paging {constexpr uint64_t PAGE_SIZE=4096;struct PageTable{};}
namespace virt {
using vaddr_t=addr::vaddr_t;using paddr_t=addr::paddr_t;
constexpr vaddr_t GUARDED_KERNEL_STACK_ARENA_BASE=ker::mod::mm::GUARDED_KERNEL_STACK_ARENA_BASE;
constexpr uint64_t GUARDED_KERNEL_STACK_STRIDE=ker::mod::mm::KERNEL_STACK_SLOT_STRIDE;
struct PageMapBatch{};
paging::PageTable root;
paging::PageTable*kernel_pagemap=&root;
bool guarded_kernel_stack_initialized=false;size_t guarded_kernel_stack_capacity=0;
auto get_kernel_pagemap()->paging::PageTable*{return fixture::early_null?nullptr:kernel_pagemap;}
auto kernel_data_page_flags()->uint64_t{return 3;}
void init_page_map_batch(PageMapBatch*,paging::PageTable*pt,uint64_t flags){fixture::check(pt==&root&&flags==3,"actual integration finite root/flags");}
void map_page_batched(PageMapBatch*,vaddr_t address,paddr_t,uint64_t){
 fixture::check(!guarded_kernel_stack_initialized&&guarded_kernel_stack_capacity==0,"data mapping precedes pool publication");
 if(address<GUARDED_KERNEL_STACK_ARENA_BASE||(address-GUARDED_KERNEL_STACK_ARENA_BASE)%4096){fixture::check(false,"bounded data mapping address");return;}
 size_t index=(address-GUARDED_KERNEL_STACK_ARENA_BASE)/4096;
 fixture::check(index<fixture::data_present.size(),"finite data map bound");
 if(index<fixture::data_present.size()){fixture::check(!fixture::data_present[index],"each actual data page mapped once");fixture::data_present[index]=true;++fixture::data_pages;}
}
void flush_page_map_batch(PageMapBatch*){fixture::flushed=true;}
void selftest_register_kernel_page_tables(){++fixture::register_calls;}
struct log {template<class...T>static void info(const char*,T...){}};
}
}
namespace ker::mod::kasan {
// ACTUAL_SHADOW_CONSTANTS
// ACTUAL_BACKING_FORMULA
auto handle_shadow_fault(uint64_t)->bool;
// ACTUAL_BACKING_HELPER
}
namespace fixture {
namespace mm=ker::mod::mm;namespace kasan=ker::mod::kasan;
uintptr_t app(size_t shadow_offset){return mm::GUARDED_KERNEL_STACK_ARENA_BASE+shadow_offset*8;}
uintptr_t base(){return reinterpret_cast<uintptr_t>(kasan::addr_to_shadow(mm::GUARDED_KERNEL_STACK_ARENA_BASE));}
size_t index(uint64_t address){
 check(address>=base()&&(address-base())%PAGE==0,"page-aligned real-formula shadow endpoint");
 auto result=static_cast<size_t>((address-base())/PAGE);check(result<PAGES,"finite backing address bound");return result;
}
void clear_existing(uint64_t address){auto i=index(address);if(i<PAGES)shadow[i].fill(0);}
void reset(){
 ++cases;irq=true;early_null=flushed=expect_unpublished=false;
 calls=materializations=saves=restores=data_pages=register_calls=fail_call=0;
 present.fill(false);for(auto&p:shadow)p.fill(0);data_present.fill(false);addresses.fill(0);
 mm::virt::kernel_pagemap=&mm::virt::root;mm::virt::guarded_kernel_stack_initialized=false;mm::virt::guarded_kernel_stack_capacity=0;
}
void set_present(size_t i,int8_t value){present[i]=true;shadow[i].fill(value);}
bool covers(size_t n){for(size_t i=0;i<n;++i)if(!present[i])return false;return true;}
bool ensure(uintptr_t start,size_t size){return kasan::ensure_shadow_backing(reinterpret_cast<void*>(start),size);}
}
namespace ker::mod::kasan {
auto handle_shadow_fault(uint64_t address)->bool{
 fixture::check(!fixture::irq,"backing mapper called with IF disabled");++fixture::calls;
 if(fixture::calls<=fixture::addresses.size())fixture::addresses[fixture::calls-1]=address;
 if(fixture::expect_unpublished){fixture::check(fixture::flushed,"shadow backing follows actual data batch flush");
 fixture::check(!mm::virt::guarded_kernel_stack_initialized&&mm::virt::guarded_kernel_stack_capacity==0,"shadow backing precedes pool publication");}
 auto i=fixture::index(address);if(i>=fixture::PAGES||fixture::calls==fixture::fail_call)return false;
 if(!fixture::present[i]){fixture::shadow[i].fill(0);fixture::present[i]=true;++fixture::materializations;}
 return true;
}
}
namespace ker::mod::mm::virt {
// ACTUAL_GUARDED_INTEGRATION
}
'''

BACKING_CASES = r'''
namespace fixture {
void run(){
 // 1: a two-byte application span crosses exactly one shadow-page boundary.
 reset();check(ensure(app(PAGE-1)+7,2),"cross-page backing succeeds");
 check(calls==2&&materializations==2&&covers(2)&&!present[2],"backing covers exact page edge");
 check(addresses[0]==base()&&addresses[1]==base()+PAGE,"backing uses exact real-formula page addresses");
 check(irq&&saves==2&&restores==2,"successful backing restores prior IF");
 // 2: unaligned application bytes retain existing partial-granule poison.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);shadow[0][1]=3;
 check(ensure(app(0)+7,2),"unaligned backing succeeds");
 check(calls==1&&materializations==0&&shadow[0][0]==kasan::SHADOW_HEAP_FREED&&shadow[0][1]==3,"backing retains partial-granule poison");
 // 3: the same poison oracle rejects a destructive whole-shadow clear.
 reset();set_present(0,kasan::SHADOW_HEAP_FREED);set_present(1,kasan::SHADOW_HEAP_RREDZONE);shadow[1][31]=3;
 check(ensure(app(0),PAGE*16),"present range backing succeeds");
 check(materializations==0&&shadow[0][0]==kasan::SHADOW_HEAP_FREED&&shadow[0][PAGE-1]==kasan::SHADOW_HEAP_FREED&&
       shadow[1][0]==kasan::SHADOW_HEAP_RREDZONE&&shadow[1][31]==3,"range backing preserves present poison");
 // 4: repeating a fully backed range allocates no further endpoint pages.
 reset();check(ensure(app(0),PAGE*24)&&ensure(app(0),PAGE*24),"repeated backing succeeds");
 check(calls==6&&materializations==3&&covers(3)&&!present[3],"repeat backing has no growth");
 // 5: invalid inputs refuse before any mapper or privileged endpoint.
 reset();check(!ensure(0,1)&&!ensure(0x800000000000ULL,1)&&!ensure(0xffff7fffffffffffULL,1)&&
               !ensure(UINTPTR_MAX-15,32)&&!ensure(app(0),SIZE_MAX)&&!ensure(app(0),0),"invalid backing ranges refuse");
 early_null=true;check(!ensure(app(0),8),"uninitialized root refuses");
 check(calls==0&&saves==0&&restores==0&&irq,"invalid range leaves mapping and IF untouched");
 // 6: partial OOM stops exactly at failure and preserves prior enabled IF.
 reset();fail_call=3;check(!ensure(app(0),PAGE*32),"partial backing failure propagates");
 check(calls==3&&materializations==2&&covers(2)&&!present[2]&&!present[3],"partial failure retains only completed permanent backing");
 check(irq&&saves==3&&restores==3,"failed backing restores enabled IF");
 // 7: the same failure never enables a caller's disabled IF.
 reset();irq=false;fail_call=3;check(!ensure(app(0),PAGE*32),"IF-off partial failure propagates");
 check(!irq&&calls==3&&saves==3&&restores==0,"failed backing preserves disabled IF");
 // 8: an exact exclusive boundary does not visit the following page.
 reset();check(ensure(app(0),PAGE*8),"one whole shadow page succeeds");
 check(calls==1&&materializations==1&&present[0]&&!present[1],"exclusive range endpoint is exact");
 // 9: execute the WHOLE actual integration, with real512KiB stack geometry.
 reset();expect_unpublished=true;
 check(mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(0x100000000ULL),4),"actual guarded integration succeeds");
 expect_unpublished=false;
 check(mm::virt::guarded_kernel_stack_initialized&&mm::virt::guarded_kernel_stack_capacity==4&&covers(65)&&!present[65],
       "pool shadow backed before publication");
 check(calls==65&&materializations==65&&data_pages==512&&register_calls==1,"actual integration covers finite four-slot geometry");
 for(size_t guard:{size_t{0},size_t{129},size_t{130},size_t{259},size_t{260},size_t{389},size_t{390},size_t{519}})
  check(!data_present[guard],"original guard data PTE remains absent");
 check(data_present[1]&&data_present[128]&&data_present[391]&&data_present[518],"usable stack data endpoints remain mapped");
 // 10: the actual pool refuses publication after a partial mapping failure.
 reset();expect_unpublished=true;fail_call=3;
 check(!mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(0x100000000ULL),4),"pool backing failure propagates");
 expect_unpublished=false;
 check(!mm::virt::guarded_kernel_stack_initialized&&mm::virt::guarded_kernel_stack_capacity==0&&calls==3&&materializations==2,
       "failed backing never publishes pool");
 check(irq&&saves==3&&restores==3&&register_calls==0,"integration failure restores IF before refusing");
 // 11: original integration admission still rejects every invalid input axis.
 reset();check(!mm::virt::init_guarded_kernel_stack_pool(nullptr,4)&&
               !mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(1),0)&&
               !mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(1),257)&&
               !mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(1),SIZE_MAX),"original integration admission retained");
 mm::virt::kernel_pagemap=nullptr;
 check(!mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(1),4),"original null pagemap admission retained");
 check(calls==0&&data_pages==0&&!mm::virt::guarded_kernel_stack_initialized,"invalid pool remains unpublished and unmapped");
 // 12: repeat the API after publication, retaining poison and page count.
 reset();expect_unpublished=true;check(mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(0x100000000ULL),4),"repeat fixture integration succeeds");
 expect_unpublished=false;shadow[10][5]=kasan::SHADOW_HEAP_FREED;shadow[10][6]=3;
 check(ensure(app(0),4*mm::KERNEL_STACK_SLOT_STRIDE),"published range reensure succeeds");
 check(materializations==65&&calls==130&&shadow[10][5]==kasan::SHADOW_HEAP_FREED&&shadow[10][6]==3,"published reensure preserves poison and count");
 check(!mm::virt::init_guarded_kernel_stack_pool(reinterpret_cast<void*>(1),4),"original repeated publication refuses");
 check(irq,"all backing cases return exact enabled IF");
 std::printf("%s kasan_backing cases=%u assertions=%u failures=%u\n",failures?"FAIL":"PASS",cases,assertions,failures);
}
}
int main(){fixture::run();return fixture::failures?1:0;}
'''

ORIGINAL_GUARDED_INTEGRATION_SHA256 = '1a76a531e389e1f6407c6b885c1760f3b01db251684eecdb7cc374cd00f3f7cf'


def generate_backing(source: str, header: str, virt: str, mm_header: str, control: str) -> tuple[str, dict]:
    backing = definition(source, 'auto ensure_shadow_backing(')
    integration = definition(virt, 'auto init_guarded_kernel_stack_pool(')
    actual = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
              [('backing', backing), ('guarded_integration', integration)]}
    call = '''#ifdef WOS_KASAN
    // The pool is permanent: back its complete actual-capacity shadow before
    // either pool publishes a slot. Explicit mapping is safe before the IDT;
    // touching shadow here to trigger a fault would not be. Preserve poison.
    if (!kasan::ensure_shadow_backing(reinterpret_cast<void*>(GUARDED_KERNEL_STACK_ARENA_BASE), ARENA_BYTES)) {
        return false;
    }
#endif
'''
    if control == 'missing-preparation':
        if integration.count(call) != 1:
            raise ValueError('exact actual preparation block changed')
        integration = integration.replace(call, '')
        if hashlib.sha256(integration.encode()).hexdigest() != ORIGINAL_GUARDED_INTEGRATION_SHA256:
            raise ValueError('missing-preparation control is not the genuine whole original integration')
    elif control == 'destructive-clear':
        mapper = 'bool const READY = handle_shadow_fault(shadow_page);'
        if backing.count(mapper) != 1:
            raise ValueError('exact current range mapper endpoint changed')
        backing = backing.replace(mapper, mapper + '\n        if (READY) fixture::clear_existing(shadow_page);')
    elif control != 'current':
        raise ValueError('unknown backing control')
    save = 'asm volatile("pushfq; popq %0; cli" : "=r"(irq_flags) : : "memory");'
    restore = 'asm volatile("sti" : : : "memory");'
    if backing.count(save) != 1 or backing.count(restore) != 1:
        raise ValueError('exact backing IRQ primitive selection changed')
    backing = backing.replace(save, 'irq_flags = fixture::irq_save_disable();')
    backing = backing.replace(restore, 'fixture::irq_restore_enable();')
    constants = []
    for name in ('KERNEL_STACK_SIZE', 'KERNEL_STACK_POOL_CAPACITY', 'KERNEL_STACK_GUARD_SIZE', 'KERNEL_STACK_SLOT_STRIDE',
                 'GUARDED_KERNEL_STACK_ARENA_BASE', 'GUARDED_KERNEL_STACK_ARENA_END'):
        rows = re.findall(r'^inline constexpr (?:size_t|uintptr_t) ' + name + r'\s*=[^;]+;', mm_header, re.M)
        if len(rows) != 1:
            raise ValueError('one actual guarded geometry constant required: ' + name)
        constants.append(rows[0])
    formula = re.findall(r'^inline auto addr_to_shadow\([^\n]+', header, re.M)
    shadows = re.findall(r'^constexpr uint64_t SHADOW_[^\n]+', header, re.M)
    sentinels = re.findall(r'^constexpr int8_t SHADOW_[^\n]+', header, re.M)
    if len(formula) != 1 or len(shadows) != 3 or len(sentinels) != 6:
        raise ValueError('actual address formula/shadow constants changed')
    text = BACKING_PRELUDE
    for name, body in {'ACTUAL_MM_CONSTANTS': '\n'.join(constants), 'ACTUAL_SHADOW_CONSTANTS': '\n'.join(shadows + ['[[maybe_unused]] ' + row for row in sentinels]),
                       'ACTUAL_BACKING_FORMULA': formula[0], 'ACTUAL_BACKING_HELPER': backing,
                       'ACTUAL_GUARDED_INTEGRATION': integration}.items():
        marker = '// ' + name
        if text.count(marker) != 1:
            raise ValueError('one finite backing seam required: ' + name)
        text = text.replace(marker, body)
    actual['ported_bodies'] = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
                               [('backing', backing), ('guarded_integration', integration)]}
    actual['finite_endpoints'] = ['privileged IRQ save/restore', 'existing shadow mapper allocation/presence/failure',
                                  'kernel-root/physical-address and data-PTE batching', 'boot selftest registry/log sink']
    actual['original_guarded_control_sha256'] = ORIGINAL_GUARDED_INTEGRATION_SHA256
    return text + BACKING_CASES, actual


# Separate whole-init group. Linker addresses, logging, fatal halt, IRQ and
# existing mapper are finite endpoints; the actual init and backing decisions
# execute. This does not claim native PTE, boot ordering or physical accounting.
ORIGINAL_IMAGE_INIT = 'void init() {\n    // The compiler\'s shadow accesses for global variables and kernel static\n    // code will fault into handle_shadow_fault() and get demand-zeroed pages.\n    // For global redzones (emitted by -asan-globals=1), Clang calls\n    // __asan_register_globals() which we stub out below — we rely on the\n    // demand-fault mechanism instead of pre-populating global shadows.\n    log::info("initialized (shadow base=0x%lx)", SHADOW_OFFSET);\n}'
ORIGINAL_IMAGE_INIT_SHA256 = 'eb75a4df29170f98d3699275d03fbad2292d33c0a51cedd404aed380debf29f2'

IMAGE_INIT_PRELUDE = r'''
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
namespace fixture {
constexpr size_t PAGE=4096,PAGES=8;
constexpr uintptr_t IMAGE_BASE=0xffffffff80000000ULL;
unsigned failures{},assertions{},cases{},calls{},materializations{},saves{},restores{},fatals{},errors{},infos{},fail_call{};
bool irq=true,early_null{};
uintptr_t image_start=IMAGE_BASE,image_end=IMAGE_BASE+PAGE*8;
std::array<bool,PAGES>present{};
std::array<std::array<int8_t,PAGE>,PAGES>shadow{};
std::array<uint64_t,32>addresses{};
struct Fatal{};
void check(bool value,const char*why){++assertions;if(!value&&failures++<64)std::fprintf(stderr,"FAIL %s\n",why);}
uint64_t irq_save_disable(){++saves;uint64_t flags=irq?0x200:0;irq=false;return flags;}
void irq_restore_enable(){check(!irq,"image mapper keeps IF masked until restore");++restores;irq=true;}
}
[[noreturn]] void hcf(){++fixture::fatals;throw fixture::Fatal{};}
namespace ker::mod::mm::paging {constexpr uint64_t PAGE_SIZE=4096;}
namespace ker::mod::mm::virt {
struct PageTable{};PageTable root;
auto get_kernel_pagemap()->PageTable*{return fixture::early_null?nullptr:&root;}
}
namespace ker::mod::kasan {
namespace mm=ker::mod::mm;
struct log {
 template<class...T>static void info(const char*,T...){++fixture::infos;}
 template<class...T>static void error(const char*,T...){++fixture::errors;}
};
// ACTUAL_IMAGE_SHADOW_CONSTANTS
// ACTUAL_IMAGE_SHADOW_FORMULA
auto handle_shadow_fault(uint64_t)->bool;
// ACTUAL_IMAGE_BACKING
// ACTUAL_IMAGE_ACTIVE
// ACTUAL_IMAGE_INIT
}
namespace fixture {
namespace kasan=ker::mod::kasan;
uintptr_t base(){return reinterpret_cast<uintptr_t>(kasan::addr_to_shadow(IMAGE_BASE));}
void reset(){
 ++cases;irq=true;early_null=false;image_start=IMAGE_BASE;image_end=IMAGE_BASE+PAGE*8;
 calls=materializations=saves=restores=fatals=errors=infos=fail_call=0;
 present.fill(false);for(auto&page:shadow)page.fill(0);addresses.fill(0);kasan::s_active=false;
}
bool invoke(){try{kasan::init();return true;}catch(const Fatal&){return false;}}
bool covers(size_t count){for(size_t i=0;i<count;++i)if(!present[i])return false;return true;}
void fill(size_t index,int8_t value){present[index]=true;shadow[index].fill(value);}
}
namespace ker::mod::kasan {
auto handle_shadow_fault(uint64_t address)->bool{
 fixture::check(!fixture::irq,"image mapper called with IF disabled");++fixture::calls;
 if(fixture::calls<=fixture::addresses.size())fixture::addresses[fixture::calls-1]=address;
 fixture::check(address>=fixture::base()&&(address-fixture::base())%fixture::PAGE==0,"image mapper exact page alignment");
 if(address<fixture::base())return false;
 size_t index=(address-fixture::base())/fixture::PAGE;
 fixture::check(index<fixture::PAGES,"image mapper finite page bound");
 if(index>=fixture::PAGES||fixture::calls==fixture::fail_call)return false;
 if(!fixture::present[index]){fixture::present[index]=true;fixture::shadow[index].fill(0);++fixture::materializations;}
 return true;
}
}
'''

IMAGE_INIT_CASES = r'''
namespace fixture {
void run(){
 // Exact exclusive span: two full shadow pages, no third-page allocation.
 reset();image_end=IMAGE_BASE+PAGE*16;check(invoke(),"image init accepts permanent span");
 check(calls==2&&materializations==2&&covers(2)&&!present[2],"image init backs exact complete linker span");
 check(addresses[0]==base()&&addresses[1]==base()+PAGE,"image init uses actual shadow formula");
 check(irq&&saves==2&&restores==2&&infos==1&&errors==0&&fatals==0&&!kasan::s_active,"image init preserves disabled runtime and enabled IF");
 // One unaligned byte on each side of a shadow-page boundary needs both pages.
 reset();image_start=IMAGE_BASE+PAGE*8-1;image_end=image_start+2;
 check(invoke()&&calls==2&&covers(2)&&!present[2],"image init covers unaligned exclusive edges");
 // Already mapped pages retain every byte, including partial-granule poison.
 reset();fill(0,kasan::SHADOW_HEAP_FREED);shadow[0][7]=3;auto saved=shadow;
 check(invoke()&&materializations==0&&shadow==saved,"image init preserves all existing poison");
 // Repeated init must not grow backing or reset runtime state.
 reset();image_end=IMAGE_BASE+PAGE*24;kasan::s_active=true;
 check(invoke(),"image init first backing succeeds");irq=false;
 check(invoke()&&calls==6&&materializations==3&&covers(3)&&!present[3],"image init repeat has no shadow growth");
 check(kasan::s_active&&!irq&&saves==6&&restores==3,"image init repeat preserves active state and both IF states");
 // Entire ordinary kernel aperture is rejected before touching shadow.
 reset();image_start=0x1000;image_end=0x2000;
 check(!invoke()&&fatals==1&&errors==1&&infos==0&&calls==0&&saves==0&&irq,"image init invalid low span is fatal before publication");
 reset();image_end=image_start;
 check(!invoke()&&fatals==1&&calls==0&&infos==0&&irq,"image init empty span is fatal");
 reset();image_end=image_start-1;
 check(!invoke()&&fatals==1&&calls==0&&infos==0&&irq,"image init reversed span is fatal");
 reset();image_start=UINTPTR_MAX-7;image_end=4;
 check(!invoke()&&fatals==1&&calls==0&&infos==0&&irq,"image init wrapped span is fatal");
 // A valid span without its early kernel root is a failed boot, not success.
 reset();early_null=true;
 check(!invoke()&&fatals==1&&errors==1&&infos==0&&calls==0&&saves==0&&irq,"image init missing root is fatal");
 // The helper restores original IF before the finite fatal endpoint.
 reset();fail_call=1;
 check(!invoke()&&fatals==1&&errors==1&&infos==0&&calls==1&&materializations==0&&irq&&saves==1&&restores==1,"image init first OOM is fatal with IF restored");
 reset();image_end=IMAGE_BASE+PAGE*24;fail_call=2;
 check(!invoke()&&fatals==1&&infos==0&&calls==2&&materializations==1&&present[0]&&!present[1]&&!present[2]&&irq&&saves==2&&restores==2,"image init partial OOM stops with IF restored");
 reset();irq=false;image_end=IMAGE_BASE+PAGE*16;fail_call=2;
 check(!invoke()&&fatals==1&&infos==0&&calls==2&&materializations==1&&!irq&&saves==2&&restores==0,"image init OOM preserves disabled IF");
}
}
int main(){fixture::run();std::printf("%s kasan_image_init cases=%u assertions=%u failures=%u\n",fixture::failures?"FAIL":"PASS",fixture::cases,fixture::assertions,fixture::failures);return fixture::failures?1:0;}
'''

def generate_image_init(source: str, header: str, control: str) -> tuple[str, dict]:
    init = definition(source, 'void init() {')
    backing = definition(source, 'auto ensure_shadow_backing(')
    active = re.findall(r'^static volatile bool s_active = false;[^\n]*', source, re.M)
    if len(active) != 1:
        raise ValueError('one actual runtime enable flag required')
    actual = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
              [('image_init', init), ('backing', backing), ('active', active[0])]}
    if hashlib.sha256(ORIGINAL_IMAGE_INIT.encode()).hexdigest() != ORIGINAL_IMAGE_INIT_SHA256:
        raise ValueError('genuine whole old-init control changed')
    if control == 'original-init':
        init = ORIGINAL_IMAGE_INIT
    elif control != 'current':
        raise ValueError('unknown image-init control')
    for old, new in [('reinterpret_cast<uintptr_t>(__kernel_image_start)', 'fixture::image_start'),
                     ('reinterpret_cast<uintptr_t>(__kernel_end)', 'fixture::image_end')]:
        expected = 0 if control == 'original-init' else 1
        if init.count(old) != expected:
            raise ValueError('exact direct linker-address endpoint changed')
        init = init.replace(old, new)
    for old, new in [('asm volatile("pushfq; popq %0; cli" : "=r"(irq_flags) : : "memory");', 'irq_flags = fixture::irq_save_disable();'),
                     ('asm volatile("sti" : : : "memory");', 'fixture::irq_restore_enable();')]:
        if backing.count(old) != 1:
            raise ValueError('exact image backing IRQ endpoint changed')
        backing = backing.replace(old, new)
    formula = re.findall(r'^inline auto addr_to_shadow\([^\n]+', header, re.M)
    shadows = re.findall(r'^constexpr uint64_t SHADOW_[^\n]+', header, re.M)
    sentinels = re.findall(r'^constexpr int8_t SHADOW_[^\n]+', header, re.M)
    if len(formula) != 1 or len(shadows) != 3 or len(sentinels) != 6:
        raise ValueError('actual image shadow formula/constants changed')
    text = IMAGE_INIT_PRELUDE
    for name, body in {'ACTUAL_IMAGE_SHADOW_CONSTANTS': '\n'.join(shadows + ['[[maybe_unused]] ' + row for row in sentinels]),
                       'ACTUAL_IMAGE_SHADOW_FORMULA': formula[0], 'ACTUAL_IMAGE_BACKING': backing,
                       'ACTUAL_IMAGE_ACTIVE': active[0], 'ACTUAL_IMAGE_INIT': init}.items():
        marker = '// ' + name
        if text.count(marker) != 1:
            raise ValueError('one finite image-init seam required: ' + name)
        text = text.replace(marker, body)
    actual['ported_bodies'] = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in
                               [('image_init', init), ('backing', backing)]}
    actual['finite_endpoints'] = ['direct linker-symbol integer addresses', 'privileged IRQ save/restore',
                                  'existing shadow mapper/root/allocation/failure', 'early log sink and noreturn fatal halt']
    actual['original_image_init_sha256'] = ORIGINAL_IMAGE_INIT_SHA256
    return text + IMAGE_INIT_CASES, actual

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kasan-source', type=Path, default=SOURCE)
    parser.add_argument('--virt-source', type=Path, default=VIRT_SOURCE)
    parser.add_argument('--cxx', default=os.environ.get('CXX', os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++')))
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    groups = []
    watched, before = [], []
    result = dict(pass_=False, commands=[], scratch_removed=False, physical_accounting_qualified=False,
                  native_IRQ_NMI_TLB_qualified=False)
    try:
        source = args.kasan_source.resolve(strict=True)
        virt_source = args.virt_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name one executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name one executable compiler ELF')
        subreaper()
        watched = [source, HEADER, Path(__file__).resolve(),
                   ROOT / 'tests/host/unit/file_mmap_registry_runtime_test.py', compiler, virt_source, MM_HEADER]
        before = [pin(path) for path in watched]
        scratch = Path(tempfile.mkdtemp(prefix='wos-kasan-shadow-', dir='/tmp'))
        for control in ('current', 'original-zero-fill', 'whole-page-clear'):
            text, actual = generate(source=source.read_text(), header=HEADER.read_text(), control=control)
            result.setdefault('actual_definition_hashes', {})[control] = actual
            translation, binary = scratch / (control + '.cpp'), scratch / control
            translation.write_text(text)
            row = command([str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-Wall', '-Wextra', '-Werror',
                           '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(translation), '-o', str(binary)],
                          scratch / (control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': compile/link failure is not a successful negative control')
            log_path = scratch / (control + '-run.log')
            row = command([str(binary)], log_path, scratch, deadline, groups)
            result['commands'].append(row)
            log = log_path.read_text(errors='replace')
            print(log[-32768:], end='')
            positive = row['exit'] == 0 and re.search(r'^PASS kasan_shadow cases=22 assertions=\d+ failures=0$', log, re.M)
            if (control == 'current') != bool(positive):
                raise ValueError(control + ': complete actual-body oracle did not distinguish expected behavior')
            if control != 'current':
                marker = {'original-zero-fill': 'FAIL absent zero keeps shadow unmapped',
                          'whole-page-clear': 'FAIL clipped shadow neighbors retain poison'}[control]
                if row['exit'] == 0 or not re.search(r'^FAIL kasan_shadow cases=22 assertions=\d+ failures=[1-9]\d*$', log, re.M) or marker not in log.splitlines():
                    raise ValueError(control + ': compiled negative must finish all22cases and fail its intended exact oracle')
        for control in ('current', 'missing-preparation', 'destructive-clear'):
            text, actual = generate_backing(source=source.read_text(), header=HEADER.read_text(),
                                           virt=virt_source.read_text(), mm_header=MM_HEADER.read_text(), control=control)
            result.setdefault('backing_actual_definition_hashes', {})[control] = actual
            translation, binary = scratch / ('backing-' + control + '.cpp'), scratch / ('backing-' + control)
            translation.write_text(text)
            row = command([str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-Wall', '-Wextra', '-Werror',
                           '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(translation), '-o', str(binary)],
                          scratch / ('backing-' + control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / ('backing-' + control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': backing compile/link failure is not a successful control')
            log_path = scratch / ('backing-' + control + '-run.log')
            row = command([str(binary)], log_path, scratch, deadline, groups)
            result['commands'].append(row)
            log = log_path.read_text(errors='replace')
            print(log[-32768:], end='')
            positive = row['exit'] == 0 and re.search(r'^PASS kasan_backing cases=12 assertions=\d+ failures=0$', log, re.M)
            if (control == 'current') != bool(positive):
                raise ValueError(control + ': actual backing/integration oracle did not distinguish expected behavior')
            if control != 'current':
                marker = {'missing-preparation': 'FAIL pool shadow backed before publication',
                          'destructive-clear': 'FAIL range backing preserves present poison'}[control]
                if row['exit'] == 0 or not re.search(r'^FAIL kasan_backing cases=12 assertions=\d+ failures=[1-9]\d*$', log, re.M) or marker not in log.splitlines():
                    raise ValueError(control + ': compiled backing control must finish all12cases and fail its exact intended oracle')
        for control in ('current', 'original-init'):
            text, actual = generate_image_init(source=source.read_text(), header=HEADER.read_text(), control=control)
            result.setdefault('image_init_actual_definition_hashes', {})[control] = actual
            translation, binary = scratch / ('image-init-' + control + '.cpp'), scratch / ('image-init-' + control)
            translation.write_text(text)
            row = command([str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-Wall', '-Wextra', '-Werror',
                           '-fsanitize=address,undefined', '-fno-omit-frame-pointer', str(translation), '-o', str(binary)],
                          scratch / ('image-init-' + control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / ('image-init-' + control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': image-init compile/link failure is not a successful control')
            log_path = scratch / ('image-init-' + control + '-run.log')
            row = command([str(binary)], log_path, scratch, deadline, groups)
            result['commands'].append(row)
            log = log_path.read_text(errors='replace')
            print(log[-32768:], end='')
            positive = row['exit'] == 0 and re.search(r'^PASS kasan_image_init cases=12 assertions=\d+ failures=0$', log, re.M)
            if (control == 'current') != bool(positive):
                raise ValueError(control + ': whole actual-init oracle did not distinguish expected behavior')
            if control != 'current':
                markers = ('FAIL image init backs exact complete linker span',
                           'FAIL image init invalid low span is fatal before publication')
                if row['exit'] == 0 or not re.search(r'^FAIL kasan_image_init cases=12 assertions=\d+ failures=[1-9]\d*$', log, re.M) or not all(marker in log.splitlines() for marker in markers):
                    raise ValueError(control + ': compiled old-init control must finish all12cases and fail the same coverage/fatal oracles')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if before:
            try:
                result['sources_held'] = [pin(path) for path in watched] == before
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole90-second invocation including cleanup exceeded'
        print(json.dumps(result))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
