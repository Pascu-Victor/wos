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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kasan-source', type=Path, default=SOURCE)
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
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name one executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name one executable compiler ELF')
        subreaper()
        watched = [source, HEADER, Path(__file__).resolve(),
                   ROOT / 'tests/host/unit/file_mmap_registry_runtime_test.py', compiler]
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
