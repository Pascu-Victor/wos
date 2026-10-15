#!/usr/bin/env python3
"""Execute actual shootdown init/online guards with controlled CPU/vector seams.

Panic is caught only in the Host harness to inspect unchanged publication state.
This tests boot admission ordering/idempotence, not interrupt delivery or SMP.
"""
import os
from pathlib import Path
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
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
namespace actual_test {
struct PageTable {};
PageTable kernel_object;
PageTable* kernel_pagemap=&kernel_object;
std::atomic<bool> tlb_shootdown_init_attempted{false};
uint8_t tlb_shootdown_vector=0;
std::array<std::atomic<PageTable*>,64> tlb_active_pagemaps{};
std::array<std::atomic<uint8_t>,64> tlb_shootdown_cpu_online{};
uint64_t cores=1,cpu_index=0;
bool cpu_data_available=false;
unsigned allocations=0,registrations=0,warnings=0,checks=0,failures=0;
uint8_t allocation_result=0;
PageTable sentinel;
namespace smt { uint64_t get_core_count(){return cores;}bool has_cpu_data(){return cpu_data_available;} }
namespace cpu { uint64_t current_cpu(){return cpu_index;} }
bool cpu_slot_valid(uint64_t n){return n<64;}
void tlb_shootdown_handler(){}
namespace gates {
uint8_t allocate_vector(){++allocations;return allocation_result;}
void set_interrupt_handler(uint8_t n,void(*)()){if(n==0)throw std::logic_error("zero handler");++registrations;}
}
namespace dbg {
[[noreturn]] void panic_handler(const char*){throw std::runtime_error("actual panic seam");}
template<class... T> void log(const char* text,T...){if(text[0]=='W')++warnings;}
}
void expect(bool ok,const char* label){++checks;if(!ok){++failures;std::fprintf(stderr,"FAIL %s\n",label);}}
'''

TESTS = r'''
void reset(uint64_t n,uint8_t result){
    cores=n;allocation_result=result;cpu_index=0;cpu_data_available=false;
    allocations=registrations=warnings=0;tlb_shootdown_vector=0;tlb_shootdown_init_attempted=false;
    for(size_t i=0;i<64;++i){tlb_active_pagemaps[i]=&sentinel;tlb_shootdown_cpu_online[i]=0;}
}
template<class F>bool panics(F f){try{f();return false;}catch(std::runtime_error const&){return true;}}
void unchanged(){for(size_t i=0;i<64;++i){expect(tlb_active_pagemaps[i].load()==&sentinel,"no active-root store before fatal refusal");expect(tlb_shootdown_cpu_online[i].load()==0,"no online-bit store before fatal refusal");}}
void init_matrix(){
    for(uint64_t count:{0U,1U,2U,8U,64U})for(uint8_t result:{0U,0x51U}){
        reset(count,result);bool should_panic=count>1 && result==0;
        expect(panics([]{init_tlb_shootdown();})==should_panic,"published SMP count rejects allocation0 even before cpu_data exists");
        expect(allocations==1 && tlb_shootdown_init_attempted.load(),"single actual init attempt records allocator call");
        expect(registrations==(result?1U:0U),"only actual nonzero vector installs a handler");
        expect(tlb_shootdown_vector==result,"published vector is actual allocator result");
        expect(warnings==((!result && !should_panic)?1U:0U),"only genuine UP/unknown-early failure keeps warning continuation");
        unchanged();
    }
}
void online_matrix(){
    for(uint64_t count:{0U,1U,2U,8U})for(uint64_t cpu:{0U,1U,7U})for(uint8_t vector:{0U,0x51U}){
        reset(count,vector);cpu_index=cpu;tlb_shootdown_vector=vector;
        bool should_panic=vector==0 && (count>1 || cpu!=0);
        bool caught=panics([]{note_tlb_shootdown_cpu_online();});
        expect(caught==should_panic,"actual AP/SMP entry refuses missing vector");
        if(should_panic)unchanged();
        else{
            expect(tlb_active_pagemaps[cpu].load()==kernel_pagemap,"ready entry publishes actual kernel root");
            expect(tlb_shootdown_cpu_online[cpu].load()==1,"ready entry publishes online only after guard");
            for(size_t i=0;i<64;++i)if(i!=cpu)expect(tlb_shootdown_cpu_online[i].load()==0,"unrelated CPU online state unchanged");
        }
        expect(allocations==0 && registrations==0,"online gate does not fabricate or allocate readiness");
    }
    reset(8,0);cpu_index=64;expect(!panics([]{note_tlb_shootdown_cpu_online();}),"existing invalid-slot refusal retained");unchanged();
}
void idempotence(){
    reset(8,0x51);expect(!panics([]{init_tlb_shootdown();}),"successful SMP init");allocation_result=0;
    expect(!panics([]{init_tlb_shootdown();}),"successful repeated init stays idempotent");
    expect(allocations==1 && registrations==1 && tlb_shootdown_vector==0x51,"repeat never loses working vector");
    reset(1,0);expect(!panics([]{init_tlb_shootdown();}),"actual early/UP allocation failure may continue");
    cores=8;allocation_result=0x51;expect(!panics([]{init_tlb_shootdown();}),"attempted guard does not invent retry");
    expect(allocations==1 && registrations==0 && tlb_shootdown_vector==0,"early failed attempt remains truthfully unavailable");
    expect(panics([]{note_tlb_shootdown_cpu_online();}),"final online boundary catches early failed attempt");unchanged();
    reset(8,0);expect(panics([]{init_tlb_shootdown();}),"SMP failure is fatal first attempt");
    allocation_result=0x51;expect(!panics([]{init_tlb_shootdown();}),"test-only caught panic preserves existing attempted behavior");
    cpu_index=1;expect(panics([]{note_tlb_shootdown_cpu_online();}),"AP cannot bypass prior fatal attempt after catch seam");unchanged();
}
int run(){init_matrix();online_matrix();idempotence();std::printf("checks=%u failures=%u\n",checks,failures);return failures?3:0;}
}
int main(){return actual_test::run();}
'''

def main():
    compiler = os.environ.get("HOST_CXX") or shutil.which("g++", path=os.defpath) or shutil.which("clang++", path=os.defpath)
    assert compiler, "host compiler missing"
    source = SOURCE.read_text()
    before = SOURCE.read_bytes()
    code = PREFIX + extract(source, "void init_tlb_shootdown(") + extract(source, "void note_tlb_shootdown_cpu_online(") + TESTS
    environment = {**os.environ, "ASAN_OPTIONS": "detect_leaks=0:halt_on_error=1", "UBSAN_OPTIONS": "halt_on_error=1"}
    with tempfile.TemporaryDirectory(prefix="wos-tlb-readiness-") as scratch:
        directory = Path(scratch)
        cpp, binary = directory / "check.cpp", directory / "check"
        cpp.write_text(code)
        commands = ([compiler, "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Wno-unused-parameter",
                     "-Wno-unused-function", "-fno-pie", "-no-pie", "-fsanitize=address,undefined",
                     "-fno-omit-frame-pointer", str(cpp), "-o", str(binary)], [str(binary)])
        for argv, timeout in zip(commands, (60, 15)):
            completed = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=environment)
            print(completed.stdout, end="")
            print(completed.stderr, end="", file=sys.stderr)
            assert completed.returncode == 0, (argv, completed.returncode)
    assert not directory.exists(), "owned compiler scratch not removed"
    assert before == SOURCE.read_bytes(), "production source changed during actual-body check"


if __name__ == "__main__":
    main()
