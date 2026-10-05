#!/usr/bin/env python3
"""Execute the actual serial driver against bounded UART and IRQ host leaves.

Privileged leaves are separately compiled/disassembled. Native emulation checks
FIFO capacity, bytes, backpressure, reentrancy and synchronous drain; actual
kernel boot/interrupt behavior and latency still need WOS runtime evidence.
"""
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SERIAL = ROOT / "modules/kern/src/mod/io/serial/serial.cpp"
HEADER = ROOT / "modules/kern/src/mod/io/serial/serial.hpp"
JOURNAL = ROOT / "modules/kern/src/platform/dbg/journal.cpp"


def extract(text, marker):
    start = text.index(marker)
    pos = text.index("{", start) + 1
    depth = 1
    while depth:
        depth += (text[pos] == "{") - (text[pos] == "}")
        pos += 1
    return text[start:pos]


def without_includes(text):
    return "\n".join(line for line in text.splitlines() if not line.startswith(("#include", "#pragma once"))) + "\n"


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <span>
#include <string>
#include <vector>
namespace model {
bool irq=true,query_task=true,dlab=false,hardware_fifo=false,shift_busy=false;
unsigned depth=0,disables=0,enables=0,status_reads=0,single_writes=0,bursts=0,busy_reads=0;
uint8_t fifo_capability=0xC1;
std::string pending,wire;
std::function<void()> interrupt;
void send(char c){assert(pending.size()<(hardware_fifo?16U:1U));pending.push_back(c);}
void drain(){wire+=pending;pending.clear();shift_busy=false;}
uint64_t disable(){bool old=irq;irq=false;return old?0x200:0;}
void restore(uint64_t flags){
 if(flags&0x200){irq=true;if(interrupt){auto callback=std::move(interrupt);interrupt=nullptr;callback();}}
}
}
namespace ker::mod::sched::task {struct alignas(8) Task{};}
namespace ker::mod::sched {
task::Task subject;
bool can_query_current_task(){return model::query_task;}
task::Task* get_current_task(){return &subject;}
task::Task* preempt_disable_token_at(uint64_t){++model::depth;++model::disables;return &subject;}
void preempt_enable_token_at(task::Task* p,uint64_t){assert(p==&subject && model::depth);--model::depth;++model::enables;}
bool interrupts_enabled(){return model::irq;}
}
namespace ker::mod::cpu {constexpr uint64_t GATE_IF_MASK=0x200;uint64_t current_cpu(){return 2;}}
namespace ker::mod::mm::virt {void service_pending_tlb_shootdowns(){}}
void outb(uint16_t port,uint8_t value){
 assert(port>=0x3F8 && port<=0x3FC);
 if(port==0x3FB){model::dlab=(value&0x80)!=0;return;}
 if(port==0x3FA){model::hardware_fifo=(value&1) && (model::fifo_capability&0xC0)==0xC0;return;}
 if(port==0x3F8 && !model::dlab){
  if(!model::irq && model::hardware_fifo){if(model::pending.empty())++model::bursts;}
  else ++model::single_writes;
  model::send(static_cast<char>(value));
 }
}
uint8_t inb(uint16_t port){
 if(port==0x3FA)return model::hardware_fifo?model::fifo_capability:(model::fifo_capability&0xBF);
 assert(port==0x3FD);++model::status_reads;
 if(model::busy_reads){--model::busy_reads;return 0;}
 if(!model::pending.empty()){model::wire+=model::pending;model::pending.clear();model::shift_busy=true;return 0x20;}
 model::shift_busy=false;return 0x60;
}
'''

MAIN = r'''
using namespace ker::mod::io::serial;
void reset(uint8_t capability=0xC1,bool initialize=true){
 model::irq=true;model::query_task=true;model::dlab=false;model::hardware_fifo=false;model::shift_busy=false;
 model::depth=model::disables=model::enables=model::status_reads=model::single_writes=model::bursts=model::busy_reads=0;
 model::pending.clear();model::wire.clear();model::interrupt=nullptr;model::fifo_capability=capability;
 is_init=false;tx_fifo_available.store(false);in_panic_mode.store(false);panic_owner_cpu.store(NO_PANIC_OWNER);
 lock_owner.store(NO_OWNER);lock_depth.store(0);lock_preempt_owner=nullptr;cpu_id_available.store(false);
 if(initialize)init();
 model::status_reads=0;
}
void check_unlock(){assert(lock_owner.load()==NO_OWNER && lock_depth.load()==0 && model::depth==0);}
int main(){
 static_assert(ker::mod::cpu::GATE_IF_MASK==0x200);
 static_assert(ker::abi::sys_log::JOURNAL_RECORD_MAGIC==0x4a4f5753 && ker::abi::sys_log::JOURNAL_RECORD_VERSION==1);
 for(size_t len: {size_t{0},size_t{1},size_t{15},size_t{16},size_t{17},size_t{31},size_t{32},size_t{33},size_t{511},size_t{2048}}){
  reset();std::string payload(len,'x');
  for(size_t i=0;i<len;++i)payload[i]=static_cast<char>((i*53)%256);
  write(len?payload.data():nullptr,len);
  assert(model::wire==payload && model::pending.empty());
  assert(model::bursts==(len+15)/16 && model::single_writes==0);
  assert(model::irq && model::disables==1 && model::enables==1);check_unlock();
  {ScopedLock lock;flush_unlocked();}
  assert(!model::shift_busy && model::pending.empty());
  assert(model::status_reads==(len?(len+15)/16+2:1));check_unlock();
 }
 for(uint8_t capability: {uint8_t{0},uint8_t{0x80}}){
  reset(capability);assert(!tx_fifo_available.load());
  write("legacy");flush_unlocked();assert(model::bursts==0 && model::single_writes==6);
  model::drain();assert(model::wire=="legacy");check_unlock();
 }
 reset(0,false);write("early");assert(model::bursts==0);model::drain();assert(model::wire=="early");check_unlock();
 reset();in_panic_mode.store(true);write("panic");flush_unlocked();
 assert(model::bursts==0 && model::single_writes==5);model::drain();assert(model::wire=="panic");
 reset();model::query_task=false;write("cpu-owner");assert(model::depth==0 && model::disables==0);check_unlock();
 reset();model::irq=false;model::busy_reads=4;write("masked");flush_unlocked();
 assert(!model::irq && model::wire=="masked" && model::bursts==1);check_unlock();
 reset();model::busy_reads=4;write("backpressure");flush_unlocked();
 assert(model::irq && model::wire=="backpressure" && model::bursts==1);check_unlock();
 reset();std::string outer(37,'o');model::interrupt=[](){assert(model::irq && model::depth==1);write("IRQ");};
 write(outer.data(),outer.size());flush_unlocked();
 assert(model::wire==std::string(16,'o')+"IRQ"+std::string(21,'o') && model::irq);
 assert(model::disables==1 && model::enables==1);check_unlock();
 reset();write(uint64_t{18446744073709551615ULL});write('|');write_hex(uint64_t{0xFEDCBA9876543210ULL});flush_unlocked();
 assert(model::wire=="18446744073709551615|fedcba9876543210");check_unlock();
 reset();write("terminated\0ignored");flush_unlocked();assert(model::wire=="terminated");check_unlock();
 for(uint64_t micros: {uint64_t{0},uint64_t{9000},uint64_t{99000},uint64_t{999000},UINT64_MAX}){
  reset();ker::mod::dbg::journal::JournalRecord record{};record.monotonic_us=micros;record.level=2;
  std::memcpy(record.module,"wki",4);std::string message(511,'z');message[20]='\0';
  std::memcpy(record.message,message.data(),message.size());record.message_len=511;
  ker::mod::dbg::journal::serial_write_record(record);
  char prefix[100];std::snprintf(prefix,sizeof(prefix),"[%llu.%03llu] INFO wki: ",
   static_cast<unsigned long long>(micros/1000000),static_cast<unsigned long long>((micros/1000)%1000));
  assert(model::wire==std::string(prefix)+message+"\n" && model::pending.empty() && !model::shift_busy);
  assert(model::bursts>0 && model::irq);check_unlock();
 }
 std::cout<<"PASS actual-source FIFO bytes, bounds, IRQ/reentrancy, backpressure, fallback and journal drain\n";
}
'''


def main():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    compiler = shutil.which("clang++", path=os.defpath) or shutil.which("c++", path=os.defpath)
    assert compiler
    source = SERIAL.read_text()
    leaves = {
        "auto disable_tx_interrupts()": "auto disable_tx_interrupts() -> uint64_t { return model::disable(); }",
        "void restore_tx_interrupts(uint64_t flags)": "void restore_tx_interrupts(uint64_t flags) { model::restore(flags); }",
    }
    asm_source = "#include <cstdint>\n#include <cstddef>\n#include <span>\nnamespace cpu {constexpr uint64_t GATE_IF_MASK=0x200;}\nconstexpr uint16_t DATA_PORT=0x3F8;\n"
    asm_source += extract((ROOT / "modules/kern/src/mod/io/port/port.cpp").read_text(), "void outb(uint16_t port, uint8_t val)") + "\n"
    asm_source += extract(source, "void write_fifo_bytes(const char* bytes, size_t count)") + "\n"
    for marker, replacement in leaves.items():
        definition = extract(source, marker)
        asm_source += definition + "\n"
        source = source.replace(definition, replacement)
    record = extract(JOURNAL.read_text(), "void serial_write_record(const JournalRecord& rec)")
    assert record.count("flush_unlocked();") == 1 and record.index("return;") < record.index("flush_unlocked();")
    abi = without_includes((ROOT / "modules/kern/src/abi/callnums/sys_log.h").read_text())
    journal_prelude = "namespace ker::mod::dbg::journal {using JournalRecord=ker::abi::sys_log::JournalRecord; using LogLevel=ker::abi::sys_log::sys_log_level; const char* level_name(LogLevel){return \"INFO\";}\n"
    harness = PRELUDE + without_includes(HEADER.read_text()) + without_includes(source) + abi + journal_prelude + record + "\n}\n" + MAIN
    with tempfile.TemporaryDirectory(prefix="wos-serial-fifo-") as temp:
        folder = Path(temp)
        privileged = folder / "privileged.cpp"
        privileged.write_text(asm_source)
        obj = folder / "privileged.o"
        subprocess.run([compiler, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", "-ffreestanding", "-mno-red-zone", "-c", str(privileged), "-o", str(obj)], check=True)
        disassembly = subprocess.check_output(["objdump", "-d", "-Mintel", str(obj)], text=True)
        assert "cli" in disassembly and "sti" in disassembly and re.search(r"\bout\s+dx,al", disassembly) and "0x3f8" in disassembly
        assert "rep outs" not in disassembly
        cpp = folder / "fixture.cpp"
        binary = folder / "fixture"
        flags = [compiler, "-std=c++20", "-O1", "-g", "-Wall", "-Wextra", "-Werror", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-pthread"]
        cpp.write_text(harness)
        subprocess.run([*flags, str(cpp), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=30)
        mutants = {
            "fifo-overrun": ("size_t const COUNT = std::min(remaining.size(), TX_FIFO_SIZE);", "size_t const COUNT = remaining.size();"),
            "missing-irq-mask": ("uint64_t const FLAGS = disable_tx_interrupts();", "uint64_t const FLAGS = model::irq ? uint64_t{0x200} : uint64_t{0};"),
        }
        for name, (before, after) in mutants.items():
            assert harness.count(before) == 1
            cpp.write_text(harness.replace(before, after))
            subprocess.run([*flags, "-Wno-unused-function", "-Wno-unused-const-variable", str(cpp), "-o", str(binary)], check=True)
            result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=30)
            assert result.returncode != 0, name
            print("PASS rejected", name, "mutant", flush=True)


if __name__ == "__main__":
    main()
