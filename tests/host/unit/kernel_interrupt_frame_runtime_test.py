#!/usr/bin/env python3
"""Execute actual NASM native64 interrupt entry and kernel-return macros with real hosted IRETQ.

All GP/normalized/physical fields, overlap directions, independent Task::Context gap,
sixteen saved-RSP residues and KASAN/commit ordering have independent byte oracles.
Finite C++ endpoints retarget only CS/SS after verifying the original complete frame,
allowing actual same-CPL3 IRETQ without pretending WOS kernel selectors run on Linux.
Generic no-error/error and IRQ32 use the whole actual gates object, synthetic hardware
five-word input, and finite handlers. Physical IRQ delivery, CPL0 privilege, SMP,
GS/FPU transitions, production KASAN and scheduler publication are not qualified.
Old complete macro prefix and gates object are compile-success negative controls.
"""
from __future__ import annotations
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(p for p in Path(__file__).resolve().parents if (p/'modules/kern/src').is_dir() and (p/'tests/host').is_dir())
KERN = ROOT/'modules/kern/src'
CONTEXT = KERN/'platform/sys/context_switch.asm'
GATES = KERN/'platform/interrupt/gates.asm'
HELPERS = KERN/'platform/asm/helpers.asm'
CPU_HEADER = KERN/'platform/asm/cpu.hpp'
GATES_HEADER = KERN/'platform/interrupt/gates.hpp'
TASK_HEADER = KERN/'platform/sched/task.hpp'
FRAME_CLASS = KERN/'platform/sched/frame_class.hpp'
SCRATCH_BUDGET = 64*1024*1024
LOG_BUDGET = 1024*1024
FILE_BUDGET = 8*1024*1024

CPP = r'''
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include "actual-abi.hpp"
extern "C" {
void fixture_return_ptrs(void*,void*); void fixture_return_stack(void*); void fixture_enter(); void fixture_landed();
void isr0(); void isr_except14(); void isr32();
alignas(16) uint64_t fixture_input_gp[15],fixture_landed_gp[15];
uint64_t fixture_landed_rsp{},fixture_landed_ss{},fixture_landed_cs{},fixture_landed_flags{};
uint64_t fixture_hardware{},fixture_target{};
unsigned char wos_smap_active=0;
}
namespace fixture {
alignas(4096) std::array<unsigned char,131072> arena;
std::array<uint64_t,7> frame;
uint64_t block{},resume{},host_cs{},host_ss{};
unsigned cases{},assertions{},returns{},entries{},stage{},kasan_calls{},commits{};
unsigned expected_vector{}; uint64_t expected_error{};
[[noreturn]] void fail(char const* text){std::fprintf(stderr,"FAIL_NATIVE_FRAME: %s\n",text);std::fflush(stderr);std::exit(1);}
void check(bool value,char const* text){++assertions;if(!value)fail(text);}
void put(void* p,void const* data,size_t size){std::memcpy(p,data,size);}
void reset(unsigned seed){
 std::memset(arena.data(),0xa5,arena.size());
 for(unsigned i=0;i<15;++i)fixture_input_gp[i]=0x1234000000000000ULL+uint64_t(seed)*256+i*17+3;
 std::memset(fixture_landed_gp,0,sizeof(fixture_landed_gp));
 fixture_landed_rsp=fixture_landed_ss=fixture_landed_cs=fixture_landed_flags=0;
 stage=kasan_calls=commits=0;
}
bool disjoint(uint64_t a,size_t n,uint64_t b,size_t m){return a+n<=b||b+m<=a;}
void check_block(){
 check(std::memcmp(reinterpret_cast<void*>(block),fixture_input_gp,120)==0 &&
       std::memcmp(reinterpret_cast<void*>(block+120),frame.data(),56)==0 &&
       std::memcmp(reinterpret_cast<void*>(block+176),frame.data()+2,40)==0,
       "normalized GP/frame and five-word physical tail");
}
void call_boundary(uint64_t actual){
 check((actual&15)==8,"actual SysV helper call alignment");
 auto low=reinterpret_cast<uint64_t>(arena.data());
 check(actual>=low+4096&&actual+8<=block&&actual<low+arena.size(),"late commit/helper on incoming stack");
}
void retarget_physical(){
 // The production macro emits and the oracle checks all five original fields.
 // Only these selectors are retargeted at the finite C++ endpoint to execute
 // the real IRETQ at hosted CPL3; WOS selector privilege policy is not modeled.
 put(reinterpret_cast<void*>(block+184),&host_cs,8);
 put(reinterpret_cast<void*>(block+208),&host_ss,8);
}
void landed(){
 check(std::memcmp(fixture_landed_gp,fixture_input_gp,120)==0,"all15 actual restored general registers");
 check(fixture_landed_rsp==resume&&fixture_landed_ss==host_ss&&fixture_landed_cs==host_cs,"actual IRETQ RSP/SS/CS restoration");
 check(fixture_landed_flags==frame[4],"actual IRETQ RFLAGS restoration");
 check(std::memcmp(reinterpret_cast<void*>(block),fixture_input_gp,120)==0&&
       std::memcmp(reinterpret_cast<void*>(block+120),frame.data(),56)==0,"normalized176B shadow survives helpers/IRET");
}
void one_return(bool stack,int delta,unsigned residue,unsigned selector,unsigned geometry){
 reset(cases+1);resume=reinterpret_cast<uint64_t>(arena.data()+65536)+residue;block=resume-216;
 uint64_t gp=block+delta,fp=gp+120;
 if(geometry==1){
  gp=block+1024;
  fp=gp+offsetof(ker::mod::sched::task::Context,frame)-offsetof(ker::mod::sched::task::Context,regs);
 } // Compile and use the actual Task::Context gap, independently asserted136B.
 if(geometry==2){gp=block+1024;fp=block+4096;} // Independently allocated-style references.
 if(geometry==3)fp=block;
 if(geometry==4)fp=block+120;
 if(geometry==5)fp=block+176;
 if(geometry==6){gp=(resume&~uint64_t{15})-176;fp=gp+120;} // Native IRQ overlap for every residue.
 if(!disjoint(gp,120,fp,56))return;
 check(!stack||fp==gp+120,"stack wrapper receives actual contiguous carrier only");
 frame={0x79,0x9876,reinterpret_cast<uint64_t>(fixture_landed),selector==0?host_cs:uint64_t{8},
        uint64_t{0x202}|(cases&1),resume,selector==0?host_ss:(selector==1?uint64_t{0}:uint64_t{0x10})};
 put(reinterpret_cast<void*>(gp),fixture_input_gp,120);put(reinterpret_cast<void*>(fp),frame.data(),56);
 if(stack)fixture_return_stack(reinterpret_cast<void*>(gp));
 else fixture_return_ptrs(reinterpret_cast<void*>(gp),reinterpret_cast<void*>(fp));
 check(kasan_calls==FIXTURE_KASAN&&commits==1&&stage==2,"KASAN then single actual late commit");
 landed();++cases;++returns;
}
void return_cases(){
 for(unsigned selector=0;selector<3;++selector)for(unsigned r=0;r<16;++r){
  one_return(false,0,r,selector,1);one_return(false,0,r,selector,2);
  one_return(true,0,r,selector,6);
  for(int delta:std::array<int,6>{-96,-32,0,32,96,512}){
   one_return(true,delta,r,selector,0);
   one_return(false,delta,r,selector,2);
   for(unsigned g=3;g<=5;++g)one_return(false,delta,r,selector,g);
  }
 }
}
void one_entry(unsigned which,unsigned residue,unsigned saved_ss){
 reset(cases+1);auto top=reinterpret_cast<uint64_t>(arena.data()+65536);
 resume=top+residue;fixture_hardware=top-40;
 frame={which==0?uint64_t{0}:which==1?uint64_t{14}:uint64_t{32},
        which==0?UINT64_MAX:which==1?uint64_t{0x4321}:uint64_t{0},
        reinterpret_cast<uint64_t>(fixture_landed),8,uint64_t{0x202}|(cases&1),resume,saved_ss};
 put(reinterpret_cast<void*>(fixture_hardware),frame.data()+2,40);
 expected_vector=which;expected_error=frame[1];
 fixture_target=reinterpret_cast<uint64_t>(which==0?isr0:which==1?isr_except14:isr32);
 fixture_enter();
 check(stage==1&&kasan_calls==0&&commits==0,"entry uses finite ISR hook, not full scheduler return");
 check(std::memcmp(fixture_landed_gp,fixture_input_gp,120)==0,"actual entry restores all15 GP registers");
 check(fixture_landed_rsp==resume&&fixture_landed_cs==host_cs&&fixture_landed_ss==host_ss&&fixture_landed_flags==frame[4],
       "actual generic/IRQ32 five-word IRET restoration");
 ++cases;++entries;
}
void entry_cases(){for(unsigned w=0;w<3;++w)for(unsigned r=0;r<16;++r)for(unsigned ss:std::array<unsigned,2>{0,16})one_entry(w,r,ss);}
void entry_hook(void* data,uint64_t actual,bool timer){
 check((reinterpret_cast<uint64_t>(data)&15)==0,"original interrupt GP carrier alignment");
 check((actual&15)==8,"actual generic or finite timer-hook call alignment");
 auto p=reinterpret_cast<unsigned char*>(data);
 check(std::memcmp(p,fixture_input_gp,120)==0&&std::memcmp(p+120,frame.data(),56)==0,
       "entry preserves actual five-word hardware RSP/SS");
 check(timer==(expected_vector==2)&&stage==0,"distinct generic versus IRQ32 endpoint");
 check(expected_error==frame[1],"error-code and vector carrier");
 // Fake kernel CS selected the actual kernel entry before the finite endpoint.
 // Retargeting occurs after that CPL branch; no privileged user-only hook runs.
 put(p+144,&host_cs,8);put(p+168,&host_ss,8);stage=1;
}
}
extern "C" void fixture_kasan(uint64_t rsp,uint64_t actual){
 fixture::check_block();fixture::call_boundary(actual);
 fixture::check(FIXTURE_KASAN==1&&rsp==fixture::resume&&fixture::stage==0,"exact saved resume RSP unpoison before commit");
 ++fixture::kasan_calls;fixture::stage=1;
}
extern "C" void fixture_commit(uint64_t actual){
 fixture::check_block();fixture::call_boundary(actual);
 fixture::check(fixture::stage==(FIXTURE_KASAN?1U:0U),"commit follows enabled KASAN before any publication");
 ++fixture::commits;fixture::stage=2;fixture::retarget_physical();
}
extern "C" void fixture_generic(void* data,uint64_t actual){fixture::entry_hook(data,actual,false);}
extern "C" void fixture_timer(void* data,uint64_t actual){fixture::entry_hook(data,actual,true);}
extern "C" [[noreturn]] void fixture_unexpected(){fixture::fail("user-only FPU endpoint reached from fake-kernel entry");}
int main(int argc,char** argv){
 unsigned short cs,ss;asm volatile("mov %%cs,%0; mov %%ss,%1":"=r"(cs),"=r"(ss));
 fixture::host_cs=cs;fixture::host_ss=ss;fixture::check((cs&3)==3&&(ss&3)==3,"real hosted same-CPL3 selectors");
 bool ret=argc==1||std::string_view(argv[1])=="--return-only";
 bool ent=argc==1||std::string_view(argv[1])=="--entry-only";
 fixture::check(argc<=2&&(ret||ent),"finite known fixture mode");
 if(ret)fixture::return_cases();if(ent)fixture::entry_cases();
 fixture::check((!ret||fixture::returns==1248)&&(!ent||fixture::entries==96),"complete bounded geometry census");
 std::printf("PASS_NATIVE_FRAME cases=%u returns=%u entries=%u assertions=%u kasan=%u actual_iretq=1 physical_irq=0 smp=0\n",
             fixture::cases,fixture::returns,fixture::entries,fixture::assertions,FIXTURE_KASAN);
}
'''

ASM = r'''
default rel
extern fixture_input_gp,fixture_landed_gp,fixture_landed_rsp,fixture_landed_ss,fixture_landed_cs,fixture_landed_flags
extern fixture_hardware,fixture_target
extern fixture_commit,fixture_kasan,fixture_generic,fixture_timer,fixture_unexpected
section .bss
align 16
host_saved: resq 7
section .text
%macro save_host 0
 mov [rel host_saved],rsp
 mov [rel host_saved+8],rbx
 mov [rel host_saved+16],rbp
 mov [rel host_saved+24],r12
 mov [rel host_saved+32],r13
 mov [rel host_saved+40],r14
 mov [rel host_saved+48],r15
%endmacro
global fixture_return_ptrs
fixture_return_ptrs:
 save_host
 build_kernel_return_from_ptrs
global fixture_return_stack
fixture_return_stack:
 save_host
 mov rsp,rdi
 build_kernel_return_from_stack
global fixture_landed
fixture_landed:
 mov [rel fixture_landed_gp],r15
 mov [rel fixture_landed_gp+8],r14
 mov [rel fixture_landed_gp+16],r13
 mov [rel fixture_landed_gp+24],r12
 mov [rel fixture_landed_gp+32],r11
 mov [rel fixture_landed_gp+40],r10
 mov [rel fixture_landed_gp+48],r9
 mov [rel fixture_landed_gp+56],r8
 mov [rel fixture_landed_gp+64],rbp
 mov [rel fixture_landed_gp+72],rdi
 mov [rel fixture_landed_gp+80],rsi
 mov [rel fixture_landed_gp+88],rdx
 mov [rel fixture_landed_gp+96],rcx
 mov [rel fixture_landed_gp+104],rbx
 mov [rel fixture_landed_gp+112],rax
 mov [rel fixture_landed_rsp],rsp
 mov ax,ss
 movzx eax,ax
 mov [rel fixture_landed_ss],rax
 mov ax,cs
 movzx eax,ax
 mov [rel fixture_landed_cs],rax
 pushfq
 pop rax
 mov [rel fixture_landed_flags],rax
 cld
 mov rbx,[rel host_saved+8]
 mov rbp,[rel host_saved+16]
 mov r12,[rel host_saved+24]
 mov r13,[rel host_saved+32]
 mov r14,[rel host_saved+40]
 mov r15,[rel host_saved+48]
 mov rsp,[rel host_saved]
 ret
global fixture_enter
fixture_enter:
 save_host
 mov rsp,[rel fixture_hardware]
 lea rax,[rel isr_except14]
 cmp qword [rel fixture_target],rax
 jne .no_error
 push qword 0x4321
.no_error:
 mov r15,[rel fixture_input_gp]
 mov r14,[rel fixture_input_gp+8]
 mov r13,[rel fixture_input_gp+16]
 mov r12,[rel fixture_input_gp+24]
 mov r11,[rel fixture_input_gp+32]
 mov r10,[rel fixture_input_gp+40]
 mov r9,[rel fixture_input_gp+48]
 mov r8,[rel fixture_input_gp+56]
 mov rbp,[rel fixture_input_gp+64]
 mov rdi,[rel fixture_input_gp+72]
 mov rsi,[rel fixture_input_gp+80]
 mov rdx,[rel fixture_input_gp+88]
 mov rcx,[rel fixture_input_gp+96]
 mov rbx,[rel fixture_input_gp+104]
 mov rax,[rel fixture_input_gp+112]
 jmp [rel fixture_target]
extern isr0,isr_except14,isr32
global wos_commit_handoff_task
wos_commit_handoff_task:
 mov rdi,rsp
 jmp fixture_commit
%ifdef WOS_KASAN
global wos_kasan_prepare_kernel_resume
wos_kasan_prepare_kernel_resume:
 mov rsi,rsp
 jmp fixture_kasan
%endif
global iterrupt_handler
iterrupt_handler:
 mov rsi,rsp
 jmp fixture_generic
global task_switch_handler
task_switch_handler:
 ; Only the timer C++ endpoint is finite. This is NOT the full context object.
 mov r12,rsp
 mov rdi,rsp
 and rsp,-16
 lea rsi,[rsp-8]
 call fixture_timer
 mov rsp,r12
 popq
 add rsp,16
 iretq
global wos_user_interrupt_save_fpu,wos_user_interrupt_restore_fpu
wos_user_interrupt_save_fpu:
wos_user_interrupt_restore_fpu:
 jmp fixture_unexpected
section .note.GNU-stack noalloc noexec nowrite progbits
'''

OLD_RETURN_PREFIX = r'''bits 64

%include "platform/asm/helpers.asm"

%assign GPREGS_SIZE 120
%assign INTERRUPT_FRAME_SIZE 56
%assign USER_IRET_FRAME_SIZE 40
%assign IRETQ_KERNEL_FRAME_SIZE 24
%assign IRETQ_KERNEL_FRAME_OFFSET GPREGS_SIZE + INTERRUPT_FRAME_SIZE
%assign USER_RETURN_BLOCK_SIZE GPREGS_SIZE + USER_IRET_FRAME_SIZE
%assign KERNEL_RETURN_BLOCK_SIZE GPREGS_SIZE + INTERRUPT_FRAME_SIZE + IRETQ_KERNEL_FRAME_SIZE

extern wos_commit_handoff_task
extern wos_user_handoff_stack_top
extern wos_restore_return_task_fpu
extern wos_record_exec_user_return
extern wos_discard_abandoned_return_fpu_state
extern deferred_task_switch
%ifdef WOS_KASAN
extern wos_kasan_prepare_kernel_resume
%endif

; Same-CPL iretq consumes only RIP/CS/RFLAGS, but the scheduler and panic
; diagnostics treat the memory below the saved RSP as GPRegs + InterruptFrame.
; Keep that normalized shadow intact, then skip it immediately before iretq.
%macro build_kernel_return_from_stack 0
    mov r10, [rsp + GPREGS_SIZE + 40]  ; saved kernel RSP
    mov r12, [rsp + GPREGS_SIZE + 16]  ; RIP
    mov r13, [rsp + GPREGS_SIZE + 24]  ; CS
    mov r14, [rsp + GPREGS_SIZE + 32]  ; RFLAGS
    sub r10, KERNEL_RETURN_BLOCK_SIZE

    %assign i 14
    %rep 15
        mov r11, [rsp + i * 8]
        mov [r10 + i * 8], r11
        %assign i i - 1
    %endrep

    %assign j 6
    %rep 7
        mov r11, [rsp + GPREGS_SIZE + j * 8]
        mov [r10 + GPREGS_SIZE + j * 8], r11
        %assign j j - 1
    %endrep

    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET], r12
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 8], r13
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 16], r14

    mov rsp, r10
    sub rsp, 8
%ifdef WOS_KASAN
    lea rdi, [rsp + 8 + KERNEL_RETURN_BLOCK_SIZE]
    call wos_kasan_prepare_kernel_resume
%endif
    call wos_commit_handoff_task
    add rsp, 8
    popq
    add rsp, INTERRUPT_FRAME_SIZE
    iretq
%endmacro

%macro build_kernel_return_from_ptrs 0
    mov r10, [rsi + 40]  ; saved kernel RSP
    mov r12, [rsi + 16]  ; RIP
    mov r13, [rsi + 24]  ; CS
    mov r14, [rsi + 32]  ; RFLAGS
    sub r10, KERNEL_RETURN_BLOCK_SIZE

    %assign i 14
    %rep 15
        mov r11, [rdi + i * 8]
        mov [r10 + i * 8], r11
        %assign i i - 1
    %endrep

    %assign j 6
    %rep 7
        mov r11, [rsi + j * 8]
        mov [r10 + GPREGS_SIZE + j * 8], r11
        %assign j j - 1
    %endrep

    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET], r12
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 8], r13
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 16], r14

    mov rsp, r10
    sub rsp, 8
%ifdef WOS_KASAN
    lea rdi, [rsp + 8 + KERNEL_RETURN_BLOCK_SIZE]
    call wos_kasan_prepare_kernel_resume
%endif
    call wos_commit_handoff_task
    add rsp, 8
    popq
    add rsp, INTERRUPT_FRAME_SIZE
    iretq
%endmacro

'''

OLD_GATES = r'''bits 64

extern iterrupt_handler
extern wos_user_interrupt_save_fpu
extern wos_user_interrupt_restore_fpu
extern wos_smap_active

%include "platform/asm/helpers.asm"

%assign GPREGS_SIZE 120
%assign INTERRUPT_FRAME_SIZE 56

%macro isr_swapgs 1
    test qword [rsp + 24], 0x3 ; swap only when the interrupted frame came from user CPL
    jz .%1
    swapgs
    .%1:
%endmacro

__idt_isr_handler:
    cmp byte [rel wos_smap_active], 0
    je .access_closed
    clac
.access_closed:
    cld
    clear_live_tf

    ; Same-CPL kernel interrupts/exceptions only provide RIP, CS, and RFLAGS
    ; after our int/error-code shim. Build a by-value copy with synthetic
    ; RSP/SS for C++ handlers, then return through the original hardware frame.
    test qword [rsp + 24], 0x3
    jnz .outer_entry

    pushq
    mov r10, rsp
    sub rsp, GPREGS_SIZE + INTERRUPT_FRAME_SIZE

    %assign j 0
    %rep 15
        mov r11, [r10 + j * 8]
        mov [rsp + j * 8], r11
        %assign j j + 1
    %endrep

    mov r11, [r10 + GPREGS_SIZE + 0x00] ; int_num
    mov [rsp + GPREGS_SIZE + 0x00], r11
    mov r11, [r10 + GPREGS_SIZE + 0x08] ; err_code
    mov [rsp + GPREGS_SIZE + 0x08], r11
    mov r11, [r10 + GPREGS_SIZE + 0x10] ; RIP
    mov [rsp + GPREGS_SIZE + 0x10], r11
    mov r11, [r10 + GPREGS_SIZE + 0x18] ; CS
    mov [rsp + GPREGS_SIZE + 0x18], r11
    mov r11, [r10 + GPREGS_SIZE + 0x20] ; RFLAGS
    mov [rsp + GPREGS_SIZE + 0x20], r11
    lea r11, [r10 + GPREGS_SIZE + 0x28] ; interrupted RSP
    mov [rsp + GPREGS_SIZE + 0x28], r11
    mov qword [rsp + GPREGS_SIZE + 0x30], 0x10

    mov rdi, rsp
    call iterrupt_handler

    mov r10, [rsp + GPREGS_SIZE + 0x28]
    sub r10, 24
    mov r11, [rsp + GPREGS_SIZE + 0x10]
    mov [r10 + 0x00], r11
    mov r11, [rsp + GPREGS_SIZE + 0x18]
    mov [r10 + 0x08], r11
    mov r11, [rsp + GPREGS_SIZE + 0x20]
    mov [r10 + 0x10], r11

    popq
    mov rsp, [rsp + 40]
    sub rsp, 24
    iretq

.outer_entry:
    isr_swapgs entry
    pushq
    call wos_user_interrupt_save_fpu
    mov r12, rax
    mov rdi, rsp
    call iterrupt_handler
    mov rdi, r12
    call wos_user_interrupt_restore_fpu
    popq
    isr_swapgs exit
    add rsp, 16
    ; Don't manually enable interrupts - iretq will restore them from saved RFLAGS
    ; Having sti here can cause race conditions if another interrupt arrives between sti and iretq
    iretq

global load_idt
load_idt:
    lidt [rdi]

    push 0x08
    lea rax, [rel .reload]
    push rax
    retfq
    .reload:
        mov ax, 0x10
        mov ds, ax
        mov es, ax
        mov fs, ax
        ; NOTE: Do NOT load GS here - we use GS.base for per-CPU data
        ; and loading a selector into GS zeroes GS.base on x86-64
        mov ss, ax
        sti
        ret

%macro isr 1
global isr%1
isr%1:
    push -1  ; No-error code
    push %1 ; INT number
    jmp __idt_isr_handler
%endmacro

%macro isr_except 1
global isr_except%1
isr_except%1:
    ; code pushed by the CPU
    push %1 ; INT number
    jmp __idt_isr_handler
%endmacro

; interupt service routines
%assign i 0
%rep 8
    isr i
%assign i i + 1
%endrep

isr_except 8
isr 9
isr_except 10
isr_except 11
isr_except 12
isr_except 13
isr_except 14

%assign i 15
%rep 17
    isr i
%assign i i + 1
%endrep

; IRQ32 (timer) needs special handling for context switching
; We can't go through iterrupt_handler because it receives the frame by value (copy),
; so modifications to the frame don't affect the actual stack
global isr32
extern task_switch_handler
isr32:
    cmp byte [rel wos_smap_active], 0
    je .access_closed
    clac
.access_closed:
    cld
    clear_live_tf

    ; Same-CPL kernel interrupts only push RIP, CS, and RFLAGS. The scheduler
    ; consumes a uniform InterruptFrame with RSP and SS as well, so build that
    ; full frame explicitly below the hardware frame instead of letting C++
    ; write synthetic RSP/SS into stack slots that do not exist.
    test qword [rsp + 8], 0x3
    jnz .user_or_outer

    sub rsp, GPREGS_SIZE + INTERRUPT_FRAME_SIZE

    mov [rsp + 0x00], r15
    mov [rsp + 0x08], r14
    mov [rsp + 0x10], r13
    mov [rsp + 0x18], r12
    mov [rsp + 0x20], r11
    mov [rsp + 0x28], r10
    mov [rsp + 0x30], r9
    mov [rsp + 0x38], r8
    mov [rsp + 0x40], rbp
    mov [rsp + 0x48], rdi
    mov [rsp + 0x50], rsi
    mov [rsp + 0x58], rdx
    mov [rsp + 0x60], rcx
    mov [rsp + 0x68], rbx
    mov [rsp + 0x70], rax

    mov qword [rsp + GPREGS_SIZE + 0x00], 32
    mov qword [rsp + GPREGS_SIZE + 0x08], 0

    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x00] ; RIP from hardware frame
    mov [rsp + GPREGS_SIZE + 0x10], rax
    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x08] ; CS from hardware frame
    mov [rsp + GPREGS_SIZE + 0x18], rax
    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x10] ; RFLAGS from hardware frame
    mov [rsp + GPREGS_SIZE + 0x20], rax

    lea rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x18] ; interrupted RSP
    mov [rsp + GPREGS_SIZE + 0x28], rax
    mov qword [rsp + GPREGS_SIZE + 0x30], 0x10

    jmp task_switch_handler

.user_or_outer:
    push qword 0   ; err_code (none for IRQ)
    push qword 32  ; int_num
    isr_swapgs isr32_entry
    pushq
    jmp task_switch_handler

%assign i 33
%rep 15
    isr i
%assign i i + 1
%endrep

%assign i 48
%rep 208
    isr i
%assign i i + 1
%endrep
'''

OLD_SOURCE_IDENTITIES = {'return_prefix_sha256': '4f87d0c2ef7460d227bb7fef05278eb9eae1b48159f08f2c2fdcac292793e91e', 'gates_sha256': 'e7925fdfbcbae35fa31bbccbab0b7f8d0ffa0a9bf5dfa2c83b7574228530a604'}

def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("bounded owned-session runner requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Sequential driver children only; the subreaper adopts escaped owned
        # compiler descendants. An original unreaped leader reserves the SID.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result


def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)


def reap_group(process: subprocess.Popen, deadline: float) -> None:
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if owned_session(process.pid):
                raise ValueError("owned session/adopted descendants remain after leader reap")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session not proven absent; preserve scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("90-second whole budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("child log/private scratch exceeded bounded budget")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("90-second whole budget exhausted")
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {sig: signal.signal(sig, signal.SIG_IGN)
                            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 6),
            "leader_reaped": True, "owned_session_absent": True, "log": pin(log)}


def dependency_pins(depfile: Path) -> list[dict]:
    if depfile.stat().st_size > LOG_BUDGET:
        raise ValueError("compiler dependency file exceeded 1MiB")
    body = depfile.read_text().replace("\\\n", " ")
    if ": " not in body:
        raise ValueError("actual compiler depfile has no target separator")
    words = re.findall(r"(?:\\.|[^\s])+", body.split(": ", 1)[1])
    paths = {Path(re.sub(r"\\(.)", r"\1", word)).resolve(strict=True) for word in words}
    if len(paths) > 2048:
        raise ValueError("actual compiler dependency count exceeded 2048")
    return [pin(p) for p in sorted(paths) if p.is_file()]


def macro_prefix(text: str) -> str:
    marker = "%macro build_user_return_from_ptrs 0"
    if text.count(marker) != 1:
        raise ValueError("unique actual macro-prefix boundary")
    prefix = text[:text.index(marker)]
    for name in ("build_kernel_return_from_ptrs", "build_kernel_return_from_stack"):
        if prefix.count("%macro " + name + " 0") != 1:
            raise ValueError("whole actual kernel return macro missing")
    return prefix


def actual_abi(scratch: Path) -> list[dict]:
    """Compile complete actual packed types, not a duplicate Host layout."""
    def packed(path: Path, name: str) -> tuple[str, dict]:
        source=path.read_text(); anchor="struct "+name+" {"
        if source.count(anchor)!=1:raise ValueError("unique complete actual type required: "+name)
        start=source.index(anchor); marker="} __attribute__((packed));"
        end=source.index(marker,start)+len(marker)
        body=source[start:end]
        return body,dict(path=str(path),name=name,bytes=len(body.encode()),sha256=hashlib.sha256(body.encode()).hexdigest())
    gp,a=packed(CPU_HEADER,"GPRegs"); frame,b=packed(GATES_HEADER,"InterruptFrame"); context,c=packed(TASK_HEADER,"Context")
    include=scratch/"include"
    (include/"platform/sched").mkdir(parents=True)
    (include/"platform/sched/frame_class.hpp").write_bytes(FRAME_CLASS.read_bytes())
    text=("#pragma once\n#include <cstddef>\n#include <cstdint>\n"
          "namespace ker::mod::cpu {\n"+gp+"\n}\nnamespace ker::mod::gates {\n"+frame+"\n}\n"
          "#include <platform/sched/frame_class.hpp>\nnamespace ker::mod::sched::task {\n"+context+"\n}\n"
          "static_assert(sizeof(ker::mod::cpu::GPRegs)==120);\n"
          "static_assert(sizeof(ker::mod::gates::InterruptFrame)==56);\n"
          "static_assert(offsetof(ker::mod::sched::task::Context,frame)-offsetof(ker::mod::sched::task::Context,regs)==136);\n")
    names=("r15","r14","r13","r12","r11","r10","r9","r8","rbp","rdi","rsi","rdx","rcx","rbx","rax")
    for index,name in enumerate(names):
        text+=f"static_assert(offsetof(ker::mod::cpu::GPRegs,{name})=={index*8});\n"
    for index,name in enumerate(("int_num","err_code","rip","cs","flags","rsp","ss")):
        text+=f"static_assert(offsetof(ker::mod::gates::InterruptFrame,{name})=={index*8});\n"
    (include/"actual-abi.hpp").write_text(text)
    return [a,b,c]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    parser.add_argument("--nasm", default=os.environ.get("NASM", "/usr/bin/nasm"))
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    started=time.monotonic(); deadline=started+90
    scratch=scratch_identity=evidence=None
    groups=[]; watched=[]; before=[]; staged=[]; staged_before=[]
    result=dict(pass_=False,errors=[],commands=[],scratch_removed=False,whole_budget_seconds=90,
                scratch_budget_bytes=SCRATCH_BUDGET,command_log_budget_bytes=LOG_BUDGET,
                core_dump_limit_bytes=0,physical_irq_qualified=False,smp_qualified=False,
                kernel_privilege_qualified=False,production_helper_execution=False,full_goal_complete=False)
    handlers={s:signal.getsignal(s) for s in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
    def interrupted(signum,_frame): raise KeyboardInterrupt(f"interrupted by signal {signum}")
    try:
        for s in handlers: signal.signal(s,interrupted)
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        cxx=Path(args.cxx).resolve(strict=True); nasm=Path(args.nasm).resolve(strict=True)
        for tool in (cxx,nasm):
            if not tool.is_file() or not os.access(tool,os.X_OK): raise ValueError("single executable tool path required")
            with tool.open("rb") as f:
                if f.read(4)!=b"\x7fELF": raise ValueError("actual Host ELF tool required")
        if args.evidence_dir is not None:
            evidence=args.evidence_dir.absolute()
            if evidence.parent.resolve(strict=True)!=evidence.parent or os.path.lexists(evidence):
                raise ValueError("fresh evidence directory with canonical existing parent required")
            evidence.mkdir(mode=0o700)
        watched=[Path(__file__).resolve(),CONTEXT,GATES,HELPERS,CPU_HEADER,GATES_HEADER,TASK_HEADER,FRAME_CLASS,cxx,nasm]
        before=[pin(p) for p in watched]; result["source_before"]=before
        subreaper(); scratch=Path(tempfile.mkdtemp(prefix="wos-interrupt-frame-",dir="/tmp"))
        s=scratch.stat(); scratch_identity=(s.st_dev,s.st_ino,s.st_uid)
        if s.st_uid!=os.getuid() or scratch.is_symlink(): raise ValueError("fresh owned scratch changed")
        inc=scratch/"include/platform/asm";inc.mkdir(parents=True)
        (inc/"helpers.asm").write_bytes(HELPERS.read_bytes())
        result["actual_type_definitions"]=actual_abi(scratch)
        (scratch/"harness.cpp").write_text(CPP)
        prefix=macro_prefix(CONTEXT.read_text())
        if (hashlib.sha256(OLD_RETURN_PREFIX.encode()).hexdigest()!=OLD_SOURCE_IDENTITIES["return_prefix_sha256"] or
                hashlib.sha256(OLD_GATES.encode()).hexdigest()!=OLD_SOURCE_IDENTITIES["gates_sha256"]):
            raise ValueError("unchanged genuine old assembly control bytes required")
        for name,text in (("returns",prefix+ASM),("old-returns",OLD_RETURN_PREFIX+ASM),
                          ("gates",GATES.read_text()),("old-gates",OLD_GATES)):
            (scratch/(name+".asm")).write_text(text)
        result["actual_macro_prefix"]=dict(bytes=len(prefix.encode()),sha256=hashlib.sha256(prefix.encode()).hexdigest())
        result["negative_sources"]=OLD_SOURCE_IDENTITIES
        staged=sorted(p for p in scratch.rglob("*") if p.is_file());staged_before=[pin(p) for p in staged]
        result["staged_sources_before"]=staged_before
        counter=0
        def run(argv,expected=0,marker=None):
            nonlocal counter
            log=scratch/f"command-{counter:02}.log";counter+=1
            row=command(argv,log,scratch,deadline,groups);result["commands"].append(row)
            text=log.read_text(errors="replace")
            if row["exit"]!=expected or (marker is not None and marker not in text):
                print(text[-32768:],end="",flush=True)
                raise ValueError(f"actual command {counter-1} exit/oracle mismatch: {row['exit']}")
            return text
        def assemble(source,name,kasan):
            target=scratch/name
            argv=[str(nasm),"-f","elf64","-I",str(scratch/"include")+"/"]
            if kasan:argv.append("-DWOS_KASAN=1")
            run(argv+[str(scratch/source),"-o",str(target)])
            return target
        gate=assemble("gates.asm","gates.o",False)
        old_gate=assemble("old-gates.asm","old-gates.o",False)
        def link(obj,gates,name,kasan):
            target=scratch/name;dep=scratch/(name+".d")
            run([str(cxx),"-std=c++23","-O1","-g0","-fno-omit-frame-pointer","-mno-red-zone",
                 "-Wall","-Wextra","-Werror","-Wl,-z,noexecstack","-DFIXTURE_KASAN="+str(int(kasan)),"-MD","-MF",str(dep),
                 "-I",str(scratch/"include"),str(scratch/"harness.cpp"),str(obj),str(gates),"-o",str(target)])
            result.setdefault("emitted_dependencies_after_only",[]).append(dict(binary=name,rows=dependency_pins(dep)))
            return target
        current={}
        for kasan in (False,True):
            obj=assemble("returns.asm",f"returns-{int(kasan)}.o",kasan);current[kasan]=obj
            binary=link(obj,gate,f"current-{int(kasan)}",kasan)
            text=run([str(binary)])
            match=re.fullmatch(r"PASS_NATIVE_FRAME cases=(\d+) returns=(\d+) entries=96 assertions=(\d+) kasan="+str(int(kasan))+r" actual_iretq=1 physical_irq=0 smp=0\n",text)
            if match is None or int(match[1])!=1344 or int(match[2])!=1248 or int(match[3])<=0:
                raise ValueError("complete actual native positive census missing")
            result.setdefault("positive_census",[]).append(dict(kasan=kasan,cases=int(match[1]),returns=int(match[2]),entries=96,assertions=int(match[3])))
            print(text,end="",flush=True)
        old_ret=assemble("old-returns.asm","old-returns.o",True)
        binary=link(old_ret,gate,"negative-old-return",True)
        run([str(binary),"--return-only"],1,"FAIL_NATIVE_FRAME: normalized GP/frame and five-word physical tail")
        binary=link(current[True],old_gate,"negative-old-entry",True)
        run([str(binary),"--entry-only"],1,"FAIL_NATIVE_FRAME: entry preserves actual five-word hardware RSP/SS")
        result["negative_controls_compiled_then_intended_failure"]=2
        result["pass_"]=True
    except (OSError,ValueError,TimeoutError,subprocess.TimeoutExpired,KeyboardInterrupt) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for s in handlers:signal.signal(s,signal.SIG_IGN)
        try:
            try:
                if staged_before:
                    result["staged_sources_after"]=[pin(p) for p in staged]
                    if result["staged_sources_after"]!=staged_before:result["errors"].append("staged sources drift")
            except (OSError,ValueError) as error:result["errors"].append("staged postflight: "+repr(error))
            try:
                if before:
                    result["source_after"]=[pin(p) for p in watched]
                    if result["source_after"]!=before:result["errors"].append("actual source/tool drift")
            except (OSError,ValueError) as error:result["errors"].append("source postflight: "+repr(error))
            if scratch is not None:
                result["scratch_allocated_bytes_before_cleanup"]=allocated(scratch)
                result["owned_sessions_pending"]=list(groups)
                if evidence is not None:
                    try:
                        saved=[]
                        for p in [*staged,*sorted(scratch.glob("command-*.log")),*sorted(scratch.glob("*.d"))]:
                            if not p.is_file():continue
                            if p.stat().st_size>LOG_BUDGET:raise ValueError("small saved evidence limit")
                            q=evidence/p.relative_to(scratch);q.parent.mkdir(parents=True,exist_ok=True)
                            with q.open("xb") as f:f.write(p.read_bytes())
                            saved.append(pin(q))
                        result["retained_small_evidence"]=saved
                    except (OSError,ValueError) as error:result["errors"].append("save evidence: "+repr(error))
                s=scratch.stat()
                if groups or scratch.is_symlink() or (s.st_dev,s.st_ino,s.st_uid)!=scratch_identity:
                    result["errors"].append("uncertain owned session/scratch; preserved")
                else:
                    shutil.rmtree(scratch);result["scratch_removed"]=not os.path.lexists(scratch)
                    if not result["scratch_removed"]:result["errors"].append("scratch remains")
            result["elapsed_seconds"]=time.monotonic()-started
            if time.monotonic()>=deadline:result["errors"].append("whole90s cleanup/publication deadline")
        except (OSError,ValueError,TimeoutError) as error:result["errors"].append("cleanup: "+repr(error))
        finally:
            for s,h in handlers.items():signal.signal(s,h)
    result["pass_"]=result["pass_"] and not result["errors"] and result["scratch_removed"]
    result["qualification"]="PASS_ACTUAL_HOST_NATIVE64_IRET_AND_FINITE_ISR_ONLY" if result["pass_"] else "FAIL"
    if evidence is not None:
        with (evidence/"result.json").open("x") as f:json.dump(result,f,sort_keys=True,indent=2);f.write("\n")
    print(json.dumps(result,sort_keys=True),flush=True)
    return 0 if result["pass_"] and time.monotonic()<deadline else 1


if __name__=="__main__":
    raise SystemExit(main())
