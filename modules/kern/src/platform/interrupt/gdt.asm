bits 64

global loadGdt

loadGdt:
    lgdt [rdi]
    push 0x08
    lea rax, [rel .flush]
    push rax
    retfq

    .flush:
        mov ax, 0x10
        mov ds, ax
        mov fs, ax
        ; NOTE: Do NOT load GS here - we use GS.base for per-CPU data
        ; and loading a selector into GS zeroes GS.base on x86-64
        mov ss, ax
        mov es, ax
        ret