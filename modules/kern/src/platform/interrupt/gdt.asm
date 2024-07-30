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
        mov gs, ax
        mov ss, ax
        mov es, ax
        ret