bits 64

global _wOS_enableSSE_asm
global _wOS_enableXSave_asm
global _wOS_xsave_area_size

_wOS_enableSSE_asm:
    mov rax, cr0
    and ax, 0xFFFB		;clear coprocessor emulation CR0.EM
    or ax, 0x2			;set coprocessor monitoring  CR0.MP
    mov cr0, rax
    mov rax, cr4
    or ax, 3 << 9		;set CR4.OSFXSR and CR4.OSXMMEXCPT at the same time
    mov cr4, rax
    ret

; Enable XSAVE and set XCR0 to save x87 + SSE + AVX (if supported).
; Returns: rax = required xsave area size (bytes), 0 if XSAVE not supported.
_wOS_enableXSave_asm:
    push rbx             ; RBX is callee-saved; cpuid clobbers it

    ; Check CPUID.1:ECX bit 26 (XSAVE support)
    mov eax, 1
    cpuid
    bt ecx, 26
    jnc .no_xsave

    ; Enable CR4.OSXSAVE (bit 18)
    mov rax, cr4
    or eax, 1 << 18
    mov cr4, rax

    ; Build XCR0 mask: bit 0 = x87, bit 1 = SSE (mandatory)
    mov ecx, 0          ; XCR0
    xgetbv               ; current XCR0 -> edx:eax
    or eax, 0x3          ; x87 + SSE

    ; Check AVX support (CPUID.1:ECX bit 28)
    push rax
    mov eax, 1
    cpuid
    bt ecx, 28
    pop rax
    jnc .no_avx
    or eax, 0x4          ; AVX (bit 2)
.no_avx:
    ; Write XCR0
    xor edx, edx
    mov ecx, 0
    xsetbv

    ; Query required xsave area size: CPUID.(EAX=0Dh, ECX=0) -> EBX = size
    mov eax, 0x0D
    xor ecx, ecx
    cpuid
    mov eax, ebx         ; return size in rax
    pop rbx
    ret

.no_xsave:
    xor eax, eax         ; return 0 = not supported
    pop rbx
    ret
