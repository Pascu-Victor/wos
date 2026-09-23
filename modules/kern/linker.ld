/* Tell the linker that we want an x86_64 ELF64 output file */
OUTPUT_FORMAT(elf64-x86-64)
OUTPUT_ARCH(i386:x86-64)

/* We want the symbol _start to be our entry point */
ENTRY(_start)

/* Define the program headers we want so the bootloader gives us the right */
/* MMU permissions; this also allows us to exert more control over the linking */
/* process. */
PHDRS
{
    headers PT_PHDR PHDRS;
    text    PT_LOAD FILEHDR PHDRS FLAGS(5); /* PF_R | PF_X */
    rodata  PT_LOAD FLAGS(4);               /* PF_R */
    note    PT_NOTE FLAGS(4);
    data    PT_LOAD FLAGS(6);                /* PF_R | PF_W */
    dynamic PT_DYNAMIC FLAGS(6);
}

SECTIONS
{
    /* A zero-based ET_DYN image lets Limine select a bounded higher-half slide. */
    /* FILEHDR/PHDRS remain inside the first read/execute PT_LOAD, so the runtime */
    /* image base is exactly the relocated value of __kernel_image_start. */
    __kernel_link_base = 0;
    . = SIZEOF_HEADERS;
    PROVIDE(__kernel_image_start = . - SIZEOF_HEADERS);

    __kernel_text_start = .;
    .text : {
        __kernel_init_start = .;
        KEEP(*(.text.init .text.init.*))
        __kernel_init_end = .;

        __kernel_trampoline_start = .;
        KEEP(*(.text.trampoline .text.trampoline.*))
        __kernel_trampoline_end = .;

        *(.text .text.*)
    } :text
    __kernel_text_end = .;

    /* Move to the next memory page for .rodata */
    . = ALIGN(CONSTANT(MAXPAGESIZE));

    __kernel_rodata_start = .;
    .rodata : {
        *(.rodata .rodata.*)
    } :rodata

    /* Compiler-generated sanitizer/KCFI metadata is immutable at runtime. */
    __kernel_sanitizer_start = .;
    .kcfi_traps : { *(.kcfi_traps) } :rodata
    .sancov_guards : { *(.sancov_guards) } :rodata
    __kernel_sanitizer_end = .;

    __kernel_relocation_start = .;
    .rela.dyn : {
        *(.rela .rela.*)
    } :rodata
    .relr.dyn : {
        *(.relr.dyn)
    } :rodata
    __kernel_relocation_end = .;

    .dynsym : { *(.dynsym) } :rodata
    .dynstr : { *(.dynstr) } :rodata
    .gnu.hash : { *(.gnu.hash) } :rodata
    .hash : { *(.hash) } :rodata

    /* Keep only the deterministic GNU build identity. Runtime image catalogs
     * consume its PT_NOTE to prove that host symbols match the live kernel. */
    .note.gnu.build-id : {
        __kernel_build_id_start = .;
        KEEP(*(.note.gnu.build-id))
        __kernel_build_id_end = .;
    } :rodata :note

    /* In-kernel self-test registry (populated by KTEST() macro, iterated by run_all()) */
    .ktests : {
        __start_ktests = .;
        KEEP(*(.ktests))
        __stop_ktests = .;
    } :rodata

    /* Destructive device retirement tests run after all ordinary tests. */
    .ktests_final : {
        __start_ktests_final = .;
        KEEP(*(.ktests_final))
        __stop_ktests_final = .;
    } :rodata
    __kernel_rodata_end = .;

    /* Move to the next memory page for .data */
    . = ALIGN(CONSTANT(MAXPAGESIZE));

    __kernel_data_start = .;
    .data : {
        *(.data .data.*)

        /* Place the sections that contain the Limine requests as part of the .data */
        /* output section. */
        KEEP(*(.requests_start_marker))
        KEEP(*(.requests))
        KEEP(*(.requests_end_marker))
    } :data

    /* Constructors and destructors arrays */
    .preinit_array : {
        __preinit_array_start = .;
        KEEP(*(.preinit_array))
        __preinit_array_end = .;
    } :data

    .init_array : {
        __init_array_start = .;
        KEEP(*(SORT_BY_INIT_PRIORITY(.init_array.*) SORT_BY_INIT_PRIORITY(.ctors.*)))
        KEEP(*(.init_array .ctors))
        __init_array_end = .;
    } :data

    .fini_array : {
        __fini_array_start = .;
        KEEP(*(SORT_BY_INIT_PRIORITY(.fini_array.*) SORT_BY_INIT_PRIORITY(.dtors.*)))
        KEEP(*(.fini_array .dtors))
        __fini_array_end = .;
    } :data

    __kernel_dynamic_start = .;
    .dynamic : {
        *(.dynamic)
    } :data :dynamic
    __kernel_dynamic_end = .;

    .got : {
        *(.got .got.*)
    } :data

    /* NOTE: .bss needs to be the last thing mapped to :data, otherwise lots of */
    /* unnecessary zeros will be written to the binary. */
    .bss : {
        __kernel_bss_start = .;
        . = ALIGN(CONSTANT(MAXPAGESIZE));
        __kernel_emergency_stack_region_start = .;
        KEEP(*(.bss.stack_chk_stack))
        __kernel_emergency_stack_region_end = .;
        *(.bss .bss.*)
        *(COMMON)
        __kernel_bss_end = .;
    } :data

    /* End of kernel image — page-aligned so MMIO regions can be mapped right after. */
    . = ALIGN(CONSTANT(MAXPAGESIZE));
    __kernel_data_end = .;
    PROVIDE(__kernel_end = .);

    /* Discard non-identity notes and .eh_frame* since they may cause issues on some hosts. */
    /* Also discard the program interpreter section since we do not need one. This is */
    /* more or less equivalent to the --no-dynamic-linker linker flag, except that it */
    /* works with ld.gold. */
    /DISCARD/ : {
        *(.eh_frame*)
        *(.note)
        *(.note.*)
        *(.interp)
    }
}
