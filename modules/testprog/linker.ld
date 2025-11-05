OUTPUT_FORMAT(elf64-x86-64)
ENTRY(_start)

PHDRS
{
    text PT_LOAD FLAGS(5);          /* R+X */
    rodata PT_LOAD FLAGS(4);        /* R */
    data PT_LOAD FLAGS(6);          /* RW */
    tls PT_TLS FLAGS(4);            /* R (TLS template) */
    dynamic PT_DYNAMIC FLAGS(6);    /* RW */
}

SECTIONS
{
    /* ELF header*/
    PROVIDE_HIDDEN(__ehdr_start = 0x1000);

    . = 0x500000;

    /* Code sections */
    .text : ALIGN(4K) {
        *(.text.unlikely .text.*_unlikely .text.unlikely.*)
        *(.text.exit .text.exit.*)
        *(.text.startup .text.startup.*)
        *(.text.hot .text.hot.*)
        *(.text .stub .text.* .gnu.linkonce.t.*)
    }

    /* Init and fini sections for constructors/destructors */
    .init : {
        KEEP(*(.init))
    }

    .fini : {
        KEEP(*(.fini))
    }

    .plt : { *(.plt) *(.plt.*) }
    .iplt : { *(.iplt) }

    /* Read-only data */
    .rodata : ALIGN(4K) {
        *(.rodata .rodata.* .gnu.linkonce.r.*)
    }

    .eh_frame_hdr : { *(.eh_frame_hdr) *(.eh_frame_entry .eh_frame_entry.*) }
    .eh_frame : ONLY_IF_RO { KEEP(*(.eh_frame)) *(.eh_frame.*) }
    .gcc_except_table : ONLY_IF_RO { *(.gcc_except_table .gcc_except_table.*) }

    /* Data sections */
    .data : ALIGN(4K) {
        *(.data .data.* .gnu.linkonce.d.*)
        SORT(CONSTRUCTORS)
    } :data

    .eh_frame : ONLY_IF_RW { KEEP(*(.eh_frame)) *(.eh_frame.*) }
    .gcc_except_table : ONLY_IF_RW { *(.gcc_except_table .gcc_except_table.*) }

    /* Thread-local storage */
    .tdata : {
        PROVIDE_HIDDEN(__tls_start = .);
        *(.tdata .tdata.* .gnu.linkonce.td.*)
    } :tls :data
    .tbss : {
        *(.tbss .tbss.* .gnu.linkonce.tb.*)
        *(.tcommon)
        PROVIDE_HIDDEN(__tls_end = .);
    } :tls :data

    /* Constructor and destructor arrays */
    .preinit_array : {
        PROVIDE_HIDDEN(__preinit_array_start = .);
        KEEP(*(.preinit_array))
        PROVIDE_HIDDEN(__preinit_array_end = .);
    } :data

    .init_array : {
        PROVIDE_HIDDEN(__init_array_start = .);
        KEEP(*(SORT_BY_INIT_PRIORITY(.init_array.*) SORT_BY_INIT_PRIORITY(.ctors.*)))
        KEEP(*(.init_array EXCLUDE_FILE(*crtbegin.o *crtbegin?.o *crtend.o *crtend?.o) .ctors))
        PROVIDE_HIDDEN(__init_array_end = .);
    } :data

    .fini_array : {
        PROVIDE_HIDDEN(__fini_array_start = .);
        KEEP(*(SORT_BY_INIT_PRIORITY(.fini_array.*) SORT_BY_INIT_PRIORITY(.dtors.*)))
        KEEP(*(.fini_array EXCLUDE_FILE(*crtbegin.o *crtbegin?.o *crtend.o *crtend?.o) .dtors))
        PROVIDE_HIDDEN(__fini_array_end = .);
    } :data

    /* Global Offset Table and relocations */
    .got : { *(.got.plt) *(.igot.plt) *(.got) *(.igot) } :data
    .got.plt : { *(.got.plt) } :data

    /* Dynamic sections (for PIE, though we're static) */
    .dynamic : { *(.dynamic) } :data :dynamic
    .dynsym : { *(.dynsym) } :data
    .dynstr : { *(.dynstr) } :data
    .hash : { *(.hash) } :data
    .gnu.hash : { *(.gnu.hash) } :data
    .gnu.version : { *(.gnu.version) } :data
    .gnu.version_d : { *(.gnu.version_d) } :data
    .gnu.version_r : { *(.gnu.version_r) } :data
    .rela.dyn : { *(.rela.dyn) } :data
    .rela.plt : { *(.rela.plt) } :data
    .rel.dyn : { *(.rel.dyn) } :data
    .rel.plt : { *(.rel.plt) } :data
    .relr.dyn : { *(.relr.dyn) } :data

    /* BSS section */
    .bss : ALIGN(4K) {
        *(.dynbss)
        *(.bss .bss.* .gnu.linkonce.b.*)
        *(COMMON)
    } :data

    /* Discard unneeded sections */
    /DISCARD/ : {
        *(.note.GNU-stack)
        *(.gnu_debuglink)
        *(.gnu.lto_*)
        *(.comment)
    }
}
