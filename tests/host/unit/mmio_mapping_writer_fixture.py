"""Unchanged native mapper entrypoints bound to the shared actual MM registry."""
from __future__ import annotations

import hashlib
from pathlib import Path

from cpu_maintenance_runtime_test import actual_definition

UNIT = Path(__file__).resolve().parent
ROOT = UNIT.parents[2]
KERN = ROOT / 'modules/kern/src'


def writer_definitions() -> tuple[str, list[dict], list[Path]]:
    cpp = KERN / 'platform/mm/virt.opt.cpp'
    header = KERN / 'platform/mm/virt.hpp'
    source = cpp.read_text()
    definitions = []

    def take(text: str, anchor: str, semicolon: bool = False) -> str:
        body = actual_definition(text, anchor, semicolon)
        definitions.append(dict(anchor=anchor, bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest()))
        return body

    helpers = source[source.index('[[gnu::always_inline]] auto typed_mmio_table_allowed('):
                     source.index('// All objects passed from this excluded TU')]
    definitions.append(dict(anchor='actual typed_mmio helpers', bytes=len(helpers.encode()),
                            sha256=hashlib.sha256(helpers.encode()).hexdigest()))
    names = ['auto pte_raw(const paging::PageTableEntry& e)', 'auto index_of(', 'auto entry_at(PageTable*',
             'auto is_aligned_to(', 'auto is_reserved_leaf(const PageTableEntry& entry) -> bool {',
             'auto leaf_entry(PageTable* root, vaddr_t vaddr) -> PageTableEntry* {',
             'auto kernel_hhdm_2m_flags_match(', 'auto kernel_hhdm_entry_raw(',
             'auto kernel_hhdm_2m_mapping_matches(',
             'auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted)',
             'auto map_2m_page(',
             'void map_page(PageTable*', 'void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, const uint64_t FLAGS) {',
             'void map_page_batched(PageMapBatch* batch, const', 'void flush_page_map_batch(PageMapBatch* batch) {',
             'void map_range(PageTable* page_table, Range range, uint64_t flags, uint64_t offset)',
             'void unmap_page(PageTable* page_table, vaddr_t vaddr)']
    prelude = (UNIT / 'mmio_mapping_writer_ports.inc').read_text()
    text = prelude + '\n' + take(header.read_text(), 'struct PageMapBatch {', True) + '\n' + helpers
    text += '\n'.join(take(source, name) for name in names)
    text += '''
}  // namespace mmio_writer_host
#ifdef WOS_MMIO_WRITER_RESTORE_SELFTEST
#define WOS_SELFTEST 1
#undef WOS_MMIO_WRITER_RESTORE_SELFTEST
#endif
'''
    return text, definitions, [cpp, header, UNIT / 'mmio_mapping_writer_ports.inc']
