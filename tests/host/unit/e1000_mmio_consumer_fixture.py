"""Unchanged complete e1000 consumer for the shared real MM/PCI host driver.

The shared driver supplies finite network/IRQ/clock effects, actual controller
issuers, actual PCI configuration inputs and the complete MM/DMA registries.
There is no fabricated claim, Mapping, Borrow or quiescence-witness factory.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

UNIT = Path(__file__).resolve().parent
ROOT = UNIT.parents[2]
KERN = ROOT / 'modules/kern/src'


def consumer_definitions() -> tuple[str, list[dict], list[Path]]:
    """Splice complete original namespace bodies after shared finite ports.

    WOS_SELFTEST is disabled just for this production-consumer source. Native
    diagnostics have a separate actual kernel fixture; the shared MM runtime
    may keep its selftest-only withholding control enabled in its own TU.
    """
    paths = [KERN / 'dev/e1000e/e1000e.hpp', KERN / 'dev/e1000e/e1000e.cpp']
    definitions = []
    chunks = []
    for path in paths:
        source = path.read_text()
        anchor = 'namespace ker::dev::e1000e {'
        start = source.index(anchor)
        body = source[start:]
        definitions.append(dict(path=str(path.relative_to(ROOT)), anchor=anchor,
                                line=source.count('\n', 0, start) + 1,
                                bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest()))
        chunks.append(body)
    prefix = '''
#ifdef WOS_SELFTEST
#define WOS_E1000_HOST_RESTORE_SELFTEST 1
#undef WOS_SELFTEST
#endif
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <string_view>
#include <utility>
#include <dev/dma/lifecycle.hpp>
#include <dev/e1000e/dma.hpp>
#include <dev/pci.hpp>
#include <dev/pci_driver_claim.hpp>
#include <platform/mm/mmio_mapping.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
'''
    suffix = '''
#ifdef WOS_E1000_HOST_RESTORE_SELFTEST
#define WOS_SELFTEST 1
#undef WOS_E1000_HOST_RESTORE_SELFTEST
#endif
'''
    return prefix + '\n'.join(chunks) + suffix, definitions, paths
