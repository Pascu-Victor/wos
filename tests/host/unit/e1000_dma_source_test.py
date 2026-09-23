#!/usr/bin/env python3
"""Integration checks for the e1000 managed DMA and retirement contract."""
from pathlib import Path
from memacc_source_test import function_body

ROOT = Path(__file__).resolve().parents[3]
source = (ROOT / 'modules/kern/src/dev/e1000e/e1000e.cpp').read_text()

def order(name, *tokens):
    body = function_body(source, name)
    pos = 0
    for token in tokens:
        pos = body.index(token, pos) + len(token)

for forbidden in ('virt_to_phys', 'page_alloc(', 'page_free(', 'pci_enable_bus_master('):
    assert forbidden not in source, forbidden
order('reset_device', 'reg_write(dev, REG_CTRL', 'sleep_us(2)', 'wait_clear(dev, REG_CTRL')
order('init_device', 'pci_set_bus_master(pci_dev, false)', 'reset_device(dev)',
      'configure_coherence(dev)', 'init_rx(dev)', 'init_tx(dev)', 'program_rings(dev)',
      'pci_set_bus_master(pci_dev, true)', 'request_irq(', 'netdev_register(', 'RCTL_EN')
order('process_rx_budget', 'RX_STATUS_DD', 'lfence', 'managed::rx_valid', '++processed',
      '.complete(dma_api', 'pkt_alloc_rx()', 'bind_rx', 'mfence', 'REG_RDT', 'netdev_rx')
order('process_tx', 'TX_STATUS_DD', 'lfence', '.complete(dma_api', 'pkt_free')
order('e1000_start_xmit', 'CallbackGuard const GUARD', 'managed::tx_valid', 'lock_irqsave',
      'buffer.bind', 'buffer.expose', 'buffer.submission.address', 'mfence', 'REG_TDT')
for callback, access in [('e1000_poll', 'process_rx_budget'), ('e1000_irq_handler', 'REG_ICR')]:
    order(callback, 'CallbackGuard const GUARD', '!GUARD', access)
order('retire_device', 'hold_kernel_shutdown', 'compare_exchange_strong', 'lifecycle::retire', 'retirement.store')
assert 'delete dev' not in source
for callback in ('process_rx_budget', 'process_tx', 'e1000_poll', 'e1000_irq_handler', 'e1000_start_xmit'):
    body = function_body(source, callback)
    assert 'allocate_streaming' not in body and 'dma::release' not in body
power = (ROOT / 'modules/kern/src/platform/power/power.cpp').read_text()
assert power.index('vfs_shutdown_unmount_all') < power.index('e1000e_shutdown()') < power.index('smt::halt_other_cores()')
print('e1000 managed address, completion, callback and terminal integration passed')
