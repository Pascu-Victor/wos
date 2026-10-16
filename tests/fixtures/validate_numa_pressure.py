#!/usr/bin/env python3
"""Strict saved NUMA pressure protocol; no execution or physical-closure authority."""
from pathlib import Path
import argparse
import copy
import hashlib
import json
import re
import stat

MAX_BYTES = 4 << 20
MAX_ENTRIES = 4096
MAX_PAYLOAD = 4 << 30
SNAPSHOT_BYTES = 262144
PAGE = 4096
LARGE = 2 << 20
U64 = (1 << 64) - 1
FNV_OFFSET = 14695981039346656037
FNV_PRIME = 1099511628211
PHASES = ("prepared", "bind-prefix", "strict-refusal", "preferred", "prefix-recheck", "payload-cleanup", "recovery")
CHECKPOINT_KEYS = "cpu affinity mode mask target owned_bytes as_current as_peak as_denied as_reason as_soft as_hard usage_rc policy_rc error stamp"
ENTRY_KEYS = "index role address bytes node resident zero_pages unavailable digest allocate_rc release_rc output_changed owned"
FINAL_KEYS = ("pass first_error cleanup_error large_rc large_sentinel strict_rc strict_sentinel preferred_rc recovery_rc attempted released "
              "outstanding high_bytes policy_restore affinity_restore original_mode original_mask original_affinity prefix_entries "
              "physical_closure metadata_cause final_as_current final_as_peak final_as_denied final_usage_rc final_checkpoint_error")

# Exact current physical descriptor metadata, populated from the pinned producer.
OWNER_META = {'user_private_mapping': ('process address-space lifetime',
                          'owner_teardown',
                          'bounded by mapped private pages'),
 'user_file_mapping': ('mapping lifetime', 'owner_teardown', 'bounded by resident file-mapping pages'),
 'user_file_cache': ('file-cache entry lifetime',
                     'pressure_reclaim',
                     'bounded by resident cached file pages'),
 'user_executable_mapping': ('process image lifetime',
                             'owner_teardown',
                             'bounded by resident executable pages'),
 'user_thread_stack': ('thread lifetime', 'owner_teardown', 'bounded by resident thread-stack pages'),
 'user_thread_tls': ('thread lifetime', 'owner_teardown', 'bounded by resident TLS pages'),
 'user_shared_memory': ('shared-memory object lifetime',
                        'owner_teardown',
                        'bounded by live shared-memory objects'),
 'anonymous_zero_page_reserve': ('boot-to-shutdown shared zero-page reserve',
                                 'permanent_reserve',
                                 'exactly one page after first anonymous read mapping'),
 'page_table': ('address-space or kernel-map lifetime',
                'owner_teardown',
                'bounded by mapped virtual-address topology'),
 'page_table_pool_reserve': ('cached tables and boot emergency root reserves',
                             'permanent_reserve',
                             'ordinary cache at most 64 plus emergency roots at most 64 per published node'),
 'physical_allocator_runtime_metadata': ('allocator lifetime',
                                         'permanent_reserve',
                                         'two pages per optional huge-page zone'),
 'kernel_stack': ('kernel task lifetime', 'owner_teardown', 'one fixed stack allocation per non-pooled task'),
 'kernel_stack_pool': ('boot-to-shutdown reserve', 'permanent_reserve', 'fixed boot-selected pool capacity'),
 'kmalloc_slab': ('small-object slab lifetime',
                  'owner_teardown',
                  'live small objects plus bounded per-CPU magazines'),
 'kmalloc_medium': ('medium allocation lifetime', 'owner_teardown', 'buddy-rounded live medium allocations'),
 'kmalloc_large': ('large allocation lifetime', 'owner_teardown', 'buddy-rounded live large allocations'),
 'kmalloc_debug': ('diagnostic record lifetime',
                   'diagnostic_lifetime',
                   'disabled in normal builds; bounded by tracked allocations'),
 'buffer_cache_data': ('buffer-cache entry lifetime', 'pressure_reclaim', 'configured cache byte limit'),
 'buffer_cache_metadata_reserve': ('boot-to-shutdown allocator reserve',
                                   'permanent_reserve',
                                   'one 256 KiB BufHead arena after first use'),
 'buffer_cache_metadata': ('BufHead object lifetime',
                           'pressure_reclaim',
                           'arenas containing live buffer objects'),
 'tmpfs_data': ('tmpfs inode/block lifetime',
                'pressure_reclaim',
                'bounded by live tmpfs file data and mount capacity'),
 'xfs_inode_metadata_reserve': ('boot-to-shutdown allocator reserve',
                                'permanent_reserve',
                                'one 256 KiB inode-object arena after first use'),
 'xfs_inode_metadata': ('cached XFS inode lifetime',
                        'pressure_reclaim',
                        'arenas containing live or cached inodes'),
 'xfs_transaction_metadata_reserve': ('boot-to-shutdown allocator reserve',
                                      'permanent_reserve',
                                      'one 256 KiB transaction-object arena after first use'),
 'xfs_transaction_metadata': ('XFS transaction lifetime',
                              'owner_teardown',
                              'arenas containing live concurrent transactions'),
 'wki_zone': ('WKI zone lifetime', 'owner_teardown', 'bounded by live local WKI zones'),
 'network_packet_reserve': ('network stack lifetime',
                            'permanent_reserve',
                            'maximum of configured per-NIC baseline and initial RX descriptor demand plus '
                            'fixed RX safety reserve'),
 'network_packet': ('dynamic packet-pool or in-flight packet lifetime',
                    'pressure_reclaim',
                    'growth chunks above baseline plus in-flight packets'),
 'device_ahci_dma': ('AHCI controller lifetime',
                     'permanent_reserve',
                     'fixed command/FIS/table capacity per AHCI port'),
 'device_virtio_dma': ('virtio device lifetime',
                       'permanent_reserve',
                       'fixed queue capacity per virtio device'),
 'device_e1000_dma': ('e1000 device lifetime', 'permanent_reserve', 'fixed RX/TX ring capacity per device'),
 'device_usb_dma': ('USB controller/device lifetime',
                    'permanent_reserve',
                    'bounded by configured controllers, rings, and attached devices'),
 'kasan_shadow': ('diagnostic kernel lifetime',
                  'diagnostic_lifetime',
                  'disabled normally; bounded by populated shadow pages'),
 'kcov_buffer': ('coverage session lifetime',
                 'diagnostic_lifetime',
                 'disabled normally; bounded by configured coverage buffers'),
 'debugger': ('debug session or kernel lifetime', 'owner_teardown', 'bounded by loaded debug metadata'),
 'selftest': ('individual kernel selftest lifetime',
              'diagnostic_lifetime',
              'zero outside isolated selftest builds'),
 'iommu_table': ('IOMMU domain lifetime',
                 'owner_teardown',
                 'root/context/translation pages retained until hardware invalidation and DMA retirement'),
 'dma_buffer': ('managed DMA region lifetime',
                'owner_teardown',
                'exclusive coherent/bounce backing retained until hardware retirement and mapping '
                'invalidation'),
 'kvm_clock': ('boot-to-shutdown hypervisor clock record lifetime',
               'permanent_reserve',
               'at most one page per configured vCPU, retained after MSR registration even on clock '
               'qualification failure'),
 'user_root_control': ('admitted user-root lifetime',
                       'owner_teardown',
                       'one control slot per live user root; empty 96-slot pages are released')}
RESERVE_META = {'firmware_reserved': ('boot_to_shutdown', 'permanent_reserve', 'firmware_memory_map_ranges'),
 'acpi_reclaimable_reserved': ('boot_to_shutdown', 'permanent_reserve', 'ACPI_table_storage'),
 'acpi_nvs_reserved': ('boot_to_shutdown', 'permanent_reserve', 'platform_NVS_storage'),
 'hardware_bad_memory': ('hardware_lifetime', 'permanent_reserve', 'firmware_reported_bad_ranges'),
 'bootloader_reclaimable_reserved': ('boot_to_shutdown', 'permanent_reserve', 'bootloader_handoff_storage'),
 'kernel_and_boot_modules': ('boot_to_shutdown', 'permanent_reserve', 'linked_kernel_and_boot_module_images'),
 'firmware_framebuffer': ('boot_to_shutdown', 'permanent_reserve', 'configured_scanout_surface'),
 'firmware_reserved_mapped': ('boot_to_shutdown', 'permanent_reserve', 'firmware_mapped_reserved_ranges')}
FIXED_META = {
    "physical_allocator_embedded_metadata": ("boot_to_shutdown", "permanent_reserve", "fixed_bytes_per_managed_page"),
    "physical_zone_descriptors": ("boot_to_shutdown", "permanent_reserve", "one_page_per_managed_zone"),
    "per_cpu_physical_page_cache_metadata": ("boot_to_shutdown", "permanent_reserve", "fixed_bytes_per_configured_cpu"),
}
SUMMARY_KEYS = ('schema',
 'total_pages',
 'total_bytes',
 'free_pages',
 'free_bytes',
 'used_pages',
 'used_bytes',
 'owner_pages',
 'owner_bytes',
 'allocator_metadata_pages',
 'allocator_metadata_bytes',
 'zone_descriptor_pages',
 'zone_descriptor_bytes',
 'per_cpu_page_cache_reserve_pages',
 'per_cpu_page_cache_reserve_bytes',
 'identity_pages',
 'identity_bytes',
 'firmware_reserve_pages',
 'firmware_reserve_bytes',
 'physical_address_pages',
 'physical_address_identity_pages',
 'identity_mismatch_pages',
 'untracked_unreclaimable_pages',
 'processes',
 'tasks',
 'kernel_tasks',
 'process_virtual_bytes',
 'process_rss_bytes',
 'process_swap_bytes',
 'process_swap_inflight_bytes',
 'process_shared_bytes',
 'process_pte_bytes',
 'process_code_bytes',
 'process_heap_bytes',
 'process_mmap_bytes',
 'process_stack_bytes',
 'process_low_address_bytes',
 'process_high_runtime_bytes',
 'process_rw_bytes',
 'process_rx_bytes',
 'process_ro_bytes',
 'logical_allocator_bytes',
 'logical_kmalloc_bytes',
 'logical_kmalloc_debug_pool_bytes',
 'logical_mini_slab_bytes',
 'logical_cache_bytes')
SWAP_KEYS = ('resident_candidates',
 'swapped_pages',
 'pageout_inflight',
 'pagein_inflight',
 'pageout_attempts',
 'pageout_successes',
 'pageout_failures',
 'pagein_attempts',
 'pagein_successes',
 'pagein_failures',
 'pagein_corruption',
 'pageout_latency_us',
 'pageout_latency_max_us',
 'pagein_latency_us',
 'pagein_latency_max_us')
ZONE_KEYS = ("id name start len_bytes page_count total_pages usable_pages free_pages metadata_pages scanned_free_pages "
             "cached_order0_pages has_allocator invalid_allocator bad_order free_count_mismatch").split()
SIGNED = {"usage_rc", "policy_rc", "error", "allocate_rc", "release_rc", "first_error", "cleanup_error", "large_rc", "large_sentinel",
          "strict_rc", "strict_sentinel", "preferred_rc", "recovery_rc", "policy_restore", "affinity_restore", "final_usage_rc", "final_checkpoint_error"}


def require(value, why):
    if not value:
        raise ValueError(why)


def fnv1a64(raw):
    value = FNV_OFFSET
    for byte in raw:
        value = ((value ^ byte) * FNV_PRIME) & U64
    return value


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def invalid_number(_value):
    raise ValueError("noninteger JSON number")


def integer(value, key):
    require(type(value) is int, "integer field: " + key)
    require(-(1 << 63) <= value < (1 << 63) if key in SIGNED else 0 <= value <= U64, "bounded field: " + key)
    return value


def decimal(value):
    require(re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is not None, "canonical unsigned decimal")
    result = int(value)
    require(result <= U64, "unsigned64 decimal")
    return result


def escaped(value):
    return "".join("%" + format(byte, "02X") if byte <= 32 or byte in (37, 61) else chr(byte) for byte in value.encode("ascii"))


def wire_string(value):
    require(bool(value), "empty producer string")
    raw = bytearray()
    index = 0
    while index < len(value):
        if value[index] == "%":
            require(re.fullmatch(r"[0-9A-F]{2}", value[index + 1:index + 3]) is not None, "canonical percent escape")
            raw.append(int(value[index + 1:index + 3], 16))
            index += 3
        else:
            require(33 <= ord(value[index]) <= 126 and value[index] != "=", "producer string byte")
            raw.append(ord(value[index]))
            index += 1
    require(escaped(raw.decode("ascii")) == value, "unnecessary or malformed percent escape")
    return raw.decode("ascii")


def record(line, kind, keys, strings=(), hexes=()):
    parts = line.split(" ")
    require(parts[0] == kind and len(parts) == len(keys) + 1, "exact raw record: " + kind)
    result = {}
    for part, key in zip(parts[1:], keys):
        require(part.startswith(key + "=") and part.count("=") == 1, "ordered raw key: " + key)
        value = part[len(key) + 1:]
        if key in strings:
            result[key] = wire_string(value)
        elif key in hexes:
            require(re.fullmatch(r"0x[0-9a-f]{16}", value) is not None, "actual producer hex64")
            result[key] = int(value, 16)
        else:
            result[key] = decimal(value)
    return result


def raw_lines(raw):
    require(0 < len(raw) < SNAPSHOT_BYTES and raw.endswith(b"\n") and b"\r" not in raw and b"\0" not in raw,
            "complete LF-terminated raw section")
    text = raw.decode("ascii", "strict")
    require(all(32 <= ord(char) <= 126 or char == "\n" for char in text), "printable producer raw bytes")
    lines = text[:-1].split("\n")
    require(all(lines), "no empty raw line")
    return lines


def summary_snapshot(raw):
    lines = raw_lines(raw)
    metadata = {**OWNER_META, **FIXED_META, **RESERVE_META}
    require(len(lines) == 2 + len(metadata) + 2, "complete summary/owner/feature inventory")
    summary = record(lines[0], "summary", SUMMARY_KEYS)
    record(lines[1], "anonymous_swap", SWAP_KEYS)
    require(summary["schema"] == 2 and summary["total_pages"] > 0, "actual summary schema2")
    owners = {}
    keys = "name pages bytes objects lifetime reclaimability scaling_bound".split()
    for line, (name, expected) in zip(lines[2:-2], metadata.items()):
        row = record(line, "physical_owner", keys, strings=("name", "lifetime", "reclaimability", "scaling_bound"))
        require(row["name"] == name and tuple(row[key] for key in ("lifetime", "reclaimability", "scaling_bound")) == expected,
                "complete ordered actual physical owner descriptor")
        require(row["bytes"] == row["pages"] * PAGE <= U64, "owner page/byte identity")
        owners[name] = row
    for line, name in zip(lines[-2:], ("page_callers", "kmalloc_debug")):
        feature = record(line, "feature", "name available default enabled generation".split(), strings=("name",))
        require(feature["name"] == name and all(feature[key] in (0, 1) for key in ("available", "default", "enabled")), "exact feature row")
        require(feature["available"] or not (feature["default"] or feature["enabled"]), "unavailable feature cannot be enabled")
    for key in SUMMARY_KEYS:
        if key.endswith("_pages") and key[:-6] + "_bytes" in summary:
            require(summary[key[:-6] + "_bytes"] == summary[key] * PAGE <= U64, "summary page/byte identity: " + key)
    require(summary["total_pages"] == summary["free_pages"] + summary["used_pages"] == summary["identity_pages"], "closed physical identity")
    require(summary["identity_mismatch_pages"] == summary["untracked_unreclaimable_pages"] == 0, "no unaccounted physical pages")
    require(summary["identity_pages"] == sum(summary[key] for key in ("free_pages", "owner_pages", "allocator_metadata_pages",
            "zone_descriptor_pages", "per_cpu_page_cache_reserve_pages")), "actual balance component identity")
    require(summary["owner_pages"] == sum(owners[name]["pages"] for name in OWNER_META), "complete physical owner sum")
    require(summary["firmware_reserve_pages"] == sum(owners[name]["pages"] for name in RESERVE_META), "complete firmware reserve sum")
    require(summary["physical_address_pages"] == summary["total_pages"] + summary["firmware_reserve_pages"] ==
            summary["physical_address_identity_pages"] <= U64, "physical address identity")
    for field, name in (("allocator_metadata_pages", "physical_allocator_embedded_metadata"),
                        ("zone_descriptor_pages", "physical_zone_descriptors"),
                        ("per_cpu_page_cache_reserve_pages", "per_cpu_physical_page_cache_metadata")):
        require(summary[field] == owners[name]["pages"], "fixed reserve summary join")
    require(owners["physical_zone_descriptors"]["objects"] == owners["physical_zone_descriptors"]["pages"], "zone descriptor objects")
    require(summary["logical_allocator_bytes"] == sum(summary[key] for key in
            ("logical_kmalloc_bytes", "logical_kmalloc_debug_pool_bytes", "logical_mini_slab_bytes")), "logical allocator identity")
    return summary, owners


def zones_snapshot(raw):
    zones = []
    active = None
    last_order = -1
    for line in raw_lines(raw):
        if line.startswith("zone "):
            row = record(line, "zone", ZONE_KEYS, strings=("name",), hexes=("start",))
            require(row["id"] not in {zone["id"] for zone in zones}, "unique zone ID")
            require(row["start"] % PAGE == row["len_bytes"] % PAGE == 0 and row["len_bytes"] > 0 and
                    row["start"] + row["len_bytes"] <= U64 and row["page_count"] == row["len_bytes"] // PAGE, "bounded zone geometry")
            require(row["has_allocator"] in (0, 1) and row["invalid_allocator"] == row["bad_order"] == row["free_count_mismatch"] == 0,
                    "no allocator/metadata error")
            row["buddy_pages"] = 0
            zones.append(row)
            active = row
            last_order = -1
        else:
            require(active is not None and active["has_allocator"] == 1, "buddy follows allocated zone")
            buddy = record(line, "buddy", "zone order free_blocks free_pages free_bytes".split())
            require(buddy["zone"] == active["id"] and last_order < buddy["order"] <= 20 and buddy["free_blocks"] > 0, "ordered nonempty buddy bins")
            require(buddy["free_pages"] == buddy["free_blocks"] * (1 << buddy["order"]) and
                    buddy["free_bytes"] == buddy["free_pages"] * PAGE <= U64, "buddy block/page/byte identity")
            active["buddy_pages"] += buddy["free_pages"]
            last_order = buddy["order"]
    require(0 < len(zones) < 32, "complete untruncated zone inventory")
    for row in zones:
        if row["has_allocator"]:
            require(row["metadata_pages"] < row["total_pages"] <= row["page_count"] and
                    row["usable_pages"] == row["total_pages"] - row["metadata_pages"] and row["free_pages"] <= row["usable_pages"],
                    "allocator zone extents")
            require(row["buddy_pages"] + row["cached_order0_pages"] == row["scanned_free_pages"] == row["free_pages"],
                    "buddy plus actual cached free identity")
        else:
            require(all(row[key] == 0 for key in ("total_pages", "usable_pages", "free_pages", "metadata_pages", "scanned_free_pages",
                    "cached_order0_pages", "buddy_pages")), "unallocated zone has no allocator counts")
    return zones


class Stream:
    def __init__(self, raw):
        self.raw = raw
        self.cursor = 0

    def line(self, cap=2048):
        end = self.raw.find(b"\n", self.cursor)
        require(end >= self.cursor and end - self.cursor < cap, "bounded complete record line")
        line = self.raw[self.cursor:end].decode("ascii", "strict")
        self.cursor = end + 1
        return line

    def take(self, kind, keys, nonce):
        line = self.line()
        require(line.startswith("NUMA_PRESSURE "), "pressure JSON record prefix")
        row = json.loads(line[len("NUMA_PRESSURE "):], object_pairs_hook=unique_object,
                         parse_float=invalid_number, parse_constant=invalid_number)
        fields = ["type", "nonce", *keys.split()]
        if kind == "begin":
            fields = ["type", "version", "nonce", *keys.split()]
        require(type(row) is dict and list(row) == fields and row["type"] == kind, "exact ordered JSON schema: " + kind)
        for key, value in row.items():
            if key not in ("type", "role", "physical_closure", "metadata_cause"):
                integer(value, key)
        require(row["nonce"] == nonce, "exact run nonce")
        return row


def decode(data, nonce, domains=2):
    require(type(nonce) is int and 0 < nonce <= U64 and type(domains) is int and domains == 2, "bounded expected run identity")
    require(type(data) in (bytes, str), "saved bytes or ASCII text")
    raw = data.encode("ascii", "strict") if type(data) is str else data
    require(0 < len(raw) <= MAX_BYTES and b"\0" not in raw and b"\r" not in raw, "bounded exact saved pressure report")
    stream = Stream(raw)
    begin = stream.take("begin", "domains pid tid timeout_ms fill_ms probe_ms cleanup_ms max_bytes max_entries", nonce)
    require(begin["version"] == 1 and begin["domains"] == 2 and 1 < begin["pid"] == begin["tid"] < (1 << 63), "calling process identity")
    require(60000 <= begin["timeout_ms"] <= 300000 and 1 <= begin["fill_ms"] <= 180000 and 1 <= begin["probe_ms"] <= 5000 and
            (begin["cleanup_ms"], begin["max_bytes"], begin["max_entries"]) == (45000, MAX_PAYLOAD, MAX_ENTRIES), "unchanged explicit budgets")
    nodes = [stream.take("node", "node managed cpu_mask", nonce) for _ in range(2)]
    for index, node in enumerate(nodes):
        require(node["node"] == index and node["managed"] > 0 and node["managed"] % PAGE == 0 and node["cpu_mask"] > 0, "ordered eligible topology")
    online = nodes[0]["cpu_mask"] | nodes[1]["cpu_mask"]
    require(not (nodes[0]["cpu_mask"] & nodes[1]["cpu_mask"]) and online == (1 << online.bit_length()) - 1, "disjoint complete dense CPU mask")
    phases = [stream.take(name, CHECKPOINT_KEYS, nonce) for name in PHASES]
    base = phases[0]
    target = base["target"]
    require(target in (0, 1) and base["cpu"] < 64 and nodes[1 - target]["cpu_mask"] & (1 << base["cpu"]), "opposite-node pressure on fixed actor")
    require(base["as_current"] <= U64 - MAX_PAYLOAD and base["as_current"] + MAX_PAYLOAD <= base["as_soft"] <= base["as_hard"] and
            base["as_peak"] >= base["as_current"] and base["as_denied"] < U64 and base["as_reason"] <= 3, "unchanged sufficient canonical AS headroom")
    for index, phase in enumerate(phases):
        require(phase["stamp"] > 0 and (index == 0 or phases[index - 1]["stamp"] <= phase["stamp"]), "ordered real checkpoint stamps")
        require((phase["cpu"], phase["affinity"], phase["target"], phase["mode"], phase["mask"]) ==
                (base["cpu"], 1 << base["cpu"], target, 1 if 3 <= index <= 5 else 2, 1 << target), "actual policy/CPU/affinity checkpoint")
        require(all(phase[key] == 0 for key in ("usage_rc", "policy_rc", "error")) and
                all(phase[key] == base[key] for key in ("as_denied", "as_reason", "as_soft", "as_hard")), "no accounting/policy/refusal ambiguity")
    entries = []
    while stream.raw[stream.cursor:].startswith(b'NUMA_PRESSURE {"type":"entry",'):
        require(len(entries) < MAX_ENTRIES, "bounded entry ledger")
        entries.append(stream.take("entry", ENTRY_KEYS, nonce))
    final = stream.take("final", FINAL_KEYS, nonce)
    prefix_count = final["prefix_entries"]
    require(1 <= prefix_count <= MAX_ENTRIES - 2 and len(entries) == prefix_count + 2, "entire prefix plus exactly two probes")
    prefix_bytes = 0
    tail = False
    held = []
    for index, entry in enumerate(entries):
        role = "bind" if index < prefix_count else "preferred" if index == prefix_count else "recovery"
        require((entry["index"], entry["role"]) == (index, role), "ordered complete allocation ledger")
        require(entry["bytes"] in ((PAGE, LARGE) if role == "bind" else (PAGE,)), "real order0 request geometry")
        require(entry["address"] >= 0x400000 and entry["address"] % PAGE == 0 and entry["address"] + entry["bytes"] <= 1 << 47,
                "bounded aligned userspace mapping")
        pages = entry["bytes"] // PAGE
        require((entry["node"], entry["resident"], entry["zero_pages"], entry["unavailable"], entry["allocate_rc"], entry["release_rc"],
                 entry["output_changed"], entry["owned"]) == (1 - target if role == "preferred" else target, pages, pages, pages, 0, 0, 1, 0),
                "all-page placement/zero/released/unavailable coverage")
        require(entry["digest"] == FNV_OFFSET * pow(FNV_PRIME, entry["bytes"], 1 << 64) & U64, "complete known-zero byte digest")
        if role == "bind":
            require(not tail or entry["bytes"] == PAGE, "2MiB prefix precedes4KiB tail")
            tail |= entry["bytes"] == PAGE
            prefix_bytes += entry["bytes"]
        if role != "recovery":
            for start, end in held:
                require(entry["address"] + entry["bytes"] <= start or end <= entry["address"], "simultaneously owned mappings disjoint")
            held.append((entry["address"], entry["address"] + entry["bytes"]))
    require(prefix_bytes + 2 * PAGE <= MAX_PAYLOAD, "reserved two-probe byte headroom")
    owned = (0, prefix_bytes, prefix_bytes, prefix_bytes + PAGE, prefix_bytes + PAGE, 0, PAGE)
    high = 0
    for phase, amount in zip(phases, owned):
        high = max(high, amount)
        require(phase["owned_bytes"] == amount and phase["as_current"] == base["as_current"] + amount and
                phase["as_peak"] == max(base["as_peak"], base["as_current"] + high), "exact outstanding AS and monotonic peak")
    require(tuple(final[key] for key in ("pass", "first_error", "cleanup_error", "large_rc", "large_sentinel", "strict_rc", "strict_sentinel",
            "preferred_rc", "recovery_rc", "policy_restore", "affinity_restore", "final_usage_rc", "final_checkpoint_error")) ==
            (1, 0, 0, -12, 1, -12, 1, 0, 0, 0, 0, 0, 0), "genuine raw refusals and complete recovery/cleanup")
    require((final["attempted"], final["released"], final["outstanding"], final["high_bytes"]) ==
            (len(entries) + 2, len(entries), 0, prefix_bytes + PAGE), "exact one-attempt probes and released ledger")
    mode, mask = final["original_mode"], final["original_mask"]
    require(mode in (0, 1, 2, 3) and (mask == 0 if mode == 0 else mask in (1, 2) if mode == 1 else 0 < mask <= 3), "original valid policy")
    require(0 < final["original_affinity"] <= online and not final["original_affinity"] & ~online, "original allowed affinity mask")
    require((final["final_as_current"], final["final_as_peak"], final["final_as_denied"]) ==
            (base["as_current"], max(base["as_peak"], base["as_current"] + prefix_bytes + PAGE), base["as_denied"]), "post-recovery AS baseline restored")
    require(final["physical_closure"] == "OPEN" and final["metadata_cause"] == "UNPROVED", "honest remaining physical/cause scope")
    snapshots = []
    for capture, (phase, kind) in enumerate((("before", "summary"), ("before", "zones"), ("after", "summary"), ("after", "zones")), 1):
        header = record(stream.line(), "NUMA_PRESSURE_RAW", "version nonce phase kind bytes fnv1a64 stamp capture".split(), strings=("phase", "kind"))
        require((header["version"], header["nonce"], header["phase"], header["kind"], header["capture"]) == (1, nonce, phase, kind, capture),
                "exact fresh descriptor frame identity/order")
        require(0 < header["bytes"] < SNAPSHOT_BYTES and 0 < header["stamp"] <= U64, "bounded raw extent/stamp")
        end = stream.cursor + header["bytes"]
        require(end <= len(raw), "raw section not truncated")
        body = raw[stream.cursor:end]
        stream.cursor = end
        require(fnv1a64(body) == header["fnv1a64"], "exact raw length/content FNV")
        ending = record(stream.line(), "NUMA_PRESSURE_RAW_END", "nonce capture".split())
        require(ending == {"nonce": nonce, "capture": capture}, "exact raw ending")
        parsed = summary_snapshot(body) if kind == "summary" else zones_snapshot(body)
        snapshots.append((header, parsed, body))
    require(stream.cursor == len(raw), "no extra/trailing records")
    stamps = [row[0]["stamp"] for row in snapshots]
    require(0 < stamps[0] <= stamps[1] <= phases[0]["stamp"] <= phases[-1]["stamp"] <= stamps[2] <= stamps[3] and
            stamps[1] < stamps[2], "snapshot boundary chronology; contents alone do not prove freshness")
    require(stamps[3] - stamps[0] <= begin["timeout_ms"] * 1000000 and
            phases[2]["stamp"] - phases[0]["stamp"] <= begin["fill_ms"] * 1000000, "observed interval within configured caps")
    before, before_owners = snapshots[0][1]
    after, after_owners = snapshots[2][1]
    for name in (*FIXED_META, *RESERVE_META):
        require(before_owners[name] == after_owners[name], "unchanged fixed reserve identity: " + name)
    for key in ("total_pages", "allocator_metadata_pages", "zone_descriptor_pages", "per_cpu_page_cache_reserve_pages", "firmware_reserve_pages"):
        require(before[key] == after[key], "immutable capacity/reserve total")
    before_zones, after_zones = snapshots[1][1], snapshots[3][1]
    require([{key: zone[key] for key in ("id", "name", "start", "len_bytes", "page_count", "total_pages", "usable_pages", "metadata_pages", "has_allocator")}
             for zone in before_zones] ==
            [{key: zone[key] for key in ("id", "name", "start", "len_bytes", "page_count", "total_pages", "usable_pages", "metadata_pages", "has_allocator")}
             for zone in after_zones], "same complete immutable zone inventory")
    return dict(qualification="PASS_SAVED_ONLY_PUBLIC_PRESSURE_PROTOCOL", runtime_qualified=False, physical_exhaustion_qualified=False,
                physical_closure="OPEN", metadata_cause="UNPROVED", snapshot_freshness_qualified=False, physical_performance_qualified=False,
                nonce=nonce, domains=2, entries=len(entries), prefix_bytes=prefix_bytes, recovered_selected_node=target,
                free_pages_delta=after["free_pages"] - before["free_pages"], owner_pages_delta=after["owner_pages"] - before["owner_pages"],
                sha256=hashlib.sha256(raw).hexdigest())


def _test_summary():
    """Synthetic wire fixture only: these numbers never describe a running WOS."""
    summary = dict.fromkeys(SUMMARY_KEYS, 0)
    summary.update(schema=2, total_pages=1000, free_pages=994, used_pages=6, owner_pages=2, allocator_metadata_pages=2,
                   zone_descriptor_pages=1, per_cpu_page_cache_reserve_pages=1, identity_pages=1000,
                   physical_address_pages=1000, physical_address_identity_pages=1000)
    for key in SUMMARY_KEYS:
        if key.endswith("_pages") and key[:-6] + "_bytes" in summary:
            summary[key[:-6] + "_bytes"] = summary[key] * PAGE
    lines = ["summary " + " ".join(key + "=" + str(summary[key]) for key in SUMMARY_KEYS),
             "anonymous_swap " + " ".join(key + "=0" for key in SWAP_KEYS)]
    for name, values in {**OWNER_META, **FIXED_META, **RESERVE_META}.items():
        pages, objects = {"user_private_mapping": (2, 2), "physical_allocator_embedded_metadata": (2, 1),
                          "physical_zone_descriptors": (1, 1), "per_cpu_physical_page_cache_metadata": (1, 2)}.get(name, (0, 0))
        lines.append("physical_owner name=" + name + " pages=" + str(pages) + " bytes=" + str(pages * PAGE) + " objects=" + str(objects) +
                     " lifetime=" + escaped(values[0]) + " reclaimability=" + escaped(values[1]) + " scaling_bound=" + escaped(values[2]))
    lines.extend("feature name=" + name + " available=0 default=0 enabled=0 generation=0" for name in ("page_callers", "kmalloc_debug"))
    return ("\n".join(lines) + "\n").encode("ascii")


def _test_zones():
    values = dict(zip(ZONE_KEYS, (0, "normal", "0x0000000000001000", 999 * PAGE, 999, 998, 996, 994, 2, 994, 0, 1, 0, 0, 0)))
    lines = ["zone " + " ".join(key + "=" + str(values[key]) for key in ZONE_KEYS)]
    for order in (1, 5, 6, 7, 8, 9):
        pages = 1 << order
        lines.append("buddy zone=0 order=" + str(order) + " free_blocks=1 free_pages=" + str(pages) + " free_bytes=" + str(pages * PAGE))
    return ("\n".join(lines) + "\n").encode("ascii")


def _test_parts():
    nonce = 40220234
    rows = [dict(type="begin", version=1, nonce=nonce, domains=2, pid=106, tid=106, timeout_ms=300000, fill_ms=180000,
                 probe_ms=5000, cleanup_ms=45000, max_bytes=MAX_PAYLOAD, max_entries=MAX_ENTRIES),
            dict(type="node", nonce=nonce, node=0, managed=2 << 30, cpu_mask=1),
            dict(type="node", nonce=nonce, node=1, managed=2 << 30, cpu_mask=2)]
    prefix = LARGE + PAGE
    high = 0
    for index, (phase, owned) in enumerate(zip(PHASES, (0, prefix, prefix, prefix + PAGE, prefix + PAGE, 0, PAGE))):
        high = max(high, owned)
        values = dict(cpu=0, affinity=1, mode=1 if 3 <= index <= 5 else 2, mask=2, target=1, owned_bytes=owned,
                      as_current=PAGE + owned, as_peak=PAGE + high, as_denied=0, as_reason=0, as_soft=U64, as_hard=U64,
                      usage_rc=0, policy_rc=0, error=0, stamp=100 + 10 * index)
        rows.append(dict(type=phase, nonce=nonce, **values))
    for index, (role, address, size, node) in enumerate((("bind", 0x40000000, LARGE, 1), ("bind", 0x50000000, PAGE, 1),
                                                       ("preferred", 0x60000000, PAGE, 0), ("recovery", 0x40000000, PAGE, 1))):
        rows.append(dict(type="entry", nonce=nonce, index=index, role=role, address=address, bytes=size, node=node,
                         resident=size // PAGE, zero_pages=size // PAGE, unavailable=size // PAGE,
                         digest=FNV_OFFSET * pow(FNV_PRIME, size, 1 << 64) & U64, allocate_rc=0, release_rc=0, output_changed=1, owned=0))
    final = dict(type="final", nonce=nonce)
    final.update(dict.fromkeys(FINAL_KEYS.split(), 0))
    final.update({"pass": 1, "large_rc": -12, "large_sentinel": 1, "strict_rc": -12, "strict_sentinel": 1,
                  "attempted": 6, "released": 4, "high_bytes": prefix + PAGE, "original_affinity": 3, "prefix_entries": 2,
                  "physical_closure": "OPEN", "metadata_cause": "UNPROVED", "final_as_current": PAGE, "final_as_peak": PAGE + high})
    rows.append(final)
    frames = [dict(phase=phase, kind=kind, body=_test_summary() if kind == "summary" else _test_zones(), stamp=stamp)
              for phase, kind, stamp in (("before", "summary", 80), ("before", "zones", 90), ("after", "summary", 200), ("after", "zones", 210))]
    return nonce, rows, frames


def _test_encode(nonce, rows, frames):
    raw = b"".join(("NUMA_PRESSURE " + json.dumps(row, separators=(",", ":")) + "\n").encode("ascii") for row in rows)
    for capture, frame in enumerate(frames, 1):
        body = frame["body"]
        raw += ("NUMA_PRESSURE_RAW version=1 nonce=" + str(nonce) + " phase=" + frame["phase"] + " kind=" + frame["kind"] +
                " bytes=" + str(len(body)) + " fnv1a64=" + str(fnv1a64(body)) + " stamp=" + str(frame["stamp"]) +
                " capture=" + str(capture) + "\n").encode("ascii")
        raw += body + ("NUMA_PRESSURE_RAW_END nonce=" + str(nonce) + " capture=" + str(capture) + "\n").encode("ascii")
    return raw


def selftest():
    """Finite saved-byte controls; no child, operational import, guest or runtime input."""
    nonce, original_rows, original_frames = _test_parts()
    baseline = _test_encode(nonce, original_rows, original_frames)
    checks = []
    result = decode(baseline, nonce)
    require(not result["runtime_qualified"] and not result["snapshot_freshness_qualified"] and result["physical_closure"] == "OPEN",
            "saved fixture cannot grant runtime/closure authority")
    checks.append(dict(name="complete synthetic protocol, legal recovery VA reuse and identical raw contents", **{"pass": True}))

    def refuse(name, mutate=None, transform=None, message=None):
        rows, frames = copy.deepcopy(original_rows), copy.deepcopy(original_frames)
        if mutate is not None:
            mutate(rows, frames)
        raw = _test_encode(nonce, rows, frames)
        if transform is not None:
            raw = transform(raw)
        try:
            decode(raw, nonce)
        except (ValueError, UnicodeError) as error:
            require(message is None or message in str(error), name + " refused for the wrong reason: " + str(error))
        else:
            raise AssertionError("accepted malformed saved protocol: " + name)
        checks.append(dict(name=name, **{"pass": True}))

    def row_change(kind, key, value):
        return lambda rows, frames: next(row for row in rows if row["type"] == kind).__setitem__(key, value)

    refuse("wrong nonce", row_change("begin", "nonce", nonce + 1), message="nonce")
    refuse("bool is not an integer", row_change("begin", "pid", True), message="integer")
    refuse("unknown JSON field", row_change("begin", "unknown", 0), message="schema")
    refuse("duplicate JSON key", transform=lambda raw: raw.replace(b'"version":1,', b'"version":1,"version":1,', 1), message="duplicate")
    refuse("float JSON number", transform=lambda raw: raw.replace(b'"pid":106,', b'"pid":106.0,', 1), message="noninteger")
    refuse("NaN JSON number", transform=lambda raw: raw.replace(b'"pid":106,', b'"pid":NaN,', 1), message="noninteger")
    refuse("whole deadline widened", row_change("begin", "timeout_ms", 300001), message="budgets")
    refuse("cleanup reserve changed", row_change("begin", "cleanup_ms", 44999), message="budgets")
    refuse("missing phase", lambda rows, frames: rows.pop(5), message="schema")
    refuse("wrong phase order", lambda rows, frames: rows.__setitem__(slice(4, 6), list(reversed(rows[4:6]))), message="schema")
    refuse("quota denial cannot masquerade as physical refusal", row_change("strict-refusal", "as_denied", 1), message="ambiguity")
    refuse("AS charge mismatch", row_change("preferred", "as_current", PAGE), message="AS")
    refuse("recovery AS charge omitted", row_change("recovery", "owned_bytes", 0), message="AS")
    refuse("final AS not restored", row_change("final", "final_as_current", 2 * PAGE), message="baseline")
    refuse("foreign strict BIND entry", lambda rows, frames: rows[10].__setitem__("node", 0), message="coverage")
    refuse("preferred stays on selected node", lambda rows, frames: rows[12].__setitem__("node", 1), message="coverage")
    refuse("foreign recovery", lambda rows, frames: rows[13].__setitem__("node", 0), message="coverage")
    refuse("zero payload digest mismatch", lambda rows, frames: rows[10].__setitem__("digest", 0), message="digest")
    refuse("resident coverage omitted", lambda rows, frames: rows[10].__setitem__("resident", 1), message="coverage")
    refuse("unavailable cleanup coverage omitted", lambda rows, frames: rows[10].__setitem__("unavailable", 0), message="coverage")
    refuse("allocation still owned", lambda rows, frames: rows[10].__setitem__("owned", 1), message="coverage")
    refuse("owned preferred overlaps prefix", lambda rows, frames: rows[12].__setitem__("address", rows[10]["address"]), message="disjoint")
    refuse("missing entry", lambda rows, frames: rows.pop(11), message="prefix")
    refuse("duplicate entry index", lambda rows, frames: rows[11].__setitem__("index", 0), message="ledger")
    refuse("failed-output sentinel changed", row_change("final", "strict_sentinel", 0), message="refusals")
    refuse("metadata ambiguity is not a success", row_change("final", "first_error", -12), message="refusals")
    refuse("failed recovery is not a success", row_change("final", "recovery_rc", -12), message="refusals")
    refuse("hidden extra attempt", row_change("final", "attempted", 7), message="one-attempt")
    refuse("claiming physical closure", row_change("final", "physical_closure", "PASS"), message="scope")
    refuse("claiming physical exhaustion cause", row_change("final", "metadata_cause", "EXHAUSTED"), message="scope")
    refuse("stale frame phase", lambda rows, frames: frames[2].__setitem__("phase", "before"), message="identity/order")
    refuse("stale snapshot timestamp", lambda rows, frames: frames[2].__setitem__("stamp", 80), message="chronology")
    refuse("missing raw ending", transform=lambda raw: raw.rsplit(b"NUMA_PRESSURE_RAW_END", 1)[0], message="line")
    refuse("extra record", transform=lambda raw: raw + b"extra\n", message="trailing")
    refuse("raw digest mismatch", transform=lambda raw: raw.replace(b"summary schema=2", b"summary schema=3", 1), message="FNV")
    refuse("raw byte count mismatch", transform=lambda raw: raw.replace(b"bytes=", b"bytes=0", 1), message="decimal")
    refuse("summary identity mismatch", lambda rows, frames: frames[2].__setitem__("body", frames[2]["body"].replace(b"identity_pages=1000 ", b"identity_pages=999 ", 1)), message="identity")
    refuse("unknown physical owner", lambda rows, frames: frames[2].__setitem__("body", frames[2]["body"].replace(b"name=user_private_mapping ", b"name=unknown_mapping ", 1)), message="descriptor")
    refuse("fixed reserve object drift", lambda rows, frames: frames[2].__setitem__("body", frames[2]["body"].replace(b"name=per_cpu_physical_page_cache_metadata pages=1 bytes=4096 objects=2 ", b"name=per_cpu_physical_page_cache_metadata pages=1 bytes=4096 objects=3 ", 1)), message="reserve identity")
    refuse("zone allocator error", lambda rows, frames: frames[3].__setitem__("body", frames[3]["body"].replace(b"bad_order=0", b"bad_order=1", 1)), message="allocator/metadata")
    refuse("buddy cached reconciliation mismatch", lambda rows, frames: frames[3].__setitem__("body", frames[3]["body"].replace(b"cached_order0_pages=0", b"cached_order0_pages=1", 1)), message="cached free")
    refuse("zone truncation", lambda rows, frames: frames[3].__setitem__("body", frames[3]["body"] + b"truncated file=zones max_rows=32\n"), message="raw record")
    refuse("missing complete feature row", lambda rows, frames: frames[2].__setitem__("body", frames[2]["body"].rsplit(b"feature name=kmalloc_debug", 1)[0]), message="inventory")
    return dict(result="PASS_PURE_SAVED_BYTE_DECODER_CONTROLS", checks=checks, count=len(checks), runtime_qualified=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--nonce", type=int, required=True)
    parser.add_argument("--domains", type=int, choices=(2,), required=True)
    args = parser.parse_args()
    before = args.log.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= MAX_BYTES, "bounded regular saved report")
    raw = args.log.read_bytes()
    after = args.log.lstat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and len(raw) == before.st_size, "saved report changed")
    print(json.dumps(decode(raw, args.nonce, args.domains), sort_keys=True))


if __name__ == "__main__":
    main()
