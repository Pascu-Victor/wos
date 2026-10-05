#!/usr/bin/env python3
"""Source invariants for the latency-sensitive mlibc allocator policy."""

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[3]
ALLOCATOR_HPP = ROOT / "toolchain/src/mlibc/options/internal/include/mlibc/allocator.hpp"
ALLOCATOR_CPP = ROOT / "toolchain/src/mlibc/options/internal/generic/allocator.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def test_first_slab_is_bounded_without_disabling_arena_reuse() -> None:
    header = ALLOCATOR_HPP.read_text()
    implementation = ALLOCATOR_CPP.read_text()

    match = re.search(r"static constexpr size_t slabsize = (\d+) \* 1024;", header)
    if not match:
        fail("VirtualAllocator must specify its latency-sensitive slab size")

    slab_size = int(match.group(1)) * 1024
    if slab_size != 64 * 1024:
        fail(f"first mlibc slab must remain 64 KiB, got {slab_size} bytes")

    bucket_match = re.search(r"static constexpr size_t num_buckets = (\d+);", header)
    if not bucket_match:
        fail("VirtualAllocator must bound frg's slab bucket count")
    bucket_count = int(bucket_match.group(1))
    if bucket_count != 12:
        fail(f"mlibc must keep 12 slab buckets, got {bucket_count}")

    if "static constexpr size_t sb_size = slabsize;" not in header:
        fail("the frg superblock alignment must track the reduced slab size")

    # With 12 buckets frg tops out at 16 KiB. A slab must hold its frame
    # (rounded to one bucket object), the object returned by the allocation that
    # constructed the slab, and at least one spare freelist object. Otherwise
    # slab_pool::allocate() asserts before attaching the new slab to its bucket.
    largest_slab_bucket = 16 * 1024
    if slab_size < 3 * largest_slab_bucket:
        fail("the slab cannot retain a spare object for the largest frg bucket")

    if "constexpr size_t arenaSpanSize = 16 * 1024 * 1024;" not in implementation:
        fail("small allocator mappings must retain the reusable arena backend")
    if "if (uintptr_t arenaSpan = take_arena_span(length))" not in implementation:
        fail("VirtualAllocator::map must continue to use retained arena spans")


if __name__ == "__main__":
    test_first_slab_is_bounded_without_disabling_arena_reuse()
    print("mlibc allocator source invariants hold")
