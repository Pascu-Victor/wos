#!/usr/bin/env python3
"""Compile complete actual XFS allocator bodies with controlled tree/capture seams.

Production record, cursor, mount, transaction and AGI types and CRC32C are real.
Tree traversal/mutation, capture, RAM buffer ownership, transaction destruction
and chunk growth are named Host seams. This does not exercise real undo/cancel,
WAL, device I/O, mounted creation, durability or the finite EUCLEAN failure.
--ialloc-source accepts a complete predecessor, which must compile and fail the
same leading-hole correctness assertion before any optional new-helper checks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
KERNEL = ROOT / "modules/kern/src"
SOURCE = KERNEL / "vfs/fs/xfs/xfs_ialloc.cpp"
CRC = KERNEL / "util/crc32c.cpp"

LOCKS = r'''
#pragma once
#include <cassert>
#include <cstdint>
namespace ker::mod::sys {
class Spinlock {
    bool held_{};
public:
    void lock() { assert(!held_); held_ = true; }
    auto try_lock() -> bool { if (held_) return false; lock(); return true; }
    void unlock() { assert(held_); held_ = false; }
    auto lock_irqsave() -> uint64_t { lock(); return 0; }
    void unlock_irqrestore(uint64_t) { unlock(); }
};
class Mutex : public Spinlock {};
class MutexGuard {
    Mutex& mutex_;
public:
    explicit MutexGuard(Mutex& mutex) : mutex_(mutex) { mutex_.lock(); }
    ~MutexGuard() { mutex_.unlock(); }
};
}
'''

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <utility>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/fs/xfs/xfs_ialloc.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>

namespace fixture {
using namespace ker::vfs;
using namespace ker::vfs::xfs;
using Record = XfsInobtTraits::IRec;
constexpr uint64_t ALL_FREE = UINT64_MAX;
constexpr xfs_agino_t START = 128;
constexpr uint32_t AGINO_LOG = 13;
constexpr uint32_t AG_BLOCK_LOG = 10;
constexpr uint32_t SECTOR_SIZE = 512;
constexpr uint32_t BLOCK_SIZE = 4096;
constexpr size_t MAX_EVENTS = 128;
const char* current_case = "initialization";
uint64_t case_count{}, assertions{}, helper_calls{}, allocator_calls{};

[[noreturn]] void failed(const char* expression, int line) {
    std::fprintf(stderr, "ASSERT case=%s line=%d expression=%s\n", current_case, line, expression);
    std::fflush(nullptr);
    std::exit(3);
}
void check(bool result, const char* expression, int line) {
    ++assertions;
    if (!result) failed(expression, line);
}
#define CHECK(expression) fixture::check(bool(expression), #expression, __LINE__)

// Independent per-slot oracle: no native mask, ctz or contains calls.
auto oracle_mask(const Record& rec) -> uint64_t {
    uint64_t result = 0;
    for (unsigned slot = 0; slot < 64; ++slot) {
        bool const present = !rec.sparse_format || ((rec.holemask >> (slot / 4)) & 1U) == 0;
        if (present && ((rec.free_mask >> slot) & 1ULL) != 0) result |= 1ULL << slot;
    }
    return result;
}
auto oracle_first(const Record& rec) -> int {
    for (unsigned slot = 0; slot < 64; ++slot) {
        bool const present = !rec.sparse_format || ((rec.holemask >> (slot / 4)) & 1U) == 0;
        if (present && ((rec.free_mask >> slot) & 1ULL) != 0) return int(slot);
    }
    return -1;
}
auto population(uint64_t bits) -> uint32_t {
    uint32_t result = 0;
    for (unsigned slot = 0; slot < 64; ++slot) result += unsigned((bits >> slot) & 1ULL);
    return result;
}
auto record(uint16_t holes, uint64_t free, bool sparse = true, xfs_agino_t start = START) -> Record {
    Record rec{};
    rec.startino = start;
    rec.holemask = holes;
    rec.free_mask = free;
    rec.sparse_format = sparse;
    Record present = rec;
    present.free_mask = ALL_FREE;
    rec.count = uint8_t(population(oracle_mask(present)));
    rec.freecount = population(oracle_mask(rec));
    return rec;
}
auto same_record(const Record& left, const Record& right) -> bool {
    return left.startino == right.startino && left.holemask == right.holemask && left.count == right.count &&
           left.freecount == right.freecount && left.free_mask == right.free_mask && left.sparse_format == right.sparse_format;
}
auto packed(const Record& rec) -> XfsInobtTraits::Rec {
    XfsInobtTraits::Rec out{};
    XfsInobtTraits::encode_rec(&out, rec);
    return out;
}

enum class Fault { NONE, LOOKUP_IN, INCREMENT, CAPTURE_PERAG, UPDATE_IN, LOOKUP_FI, UPDATE_FI, DELETE_FI, READ_AGI, CAPTURE_AGI };
enum class Op { LOOKUP_IN, GET_IN, INCREMENT, CAPTURE_PERAG, UPDATE_IN, LOOKUP_FI, UPDATE_FI, DELETE_FI,
                READ_AGI, CAPTURE_AGI, LOG_AGI, RELEASE_AGI, GROW };
auto op_name(Op op) -> const char* {
    switch (op) {
        case Op::LOOKUP_IN: return "lookup-inobt";
        case Op::GET_IN: return "get-inobt";
        case Op::INCREMENT: return "increment-inobt";
        case Op::CAPTURE_PERAG: return "capture-perag";
        case Op::UPDATE_IN: return "update-inobt";
        case Op::LOOKUP_FI: return "lookup-finobt";
        case Op::UPDATE_FI: return "update-finobt";
        case Op::DELETE_FI: return "delete-finobt";
        case Op::READ_AGI: return "read-agi";
        case Op::CAPTURE_AGI: return "capture-agi";
        case Op::LOG_AGI: return "log-agi";
        case Op::RELEASE_AGI: return "release-agi";
        case Op::GROW: return "chunk-growth-seam";
    }
    std::abort();
}
struct Event {
    Op op{};
    xfs_agnumber_t ag{};
    uint64_t root{};
    int rc{};
    Record rec{};
};
struct Tree {
    uint64_t root{};
    std::array<XfsInobtTraits::Rec, 4> records{};
    size_t size{};
};
struct Fixture;
Fixture* active{};
struct Fixture {
    XfsMountContext mount{};
    std::array<XfsPerAG, 2> pags{};
    XfsTransaction transaction{};
    std::array<Tree, 2> inobt{}, finobt{};
    std::array<std::array<uint8_t, BLOCK_SIZE>, 2> agi_bytes{};
    std::array<BufHead, 2> agi_buffers{};
    std::array<Event, MAX_EVENTS> events{};
    size_t event_count{};
    Fault fault = Fault::NONE;
    int failure = -EIO;
    bool has_finobt{};
    bool permit_growth{};

    explicit Fixture(const char* name, const Record& rec, bool fin = false, unsigned ag_count = 1) {
        CHECK(active == nullptr && ag_count >= 1 && ag_count <= 2);
        active = this;
        current_case = name;
        ++case_count;
        std::printf("CASE %s\n", name);
        std::fflush(stdout);
        mount.per_ag = pags.data();
        mount.ag_count = ag_count;
        mount.agino_log = AGINO_LOG;
        mount.ag_blk_log = AG_BLOCK_LOG;
        mount.ag_blocks = 1U << AG_BLOCK_LOG;
        mount.block_size = BLOCK_SIZE;
        mount.sect_size = SECTOR_SIZE;
        mount.inode_size = 512;
        mount.inodes_per_block = 8;
        mount.feat_ro_compat = fin ? XFS_SB_FEAT_RO_COMPAT_FINOBT : 0;
        has_finobt = fin;
        transaction.mount = &mount;
        for (unsigned ag = 0; ag < ag_count; ++ag) {
            auto& pag = pags.at(ag);
            pag.agno = ag;
            pag.agi_root = 10;
            pag.agi_level = 1;
            pag.agi_count = rec.count;
            pag.agi_freecount = rec.freecount;
            pag.agi_free_root = fin ? 11 : 0;
            pag.agi_free_level = fin ? 1 : 0;
            inobt.at(ag).root = 10;
            inobt.at(ag).records.at(0) = packed(rec);
            inobt.at(ag).size = 1;
            finobt.at(ag).root = 11;
            finobt.at(ag).records.at(0) = packed(rec);
            finobt.at(ag).size = fin ? 1 : 0;
            agi_bytes.at(ag).fill(0x5a);
            agi_buffers.at(ag).data = agi_bytes.at(ag).data();
            agi_buffers.at(ag).size = BLOCK_SIZE;
            agi_buffers.at(ag).refcount.store(1);
        }
    }
    ~Fixture() {
        CHECK(active == this);
        for (unsigned ag = 0; ag < mount.ag_count; ++ag) CHECK(agi_buffers.at(ag).refcount.load() == 1);
        std::printf("CASE %s PASS events=%zu\n", current_case, event_count);
        active = nullptr;
    }
    void event(Op op, unsigned ag = 0, uint64_t root = 0, int rc = 0, const Record& rec = {}) {
        CHECK(ag < mount.ag_count && event_count < MAX_EVENTS);
        events.at(event_count++) = {op, ag, root, rc, rec};
        std::printf("EVENT %s ag=%u root=%llu rc=%d start=%u holes=%04x count=%u freecount=%u free=%016llx sparse=%u\n",
                    op_name(op), ag, static_cast<unsigned long long>(root), rc, rec.startino, unsigned(rec.holemask),
                    unsigned(rec.count), rec.freecount, static_cast<unsigned long long>(rec.free_mask), unsigned(rec.sparse_format));
    }
    auto count(Op op) const -> size_t {
        size_t result = 0;
        for (size_t i = 0; i < event_count; ++i) if (events.at(i).op == op) ++result;
        return result;
    }
    auto index(Op op) const -> size_t {
        for (size_t i = 0; i < event_count; ++i) if (events.at(i).op == op) return i;
        return MAX_EVENTS;
    }
    auto result(unsigned ag = 0) const -> Record {
        CHECK(inobt.at(ag).size != 0);
        return XfsInobtTraits::decode_rec(&inobt.at(ag).records.at(0));
    }
    auto agi(unsigned ag = 0) const -> const XfsAgi* {
        return reinterpret_cast<const XfsAgi*>(agi_bytes.at(ag).data() + 2 * SECTOR_SIZE);
    }
    auto owns(BufHead* bp) -> unsigned {
        for (unsigned ag = 0; ag < mount.ag_count; ++ag) if (bp == &agi_buffers.at(ag)) return ag;
        CHECK(false);
        return 0;
    }
};
auto context() -> Fixture& { CHECK(active != nullptr); return *active; }
template<class Traits> auto tree(const XfsBtreeCursor<Traits>* cur) -> Tree& {
    auto& f = context();
    CHECK(cur != nullptr && cur->mount == &f.mount && cur->agno < f.mount.ag_count);
    if constexpr (std::is_same_v<Traits, XfsInobtTraits>) return f.inobt.at(cur->agno);
    else {
        static_assert(std::is_same_v<Traits, XfsFinobtTraits>);
        CHECK(f.has_finobt);
        return f.finobt.at(cur->agno);
    }
}
}

namespace ker::vfs::xfs {
// These are controlled call boundaries, not production btree/transaction code.
XfsTransaction::~XfsTransaction() {
    CHECK(buf_undo == nullptr && perag_undo == nullptr && inode_undo == nullptr && item_count == 0 &&
          items == inline_items.data() && !committed && !cancelled);
}
template<class Traits>
auto xfs_btree_lookup(XfsBtreeCursor<Traits>* cur, uint64_t root, uint8_t levels,
                      const typename Traits::IRec& target, XfsBtreeLookup dir) -> int {
    using namespace fixture;
    auto& f = context();
    auto& t = tree(cur);
    bool constexpr FIN = std::is_same_v<Traits, XfsFinobtTraits>;
    CHECK(root == t.root && levels == 1);
    int rc = -ENOENT;
    size_t selected = t.size;
    if (f.fault == (FIN ? Fault::LOOKUP_FI : Fault::LOOKUP_IN)) rc = f.failure;
    else {
        for (size_t i = 0; i < t.size; ++i) {
            auto const rec = Traits::decode_rec(&t.records.at(i));
            if (dir == XfsBtreeLookup::GE && rec.startino >= target.startino) { selected = i; break; }
            if (dir == XfsBtreeLookup::EQ && rec.startino == target.startino) { selected = i; break; }
            if (dir == XfsBtreeLookup::LE && rec.startino <= target.startino) selected = i;
        }
        if (selected != t.size) {
            cur->nlevels = 1;
            cur->level_at(0).ptr = int(selected + 1);
            rc = 0;
        }
    }
    f.event(FIN ? Op::LOOKUP_FI : Op::LOOKUP_IN, cur->agno, root, rc, target);
    return rc;
}
template<class Traits>
auto xfs_btree_get_rec(const XfsBtreeCursor<Traits>* cur) -> typename Traits::IRec {
    using namespace fixture;
    static_assert(std::is_same_v<Traits, XfsInobtTraits>);
    auto& t = tree(cur);
    CHECK(cur->nlevels == 1 && cur->level_at(0).ptr > 0 && size_t(cur->level_at(0).ptr) <= t.size);
    auto rec = Traits::decode_rec(&t.records.at(size_t(cur->level_at(0).ptr - 1)));
    context().event(Op::GET_IN, cur->agno, t.root, 0, rec);
    return rec;
}
template<class Traits>
auto xfs_btree_increment(XfsBtreeCursor<Traits>* cur) -> int {
    using namespace fixture;
    static_assert(std::is_same_v<Traits, XfsInobtTraits>);
    auto& f = context();
    auto& t = tree(cur);
    int rc = 0;
    if (f.fault == Fault::INCREMENT) rc = f.failure;
    else if (size_t(cur->level_at(0).ptr) >= t.size) rc = -ENOENT;
    else ++cur->level_at(0).ptr;
    f.event(Op::INCREMENT, cur->agno, t.root, rc);
    return rc;
}
template<class Traits>
auto xfs_btree_update(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& rec) -> int {
    using namespace fixture;
    auto& f = context();
    auto& t = tree(cur);
    bool constexpr FIN = std::is_same_v<Traits, XfsFinobtTraits>;
    CHECK(tp == &f.transaction && cur->level_at(0).ptr > 0 && size_t(cur->level_at(0).ptr) <= t.size);
    CHECK(f.index(Op::CAPTURE_PERAG) < f.event_count);
    int const rc = f.fault == (FIN ? Fault::UPDATE_FI : Fault::UPDATE_IN) ? f.failure : 0;
    f.event(FIN ? Op::UPDATE_FI : Op::UPDATE_IN, cur->agno, t.root, rc, rec);
    if (rc == 0) Traits::encode_rec(&t.records.at(size_t(cur->level_at(0).ptr - 1)), rec);
    return rc;
}
template<class Traits>
auto xfs_btree_delete(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, uint64_t root, uint8_t levels,
                      uint64_t* new_root, uint8_t* new_levels) -> int {
    using namespace fixture;
    static_assert(std::is_same_v<Traits, XfsFinobtTraits>);
    auto& f = context();
    auto& t = tree(cur);
    CHECK(tp == &f.transaction && root == t.root && levels == 1 && t.size == 1 && cur->level_at(0).ptr == 1);
    CHECK(f.index(Op::CAPTURE_PERAG) < f.event_count);
    int const rc = f.fault == Fault::DELETE_FI ? f.failure : 0;
    auto const rec = Traits::decode_rec(&t.records.at(0));
    f.event(Op::DELETE_FI, cur->agno, root, rc, rec);
    if (rc == 0) { t.size = 0; *new_root = 0; *new_levels = 0; }
    return rc;
}
auto xfs_trans_capture_perag(XfsTransaction* tp, xfs_agnumber_t ag) -> int {
    using namespace fixture;
    auto& f = context();
    CHECK(tp == &f.transaction && tp->mount == &f.mount && ag < f.mount.ag_count);
    CHECK(f.count(Op::UPDATE_IN) == 0 && f.count(Op::UPDATE_FI) == 0 && f.count(Op::DELETE_FI) == 0);
    int const rc = f.fault == Fault::CAPTURE_PERAG ? f.failure : 0;
    if (rc != 0) tp->error = rc;
    f.event(Op::CAPTURE_PERAG, ag, 0, rc);
    return rc;
}
auto xfs_buf_read(XfsMountContext* mount, uint64_t block) -> BufHead* {
    using namespace fixture;
    auto& f = context();
    CHECK(mount == &f.mount && (block & ((1ULL << AG_BLOCK_LOG) - 1)) == 0);
    auto const ag = unsigned(block >> AG_BLOCK_LOG);
    CHECK(ag < f.mount.ag_count && f.count(Op::UPDATE_IN) == 1);
    int const rc = f.fault == Fault::READ_AGI ? f.failure : 0;
    f.event(Op::READ_AGI, ag, block, rc);
    if (rc != 0) return nullptr;
    auto* bp = &f.agi_buffers.at(ag);
    CHECK(bp->refcount.fetch_add(1) == 1);
    return bp;
}
auto xfs_trans_capture_buf(XfsTransaction* tp, BufHead* bp) -> int {
    using namespace fixture;
    auto& f = context();
    CHECK(tp == &f.transaction && bp->refcount.load() == 2);
    auto const ag = f.owns(bp);
    int const rc = f.fault == Fault::CAPTURE_AGI ? f.failure : 0;
    if (rc != 0) tp->error = rc;
    f.event(Op::CAPTURE_AGI, ag, 0, rc);
    return rc;
}
void xfs_trans_log_buf(XfsTransaction* tp, BufHead* bp, uint32_t offset, uint32_t length) {
    using namespace fixture;
    auto& f = context();
    CHECK(tp == &f.transaction && offset == SECTOR_SIZE * 2 && length == sizeof(XfsAgi));
    CHECK(f.index(Op::CAPTURE_AGI) < f.event_count && bp->refcount.load() == 2);
    f.event(Op::LOG_AGI, f.owns(bp), offset, 0);
}
}
namespace ker::vfs {
void brelse(BufHead* bp) {
    using namespace fixture;
    auto& f = context();
    CHECK(bp != nullptr && bp->refcount.fetch_sub(1) == 2);
    f.event(Op::RELEASE_AGI, f.owns(bp));
}
}

namespace ker::vfs::xfs {
// Chunk growth is a controlled boundary, distinct from actual allocation body.
auto allocate_inode_chunk(XfsMountContext* mount, XfsTransaction* tp) -> xfs_ino_t {
    auto& f = fixture::context();
    CHECK(mount == &f.mount && tp == &f.transaction && f.permit_growth && tp->error == 0);
    f.event(fixture::Op::GROW);
    return NULLFSINO;
}
}
'''

TESTS = r'''
namespace fixture {
using namespace ker::vfs::xfs;
auto allocate(Fixture& f, bool public_entry = false) -> xfs_ino_t {
    ++allocator_calls;
    return public_entry ? xfs_ialloc(&f.mount, &f.transaction, 0100644) : ialloc_ag(&f.mount, &f.transaction, 0);
}
void success(const char* name, const Record& original, bool fin = false) {
    Fixture f(name, original, fin);
    Record const stored = f.result();
    int const expected = oracle_first(stored);
    CHECK(expected >= 0 && stored.freecount > 0);
    uint64_t const ino = allocate(f);
    // Keep this exact first selection assertion reachable for predecessor runs.
    CHECK(ino == uint64_t(stored.startino) + unsigned(expected));
    CHECK(!stored.sparse_format || ((stored.holemask >> (unsigned(expected) / 4)) & 1U) == 0);
    CHECK(f.transaction.error == 0);
    Record const updated = f.result();
    CHECK(updated.startino == stored.startino && updated.holemask == stored.holemask && updated.count == stored.count &&
          updated.sparse_format == stored.sparse_format);
    CHECK(updated.free_mask == (stored.free_mask & ~(1ULL << unsigned(expected))));
    CHECK(updated.freecount == stored.freecount - 1 && population(oracle_mask(updated)) == updated.freecount);
    CHECK(f.pags.at(0).agi_freecount == stored.freecount - 1 && f.pags.at(0).agi_count == stored.count);
    CHECK(f.pags.at(0).ialloc_hint_valid);
    CHECK(f.pags.at(0).ialloc_hint_startino == (updated.freecount ? stored.startino : stored.startino + 64));
    CHECK(f.count(Op::CAPTURE_PERAG) == 1 && f.count(Op::UPDATE_IN) == 1 && f.count(Op::LOG_AGI) == 1);
    CHECK(f.index(Op::CAPTURE_PERAG) < f.index(Op::UPDATE_IN) && f.index(Op::READ_AGI) < f.index(Op::CAPTURE_AGI) &&
          f.index(Op::CAPTURE_AGI) < f.index(Op::LOG_AGI) && f.index(Op::LOG_AGI) < f.index(Op::RELEASE_AGI));
    CHECK(f.count(Op::GROW) == 0);
    if (fin) {
        CHECK(f.count(Op::LOOKUP_FI) == 1 && f.index(Op::UPDATE_IN) < f.index(Op::LOOKUP_FI));
        if (updated.freecount == 0) {
            CHECK(f.count(Op::DELETE_FI) == 1 && f.count(Op::UPDATE_FI) == 0 && f.finobt.at(0).size == 0);
            CHECK(f.pags.at(0).agi_free_root == 0 && f.pags.at(0).agi_free_level == 0);
        } else {
            CHECK(f.count(Op::UPDATE_FI) == 1 && f.count(Op::DELETE_FI) == 0 && f.finobt.at(0).size == 1);
            CHECK(same_record(XfsFinobtTraits::decode_rec(&f.finobt.at(0).records.at(0)), updated));
        }
    } else CHECK(f.count(Op::LOOKUP_FI) == 0 && f.count(Op::UPDATE_FI) == 0 && f.count(Op::DELETE_FI) == 0);
    auto const* agi = f.agi();
    CHECK(agi->agi_count.to_cpu() == f.pags.at(0).agi_count && agi->agi_freecount.to_cpu() == f.pags.at(0).agi_freecount);
    CHECK(agi->agi_root.to_cpu() == f.pags.at(0).agi_root && agi->agi_level.to_cpu() == f.pags.at(0).agi_level);
    CHECK(agi->agi_free_root.to_cpu() == f.pags.at(0).agi_free_root && agi->agi_free_level.to_cpu() == f.pags.at(0).agi_free_level);
    CHECK(agi->agi_newino.to_cpu() == ino && agi->agi_lsn.to_cpu() == 0);
    uint32_t crc = 0;
    std::memcpy(&crc, &agi->agi_crc, sizeof(crc));
    CHECK(crc == ker::util::crc32c_block_with_cksum(agi, SECTOR_SIZE, XFS_AGI_CRC_OFF));
    // Actual allocation-state consumer agrees with selected present inode.
    CHECK(xfs_inode_allocated(&f.mount, ino) == 1);
    if (stored.sparse_format && stored.holemask != 0) {
        for (unsigned bit = 0; bit < 64; ++bit) if (((stored.holemask >> (bit / 4)) & 1U) != 0) {
            CHECK(xfs_inode_allocated(&f.mount, uint64_t(stored.startino) + bit) == 0);
            break;
        }
    }
}
void invalid(const char* name, Record rec, bool public_entry = false) {
    rec.freecount = 1;
    Fixture f(name, rec, false, public_entry ? 2 : 1);
    auto const records_before = f.inobt;
    auto const agi_before = f.agi_bytes;
    f.pags.at(0).ialloc_hint_valid = true;
    f.pags.at(0).ialloc_hint_startino = rec.startino;
    if (public_entry) {
        Record const good = record(0, ALL_FREE);
        f.inobt.at(1).records.at(0) = packed(good);
        f.pags.at(1).agi_freecount = good.freecount;
    }
    CHECK(oracle_mask(rec) == 0);
    CHECK(allocate(f, public_entry) == NULLFSINO && f.transaction.error == -EIO);
    CHECK(f.count(Op::CAPTURE_PERAG) == 0 && f.count(Op::UPDATE_IN) == 0 && f.count(Op::LOOKUP_FI) == 0 &&
          f.count(Op::DELETE_FI) == 0 && f.count(Op::READ_AGI) == 0 && f.count(Op::CAPTURE_AGI) == 0 && f.count(Op::GROW) == 0);
    CHECK(std::memcmp(f.inobt.at(0).records.data(), records_before.at(0).records.data(), sizeof(f.inobt.at(0).records)) == 0);
    CHECK(f.agi_bytes == agi_before && f.pags.at(0).agi_freecount == 1 && f.pags.at(0).agi_count == rec.count);
    CHECK(f.pags.at(0).ialloc_hint_valid && f.pags.at(0).ialloc_hint_startino == rec.startino);
    if (public_entry) for (size_t i = 0; i < f.event_count; ++i) CHECK(f.events.at(i).ag == 0);
}
void exhausted_and_hint() {
    Record exhausted = record(0x0001, 0xFULL);
    CHECK(exhausted.freecount == 0);
    Record const good = record(0x0001, ALL_FREE, true, START + 64);
    {
        Fixture f("valid-exhausted-sparse-record-is-skipped", exhausted);
        f.inobt.at(0).records.at(1) = packed(good);
        f.inobt.at(0).size = 2;
        f.pags.at(0).agi_freecount = good.freecount;
        CHECK(allocate(f) == good.startino + 4 && f.transaction.error == 0);
        CHECK(f.count(Op::INCREMENT) == 1 && f.count(Op::CAPTURE_PERAG) == 1);
        CHECK(same_record(XfsInobtTraits::decode_rec(&f.inobt.at(0).records.at(0)), exhausted));
        auto const after = XfsInobtTraits::decode_rec(&f.inobt.at(0).records.at(1));
        CHECK(after.freecount == good.freecount - 1 && after.free_mask == (good.free_mask & ~(1ULL << 4)));
    }
    {
        Fixture f("stale-hint-falls-back-to-zero", good);
        f.pags.at(0).ialloc_hint_valid = true;
        f.pags.at(0).ialloc_hint_startino = good.startino + 64;
        CHECK(allocate(f) == good.startino + 4 && f.transaction.error == 0);
        CHECK(f.count(Op::LOOKUP_IN) == 2);
        CHECK(f.events.at(0).rec.startino == good.startino + 64 && f.events.at(1).rec.startino == 0);
    }
    {
        Fixture f("exhausted-public-allocation-reaches-growth-seam", exhausted);
        f.pags.at(0).agi_freecount = 1; // Stale aggregate, not a claimed-free record.
        f.permit_growth = true;
        CHECK(allocate(f, true) == NULLFSINO && f.transaction.error == 0);
        CHECK(f.count(Op::INCREMENT) == 1 && f.count(Op::GROW) == 1 && f.count(Op::CAPTURE_PERAG) == 0);
    }
}
void boundaries() {
    for (auto fault : {Fault::LOOKUP_IN, Fault::CAPTURE_PERAG, Fault::UPDATE_IN, Fault::LOOKUP_FI,
                       Fault::UPDATE_FI, Fault::DELETE_FI, Fault::READ_AGI, Fault::CAPTURE_AGI}) {
        bool const last = fault == Fault::DELETE_FI;
        auto const rec = record(0x0001, last ? (0xFULL | (1ULL << 63)) : ALL_FREE);
        Fixture f("existing-error-boundary-order", rec, true);
        f.fault = fault;
        f.failure = fault == Fault::CAPTURE_PERAG || fault == Fault::CAPTURE_AGI ? -ENOMEM : -EIO;
        auto const bytes_before = f.agi_bytes;
        auto const tree_before = f.inobt.at(0).records;
        CHECK(allocate(f) == NULLFSINO && f.transaction.error == f.failure);
        CHECK(f.count(Op::LOG_AGI) == 0 && f.agi_bytes == bytes_before && f.count(Op::GROW) == 0);
        if (fault == Fault::LOOKUP_IN || fault == Fault::CAPTURE_PERAG || fault == Fault::UPDATE_IN) {
            CHECK(std::memcmp(f.inobt.at(0).records.data(), tree_before.data(), sizeof(tree_before)) == 0);
            CHECK(f.pags.at(0).agi_freecount == rec.freecount && f.count(Op::LOOKUP_FI) == 0);
        }
        if (fault == Fault::LOOKUP_IN) CHECK(f.count(Op::CAPTURE_PERAG) == 0);
        if (fault == Fault::CAPTURE_PERAG) CHECK(f.count(Op::UPDATE_IN) == 0);
        if (fault == Fault::LOOKUP_FI || fault == Fault::UPDATE_FI || fault == Fault::DELETE_FI) {
            CHECK(f.count(Op::UPDATE_IN) == 1 && f.pags.at(0).agi_freecount == rec.freecount);
            CHECK(f.count(Op::READ_AGI) == 0);
        }
        if (fault == Fault::READ_AGI || fault == Fault::CAPTURE_AGI) {
            CHECK(f.pags.at(0).agi_freecount == rec.freecount - 1 && f.count(Op::UPDATE_IN) == 1);
            CHECK(f.count(Op::RELEASE_AGI) == (fault == Fault::CAPTURE_AGI ? 1 : 0));
        }
    }
    {
        auto exhausted = record(1, 0xf);
        Fixture f("existing-increment-error-stops-before-growth", exhausted);
        f.pags.at(0).agi_freecount = 1;
        f.fault = Fault::INCREMENT;
        CHECK(allocate(f, true) == NULLFSINO && f.transaction.error == -EIO);
        CHECK(f.count(Op::CAPTURE_PERAG) == 0 && f.count(Op::GROW) == 0);
    }
}
void helpers() {
    current_case = "dense-first-free-boundaries";
    ++case_count;
    CHECK(first_free_inode_bit(0) == -1);
    for (unsigned slot = 0; slot < 64; ++slot) {
        CHECK(first_free_inode_bit(1ULL << slot) == int(slot));
        CHECK(inobt_free_mask_for_bit(slot) == (1ULL << slot));
    }
    CHECK(inobt_free_mask_for_bit(64) == 0 && inobt_free_mask_for_bit(65) == 0);
#if XFS_SPARSE_SELECTOR_PRESENT
    current_case = "all-65536-sparse-holemasks-independent-slot-oracle";
    ++case_count;
    constexpr std::array<uint64_t, 5> patterns{{ALL_FREE, 0, 0x0123456789abcdefULL, 0x8000000000000001ULL, 0xaaaaaaaaaaaaaaaaULL}};
    for (uint32_t holes = 0; holes <= UINT16_MAX; ++holes) {
        for (uint64_t bits : patterns) {
            Record const rec = record(uint16_t(holes), bits);
            ++helper_calls;
            uint64_t const actual_mask = inobt_present_free_mask(rec);
            CHECK(actual_mask == oracle_mask(rec));
            CHECK(first_free_inode_bit(actual_mask) == oracle_first(rec));
            Record dense = rec;
            dense.sparse_format = false;
            ++helper_calls;
            CHECK(inobt_present_free_mask(dense) == bits);
        }
    }
    std::printf("CASE all-65536-sparse-holemasks-independent-slot-oracle PASS masks=65536 patterns=5 calls=%llu\n",
                static_cast<unsigned long long>(helper_calls));
#else
    CHECK(false); // The predecessor must already fail the actual leading-hole allocation above.
#endif
}
void tests() {
    // FIRST: predecessor lacks the new helper but must compile and fail here.
    success("leading-holes-valid-ALL_FREE-original-defect", record(0x0007, ALL_FREE));
    success("sparse-full-chunk", record(0, ALL_FREE));
    success("highest-present-group", record(0x7fff, ALL_FREE));
    success("noncontiguous-present-groups", record(0xfffa, ALL_FREE & ~(1ULL << 8)));
    success("finobt-updates-present-record", record(1, ALL_FREE), true);
    success("finobt-deletes-last-present-slot63", record(1, 0xFULL | (1ULL << 63)), true);
    for (unsigned slot = 0; slot < 64; ++slot) success("dense-single-bit-compatible", record(0, 1ULL << slot, false));
    for (uint64_t bits : std::array<uint64_t, 4>{ALL_FREE, 0x8000000000000001ULL, 0x0123456789abcdefULL, 0xaaaaaaaaaaaaaaaaULL})
        success("dense-multiple-bits-compatible", record(0, bits, false));
    invalid("positive-freecount-zero-raw-mask", record(0, 0));
    invalid("positive-freecount-holes-only", record(1, 0xf));
    invalid("positive-freecount-all-holes", record(UINT16_MAX, ALL_FREE));
    invalid("public-error-refuses-next-AG-and-growth", record(1, 0xf), true);
    exhausted_and_hint();
    boundaries();
    helpers();
    CHECK(active == nullptr);
    std::printf("PASS cases=%llu assertions=%llu helper_calls=%llu allocator_calls=%llu "
                "actual_allocator_bodies=1 real_filesystem_types=1 actual_agi_crc=1 "
                "tree_capture_seams=1 real_transactions=0 wal=0 device=0 runtime_cause_proven=0\n",
                static_cast<unsigned long long>(case_count), static_cast<unsigned long long>(assertions),
                static_cast<unsigned long long>(helper_calls), static_cast<unsigned long long>(allocator_calls));
}
}
int main() { fixture::tests(); }
'''


def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def definition(source: str, name: str) -> tuple[str, dict]:
    """Capture one complete definition, ignoring comments/strings for braces."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"',
                   lambda match: " " * len(match[0]), source)
    matches = list(re.finditer(rf"^(?:auto|void) {re.escape(name)}\([^{{;]*\{{", clean, re.M))
    if len(matches) != 1:
        raise ValueError(f"expected one complete definition for {name}, got {len(matches)}")
    start = matches[0].start()
    opening = clean.index("{", start)
    depth = 0
    for end in range(opening, len(clean)):
        if clean[end] == "{":
            depth += 1
        elif clean[end] == "}":
            depth -= 1
            if depth == 0:
                body = source[start:end + 1]
                data = body.encode()
                return body, {"name": name, "start_character": start, "end_character_exclusive": end + 1,
                              "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    raise ValueError(f"unterminated definition: {name}")


def generate(source: str) -> tuple[str, list[dict]]:
    names = ["inobt_record_contains", "inobt_free_mask_for_bit", "first_free_inode_bit", "log_agi_state",
             "ialloc_ag", "xfs_ialloc", "xfs_inode_allocated"]
    present = re.search(r"^auto inobt_present_free_mask\(", source, re.M) is not None
    if present:
        names.insert(2, "inobt_present_free_mask")
    captures = [definition(source, name) for name in names]
    constants = []
    for name in ["XFS_INODES_PER_CHUNK", "XFS_INODES_PER_HOLEMASK_BIT"]:
        matches = re.findall(rf"^constexpr uint32_t {name}\s*=.*?;", source, re.M)
        if len(matches) != 1:
            raise ValueError(f"missing/duplicate actual constant {name}")
        constants.extend(matches)
    actual = "namespace ker::vfs::xfs {\n" + "\n".join(constants) + "\n\n"
    actual += "\n\n".join(body for body, _ in captures) + "\n}\n"
    return PRELUDE + f"\n#define XFS_SPARSE_SELECTOR_PRESENT {int(present)}\n" + actual + TESTS, [row for _, row in captures]


def dependency_paths(path: Path) -> list[Path]:
    """Parse GCC/Clang -MD escaping without executing shell text."""
    text = path.read_text().replace("\\\n", "")
    _, separator, values = text.partition(":")
    if not separator:
        raise ValueError("compiler depfile has no target separator")
    tokens, token, escaped = [], [], False
    for char in values:
        if escaped:
            token.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char.isspace():
            if token:
                tokens.append("".join(token))
                token = []
        else:
            token.append(char)
    if escaped:
        raise ValueError("unterminated depfile escape")
    if token:
        tokens.append("".join(token))
    return sorted({Path(token).resolve() for token in tokens})


def command(argv: list[str], log: Path, deadline: float) -> dict:
    # Reserve one second inside the same 60s budget for kill/reap on failure.
    remaining = deadline - time.monotonic() - 1
    if remaining <= 0:
        raise TimeoutError("whole compiled allocator test deadline expired")
    started = time.monotonic()
    def limits() -> None:
        # A predecessor assertion must never create a Host SSD coredump. The
        # per-file cap also bounds unexpected compiler/runtime output growth.
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (16 * 1024 * 1024, 16 * 1024 * 1024))

    # Each command has an owned new session. Kill only that exact Popen group
    # on its deadline/output failure, and reap it before removing its files.
    with log.open("xb") as output:
        process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                                   preexec_fn=limits, env=dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                            LSAN_OPTIONS="detect_leaks=0"))
        try:
            while True:
                if log.stat().st_size > 1024 * 1024:
                    raise ValueError("owned command output exceeded 1 MiB")
                remaining = deadline - time.monotonic() - 1
                if remaining <= 0:
                    raise TimeoutError("whole compiled allocator test deadline expired")
                try:
                    exit_code = process.wait(timeout=min(0.05, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            raise
    if log.stat().st_size > 1024 * 1024:
        raise ValueError("owned completed command output exceeded 1 MiB")
    return {"argv": argv, "exit": exit_code, "elapsed_seconds": time.monotonic() - started,
            "log": pin(log), "rlimit_core": [0, 0], "owned_session_reaped": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ialloc-source", type=Path, default=SOURCE)
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    parser.add_argument("--receipt", type=Path, help="exclusive small receipt; logs/source text included, temporary ELF removed")
    args = parser.parse_args()
    start = time.monotonic()
    deadline = start + 60
    source_path = args.ialloc_source.resolve(strict=True)
    compiler = Path(args.cxx).resolve(strict=True)
    if not compiler.is_file() or not os.access(compiler, os.X_OK):
        raise ValueError("CXX/--cxx must name one executable compiler path")
    source = source_path.read_text()
    harness, definitions = generate(source)
    source_paths = [source_path, Path(__file__).resolve(), CRC, KERNEL / "util/crc32c.hpp",
                    KERNEL / "vfs/fs/xfs/xfs_ialloc.hpp", KERNEL / "vfs/fs/xfs/xfs_btree.hpp",
                    KERNEL / "vfs/fs/xfs/xfs_format.hpp", KERNEL / "vfs/fs/xfs/xfs_mount.hpp",
                    KERNEL / "vfs/fs/xfs/xfs_trans.hpp", KERNEL / "vfs/fs/xfs/xfs_inode.hpp",
                    KERNEL / "vfs/buffer_cache.hpp", KERNEL / "dev/block_device.hpp",
                    KERNEL / "net/endian.hpp", KERNEL / "vfs/native_coherency.hpp", KERNEL / "vfs/coherency.hpp",
                    KERNEL / "vfs/exec_content.hpp", KERNEL / "vfs/stat.hpp",
                    ROOT / "tests/host/kern_shim/platform/dbg/dbg.hpp", compiler]
    before = [pin(path) for path in source_paths]
    errors = []
    receipt = {"qualification": "FAIL", "source_before": before, "definitions": definitions, "commands": [],
               "scope": "actual allocator bodies/filesystem types/AGI CRC; controlled tree/capture/destructor/growth seams",
               "flags": {"compiler_executed": False, "host_processes_executed": False,
                         "actual_btree_transaction_wal_device_executed": False, "runtime_cause_proven": False,
                         "vm_launch_performed": False, "all99_admitted": False, "twohour_runtime_qualified": False,
                         "criterion8_complete": False, "full_goal_complete": False},
               "errors": errors}
    receipt["limits"] = {"whole_deadline_seconds": 60, "command_cleanup_reserve_seconds": 1,
                         "command_output_bytes": 1024 * 1024, "child_file_bytes": 16 * 1024 * 1024,
                         "owned_temporary_allocated_bytes": 64 * 1024 * 1024, "core_bytes": 0}
    if args.receipt is not None and args.receipt.exists():
        raise ValueError("receipt already exists")
    temporary = None
    try:
        with tempfile.TemporaryDirectory(prefix="xfs-sparse-ialloc-", dir=os.environ.get("TMPDIR")) as name:
            temporary = Path(name)
            receipt["owned_temporary_path"] = name
            for relative, content in {
                "platform/sys/locks.hpp": LOCKS,
                "platform/sys/spinlock.hpp": '#pragma once\n#include "locks.hpp"\n',
                "platform/sys/mutex.hpp": '#pragma once\n#include "locks.hpp"\n',
                "bits/off_t.h": "#pragma once\n#include <sys/types.h>\n",
                "platform/dbg/dbg.hpp": (ROOT / "tests/host/kern_shim/platform/dbg/dbg.hpp").read_text(),
            }.items():
                path = temporary / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            cpp, binary, depfile = temporary / "allocator.cpp", temporary / "allocator", temporary / "allocator.d"
            cpp.write_text(harness)
            receipt["generated_translation_unit"] = pin(cpp)
            receipt["generated_source"] = harness
            receipt["generated_headers"] = [pin(path) for path in sorted(temporary.rglob("*.hpp"))]
            # Compile TU and real CRC separately: each has its own complete -MD
            # depfile; no overwritten depfile from a multi-input driver call.
            objects = []
            emitted_dependencies = set()
            for index, input_path in enumerate([cpp, CRC]):
                if sum(path.stat().st_blocks * 512 for path in temporary.rglob("*") if path.is_file()) > 64 * 1024 * 1024:
                    raise ValueError("owned compiler temporary storage exceeded 64 MiB")
                obj = temporary / f"input-{index}.o"
                deps = depfile if index == 0 else temporary / "crc.d"
                argv = [str(compiler), "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                        "-fno-omit-frame-pointer", "-fno-sanitize=vptr", "-I", str(temporary), "-I", str(KERNEL),
                        "-MD", "-MF", str(deps), "-c", str(input_path), "-o", str(obj)]
                receipt["flags"]["compiler_executed"] = True
                receipt["flags"]["host_processes_executed"] = True
                row = command(argv, temporary / f"compile-{index}.log", deadline)
                receipt["commands"].append(row)
                receipt.setdefault("logs", {})[row["log"]["path"]] = Path(row["log"]["path"]).read_text(errors="replace")
                if row["exit"] != 0:
                    errors.append(f"actual compile {index} exit {row['exit']}")
                    print(receipt["logs"][row["log"]["path"]], end="")
                    break
                objects.append(obj)
                emitted_dependencies.update(dependency_paths(deps))
                receipt.setdefault("depfiles", []).append({"pin": pin(deps), "text": deps.read_text()})
            if not errors:
                if sum(path.stat().st_blocks * 512 for path in temporary.rglob("*") if path.is_file()) > 64 * 1024 * 1024:
                    raise ValueError("owned compiler temporary storage exceeded 64 MiB")
                receipt["emitted_dependency_pins_after_compile"] = [pin(path) for path in sorted(emitted_dependencies)]
                # These are observed emitted dependencies after compilation;
                # only source_before subjects have an in-script before sample.
                receipt["all_external_dependencies_before_sampled"] = False
                argv = [str(compiler), "-fsanitize=address,undefined", *(str(obj) for obj in objects), "-o", str(binary)]
                row = command(argv, temporary / "link.log", deadline)
                receipt["commands"].append(row)
                receipt.setdefault("logs", {})[row["log"]["path"]] = Path(row["log"]["path"]).read_text(errors="replace")
                if row["exit"] != 0:
                    errors.append(f"actual link exit {row['exit']}")
                else:
                    if sum(path.stat().st_blocks * 512 for path in temporary.rglob("*") if path.is_file()) > 64 * 1024 * 1024:
                        raise ValueError("owned compiler temporary storage exceeded 64 MiB")
                    receipt["artifact"] = pin(binary)
                    row = command([str(binary)], temporary / "runtime.log", deadline)
                    receipt["commands"].append(row)
                    log_text = Path(row["log"]["path"]).read_text(errors="replace")
                    receipt.setdefault("logs", {})[row["log"]["path"]] = log_text
                    print(log_text, end="")
                    if row["exit"] != 0:
                        errors.append(f"actual allocator assertions exit {row['exit']}")
                    elif not re.search(r"^PASS cases=\d+ assertions=\d+ helper_calls=655360 allocator_calls=\d+ ", log_text, re.M):
                        errors.append("actual complete suite terminal row missing")
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired) as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        receipt["source_after"] = [pin(path) for path in source_paths]
        receipt["selected_sources_held"] = before == receipt["source_after"]
        if not receipt["selected_sources_held"]:
            errors.append("selected source/compiler pins changed")
        receipt["owned_temporary_removed"] = temporary is not None and not temporary.exists()
        if temporary is not None and not receipt["owned_temporary_removed"]:
            errors.append("owned temporary compiler/test artifacts remain")
        receipt["elapsed_seconds"] = time.monotonic() - start
        if not errors:
            receipt["qualification"] = "PASS_ACTUAL_COMPILED_ALLOCATOR_BODIES_REAL_TYPES_AGI_CRC_WITH_CONTROLLED_SEAMS_ONLY"
        if args.receipt is not None:
            with args.receipt.open("x") as output:
                json.dump(receipt, output, indent=2)
                output.write("\n")
        print(json.dumps({key: receipt[key] for key in ["qualification", "selected_sources_held", "owned_temporary_removed", "errors"]}))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
