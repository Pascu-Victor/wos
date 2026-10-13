#!/usr/bin/env python3
"""Compile complete current XFS directory bodies against bounded RAM seams.

Both block add/remove/lookup and leaf prepare/reuse/ensure are extracted intact.
On-disk/endian/mount/inode/transaction/buffer types and CRC implementation are
real. RAM read/release and transaction capture/log/destruction are explicit Host
seams: no real undo/WAL/device/concurrency or captured EUCLEAN attribution.
Optional --dir-source accepts a complete predecessor, and --only block/leaf
allows two independent genuine compiled sortedness negative controls.
The driver needs Linux process groups/subreaping and an ordinary host C++23
compiler. It uses no network, privileged operation, VM or mounted filesystem.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(path for path in Path(__file__).resolve().parents
            if (path / "modules/kern/src").is_dir() and (path / ".github/instructions").is_dir())
KERNEL = ROOT / "modules/kern/src"
SOURCE = KERNEL / "vfs/fs/xfs/xfs_dir2.cpp"
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
#include <compare>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/buffer_cache.hpp>

namespace fixture {
using namespace ker::vfs;
using namespace ker::vfs::xfs;
namespace util = ker::util;
constexpr size_t BS = 4096;
static_assert(sizeof(XfsDir3DataHdr) == 64 && sizeof(XfsDir2LeafEntry) == 8 && sizeof(XfsDir2BlockTail) == 8);
const char* current_case = "initialization";
uint64_t cases{}, assertions{}, block_calls{}, leaf_calls{};
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
enum class Op { READ, CAPTURE, LOG_BUFFER, RELEASE, LOG_INODE, BAD_MAGIC };
struct Ram {
    alignas(8) std::array<uint8_t, BS> bytes{};
    XfsMountContext mount{};
    XfsInode inode{};
    XfsTransaction transaction{};
    BufHead buffer{};
    std::vector<Op> events{};
    std::array<uint8_t, BS> captured{};
    std::array<uint8_t, BS> before_operation{};
    int read_error{}, capture_error{};
    bool has_capture{}, has_log{};
    unsigned retained{}, released{};
    explicit Ram(bool ftype = true);
    ~Ram();
    void reset_events() {
        CHECK(buffer.refcount.load() == 0 && !has_capture && !has_log && retained == released);
        events.clear(); inode.dirty = false; before_operation = bytes;
        retained = released = 0;
    }
    void retire_transaction() {
        // Explicit seam for the two distinct references owned by capture/log.
        // This checks the algorithm's caller reference independently of them.
        CHECK(buffer.refcount.load() == int(has_capture) + int(has_log));
        if (has_log) { CHECK(buffer.refcount.fetch_sub(1) > 0); ++released; has_log = false; }
        if (has_capture) { CHECK(buffer.refcount.fetch_sub(1) > 0); ++released; has_capture = false; }
        CHECK(buffer.refcount.load() == 0 && retained == released);
    }
};
Ram* active{};
}

// Named Host boundaries only; the directory algorithms below are actual code.
namespace ker::vfs {
void brelse(BufHead* bh) {
    CHECK(fixture::active != nullptr && bh == &fixture::active->buffer);
    CHECK(bh->refcount.fetch_sub(1) > 0);
    ++fixture::active->released;
    fixture::active->events.push_back(fixture::Op::RELEASE);
}
}
namespace ker::vfs::xfs {
auto dir2_read_block(XfsInode* dp, xfs_dir2_db_t db, BufHead** bhp) -> int {
    CHECK(fixture::active != nullptr && dp == &fixture::active->inode && db == 0 && bhp != nullptr);
    auto& f = *fixture::active;
    f.events.push_back(fixture::Op::READ);
    *bhp = nullptr;
    if (f.read_error != 0) return f.read_error;
    CHECK(f.buffer.refcount.fetch_add(1) == 0);
    ++f.retained;
    *bhp = &f.buffer;
    return 0;
}
auto xfs_trans_capture_buf(XfsTransaction* tp, BufHead* bh) -> int {
    CHECK(fixture::active != nullptr && tp == &fixture::active->transaction && bh == &fixture::active->buffer);
    auto& f = *fixture::active;
    CHECK(bh->refcount.load() == 1 && !f.has_capture);
    f.events.push_back(fixture::Op::CAPTURE);
    CHECK(f.bytes == f.before_operation); // Capture must precede every byte write.
    f.captured = f.bytes;
    if (f.capture_error != 0) return f.capture_error;
    f.has_capture = true;
    CHECK(bh->refcount.fetch_add(1) == 1); ++f.retained;
    return 0;
}
void xfs_trans_log_buf_full(XfsTransaction* tp, BufHead* bh) {
    CHECK(fixture::active != nullptr && tp == &fixture::active->transaction && bh == &fixture::active->buffer);
    auto& f = *fixture::active;
    CHECK(f.has_capture && !f.has_log && bh->refcount.load() == 2 && f.captured != f.bytes);
    f.events.push_back(fixture::Op::LOG_BUFFER);
    CHECK(bh->refcount.fetch_add(1) == 2); ++f.retained; f.has_log = true;
}
void xfs_trans_log_inode(XfsTransaction* tp, XfsInode* dp) {
    CHECK(fixture::active != nullptr && tp == &fixture::active->transaction && dp == &fixture::active->inode);
    CHECK(dp->dirty && fixture::active->buffer.refcount.load() == 2 && fixture::active->has_capture && fixture::active->has_log);
    fixture::active->events.push_back(fixture::Op::LOG_INODE);
}
void dir2_log_bad_magic(const char*, XfsInode*, xfs_dir2_db_t, BufHead*, uint32_t) {
    CHECK(fixture::active != nullptr);
    fixture::active->events.push_back(fixture::Op::BAD_MAGIC);
}
XfsTransaction::~XfsTransaction() {
    CHECK(buf_undo == nullptr && perag_undo == nullptr && inode_undo == nullptr && item_count == 0 &&
          items == inline_items.data() && !committed && !cancelled);
}
}
'''

TESTS = r'''
namespace fixture {
// Independent byte readers and structural predicates: never call DUT layout,
// free-record, bestfree, entry-size or insertion-position helpers here.
auto be16(const uint8_t* p) -> uint16_t { return uint16_t((uint16_t(p[0]) << 8) | p[1]); }
auto be32(const uint8_t* p) -> uint32_t {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3];
}
auto be64(const uint8_t* p) -> uint64_t { return (uint64_t(be32(p)) << 32) | be32(p + 4); }
void put16(uint8_t* p, uint16_t x) { p[0] = uint8_t(x >> 8); p[1] = uint8_t(x); }
void put32(uint8_t* p, uint32_t x) { for (int i = 0; i < 4; ++i) p[i] = uint8_t(x >> ((3 - i) * 8)); }
void put64(uint8_t* p, uint64_t x) { put32(p, uint32_t(x >> 32)); put32(p + 4, uint32_t(x)); }
auto hash(const std::string& name) -> uint32_t {
    uint32_t h = 0;
    for (unsigned char c : name) h = ((h << 7) | (h >> 25)) ^ c;
    return h;
}
auto entry_size(size_t name, bool ftype) -> size_t { return (8 + 1 + name + size_t(ftype) + 2 + 7) & ~size_t(7); }
auto oracle_crc(const uint8_t* p, size_t size, size_t checksum_offset) -> uint32_t {
    // Independent bitwise Castagnoli oracle, including the little-endian XFS
    // checksum slot. It does not reuse the DUT table, CPUID or CRC helpers.
    uint32_t crc = UINT32_MAX;
    for (size_t i = 0; i < size; ++i) {
        crc ^= i >= checksum_offset && i < checksum_offset + 4 ? 0 : p[i];
        for (unsigned bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ ((crc & 1U) != 0 ? 0x82f63b78U : 0);
    }
    return ~crc;
}
void checksum(Ram& f) {
    dir2_recompute_data_crc(f.bytes.data(), BS);
}
void free_record(Ram& f, size_t off, size_t len) {
    CHECK(len >= 8 && len % 8 == 0 && off + len <= BS);
    put16(f.bytes.data() + off, 0xffff);
    put16(f.bytes.data() + off + 2, uint16_t(len));
    put16(f.bytes.data() + off + len - 2, uint16_t(off));
}
void initial_entry(Ram& f, size_t off, const std::string& name, uint64_t ino, uint8_t type) {
    bool ft = f.mount.feat_incompat != 0;
    auto* p = f.bytes.data() + off;
    put64(p, ino); p[8] = uint8_t(name.size());
    std::memcpy(p + 9, name.data(), name.size());
    if (ft) p[9 + name.size()] = type;
    put16(p + entry_size(name.size(), ft) - 2, uint16_t(off));
}
Ram::Ram(bool ft) {
    CHECK(active == nullptr);
    active = this;
    mount.dir_blk_size = BS; mount.block_log = 12; mount.dir_blk_log = 0;
    mount.feat_incompat = ft ? XFS_SB_FEAT_INCOMPAT_FTYPE : 0;
    inode.mount = &mount; inode.ino = 128; inode.mode = 0040755;
    buffer.data = bytes.data(); buffer.refcount.store(0); buffer.size = BS;
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(bytes.data());
    hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_BLOCK_MAGIC);
    hdr->hdr.blkno = Be64::from_cpu(0x400);
    hdr->hdr.owner = Be64::from_cpu(inode.ino);
    // Stable UUID bytes, without assuming an assignment operator for its type.
    for (size_t i = 0; i < 16; ++i) bytes[offsetof(XfsDir3BlkHdr, uuid) + i] = uint8_t(0x40 + i);
    initial_entry(*this, 64, ".", 128, XFS_DIR3_FT_DIR);
    initial_entry(*this, 80, "..", 64, XFS_DIR3_FT_DIR);
    size_t end = BS - 8 - 2 * 8;
    free_record(*this, 96, end - 96);
    put16(bytes.data() + 48, 96); put16(bytes.data() + 50, uint16_t(end - 96));
    put32(bytes.data() + end, hash(".")); put32(bytes.data() + end + 4, 64 / 8);
    put32(bytes.data() + end + 8, hash("..")); put32(bytes.data() + end + 12, 80 / 8);
    put32(bytes.data() + BS - 8, 2); put32(bytes.data() + BS - 4, 0);
    checksum(*this);
}
Ram::~Ram() {
    CHECK(buffer.refcount.load() == 0 && !has_capture && !has_log && retained == released && active == this);
    active = nullptr;
}
struct Identity { uint64_t ino; uint8_t type; auto operator<=>(const Identity&) const = default; };
using Names = std::map<std::string, Identity>;
using Pair = std::pair<uint32_t, uint32_t>;
using Pairs = std::multiset<Pair>;
auto block_pairs(const Ram& f) -> Pairs {
    uint32_t n = be32(f.bytes.data() + BS - 8);
    CHECK(n <= (BS - 64 - 8) / 8);
    Pairs result;
    size_t start = BS - 8 - n * 8;
    for (uint32_t i = 0; i < n; ++i) result.emplace(be32(f.bytes.data() + start + i * 8), be32(f.bytes.data() + start + i * 8 + 4));
    return result;
}
void verify(const Ram& f, const Names& expected) {
    const auto* p = f.bytes.data();
    CHECK(be32(p) == XFS_DIR3_BLOCK_MAGIC && be64(p + 8) == 0x400 && be64(p + 16) == 0 && be64(p + 40) == 128);
    for (size_t i = 0; i < 16; ++i) CHECK(p[24 + i] == uint8_t(0x40 + i));
    uint32_t actual_crc; std::memcpy(&actual_crc, p + 4, 4);
    CHECK(actual_crc == oracle_crc(p, BS, 4));
    uint32_t n = be32(p + BS - 8), stale = be32(p + BS - 4);
    CHECK(n <= (BS - 64 - 8) / 8 && stale <= n);
    size_t end = BS - 8 - n * 8;
    std::map<uint32_t, std::pair<std::string, Identity>> data;
    std::map<size_t, size_t> free;
    bool previous_free = false, ft = f.mount.feat_incompat != 0;
    for (size_t off = 64; off < end;) {
        CHECK(off + 8 <= end && off % 8 == 0);
        if (be16(p + off) == 0xffff) {
            size_t len = be16(p + off + 2);
            CHECK(!previous_free && len >= 8 && len % 8 == 0 && off + len <= end && be16(p + off + len - 2) == off);
            CHECK(free.emplace(off, len).second); previous_free = true; off += len;
        } else {
            size_t len = p[off + 8], size = entry_size(len, ft);
            CHECK(len > 0 && off + size <= end && be16(p + off + size - 2) == off);
            std::string name(reinterpret_cast<const char*>(p + off + 9), len);
            uint8_t type = ft ? p[off + 9 + len] : XFS_DIR3_FT_UNKNOWN;
            CHECK(type <= XFS_DIR3_FT_WHT);
            for (size_t z = off + 9 + len + size_t(ft); z < off + size - 2; ++z) CHECK(p[z] == 0);
            CHECK(data.emplace(uint32_t(off / 8), std::make_pair(name, Identity{be64(p + off), type})).second);
            previous_free = false; off += size;
        }
    }
    uint32_t seen_stale = 0, last = 0;
    Names names;
    std::set<uint32_t> indexed;
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t h = be32(p + end + i * 8), addr = be32(p + end + i * 8 + 4);
        // This independent assertion is the original block defect oracle.
        CHECK(i == 0 || last <= h); last = h;
        if (addr == 0) { ++seen_stale; continue; }
        CHECK(data.contains(addr) && indexed.insert(addr).second);
        const auto& [name, id] = data.at(addr);
        CHECK(hash(name) == h && id.ino != 0 && names.emplace(name, id).second);
    }
    CHECK(seen_stale == stale && indexed.size() == data.size() && names == expected);
    size_t last_len = SIZE_MAX;
    std::set<size_t> best_offsets;
    std::vector<size_t> free_lengths;
    for (const auto& [off, len] : free) { (void)off; free_lengths.push_back(len); }
    std::sort(free_lengths.begin(), free_lengths.end(), std::greater<size_t>());
    for (size_t i = 0; i < 3; ++i) {
        size_t off = be16(p + 48 + 4 * i), len = be16(p + 50 + 4 * i);
        CHECK(len <= last_len); last_len = len;
        CHECK(len == (i < free_lengths.size() ? free_lengths[i] : 0));
        if (len == 0) CHECK(off == 0);
        else CHECK(free.contains(off) && free.at(off) == len && best_offsets.insert(off).second);
    }
}
void check_lookup(Ram& f, const Names& names) {
    for (const auto& [name, id] : names) {
        XfsDirEntry out{};
        CHECK(dir2_block_lookup_loaded(&f.inode, &f.buffer, name.data(), uint16_t(name.size()), &out, false) == 0);
        CHECK(out.ino == id.ino && out.ftype == id.type && out.namelen == name.size() && out.cookie == 0);
        CHECK(std::string(out.name.data()) == name);
    }
    XfsDirEntry untouched{};
    untouched.ino = 0xfeed;
    CHECK(dir2_block_lookup_loaded(&f.inode, &f.buffer, "absent-entry", 12, &untouched, false) == -ENOENT);
    CHECK(untouched.ino == 0xfeed);
}
void add(Ram& f, Names& names, const std::string& name, uint64_t ino = 256, uint8_t type = XFS_DIR3_FT_REG_FILE) {
    CHECK(!names.contains(name));
    auto before = block_pairs(f); uint32_t n = be32(f.bytes.data() + BS - 8), st = be32(f.bytes.data() + BS - 4);
    f.reset_events(); ++block_calls;
    CHECK(dir2_block_addname(&f.inode, name.data(), uint16_t(name.size()), ino, type, &f.transaction) == 0);
    CHECK((f.events == std::vector<Op>{Op::READ, Op::CAPTURE, Op::LOG_BUFFER, Op::RELEASE, Op::LOG_INODE}));
    f.retire_transaction();
    CHECK(f.buffer.refcount.load() == 0 && f.inode.dirty);
    names.emplace(name, Identity{ino, f.mount.feat_incompat != 0 ? type : XFS_DIR3_FT_UNKNOWN});
    verify(f, names); check_lookup(f, names);
    CHECK(be32(f.bytes.data() + BS - 8) == n + (st == 0 ? 1U : 0U));
    CHECK(be32(f.bytes.data() + BS - 4) == (st == 0 ? 0U : st - 1));
    auto after = block_pairs(f);
    // Independent multiset difference: exactly the new live pair and, for
    // reuse, exactly one former stale pair. No copied selection algorithm.
    auto inserted = after.end();
    for (auto it = after.begin(); it != after.end(); ++it) if (it->first == hash(name) && it->second != 0 && !before.contains(*it)) {
        CHECK(inserted == after.end()); inserted = it;
    }
    CHECK(inserted != after.end()); after.erase(inserted);
    if (st == 0) CHECK(after == before);
    else {
        Pairs missing = before;
        for (const auto& pair : after) { auto it = missing.find(pair); CHECK(it != missing.end()); missing.erase(it); }
        CHECK(missing.size() == 1 && missing.begin()->second == 0);
    }
}
void remove(Ram& f, Names& names, const std::string& name) {
    CHECK(names.contains(name)); auto before = block_pairs(f); uint32_t st = be32(f.bytes.data() + BS - 4);
    auto removed = before.end();
    for (auto it = before.begin(); it != before.end(); ++it) {
        if (it->second == 0 || it->first != hash(name)) continue;
        size_t off = size_t(it->second) * 8;
        CHECK(off >= 64 && off + 9 + name.size() <= BS);
        if (f.bytes[off + 8] == name.size() && std::memcmp(f.bytes.data() + off + 9, name.data(), name.size()) == 0) {
            CHECK(removed == before.end()); removed = it;
        }
    }
    CHECK(removed != before.end()); before.erase(removed); before.emplace(hash(name), 0);
    f.reset_events(); ++block_calls;
    CHECK(dir2_block_removename(&f.inode, name.data(), uint16_t(name.size()), &f.transaction) == 0);
    CHECK((f.events == std::vector<Op>{Op::READ, Op::CAPTURE, Op::LOG_BUFFER, Op::RELEASE}));
    f.retire_transaction();
    CHECK(!f.inode.dirty && f.buffer.refcount.load() == 0);
    names.erase(name); verify(f, names); check_lookup(f, names);
    CHECK(be32(f.bytes.data() + BS - 4) == st + 1);
    CHECK(block_pairs(f) == before); // All other hash/address pairs stay exact.
}
auto dots(bool ft) -> Names {
    return {{".", {128, uint8_t(ft ? XFS_DIR3_FT_DIR : XFS_DIR3_FT_UNKNOWN)}},
            {"..", {64, uint8_t(ft ? XFS_DIR3_FT_DIR : XFS_DIR3_FT_UNKNOWN)}}};
}
void defect_block(bool ft) {
    current_case = ft ? "block-stale-c-e-g-insert-f" : "block-stale-no-ftype"; ++cases;
    Ram f(ft); auto names = dots(ft); verify(f, names);
    for (const char* name : {"a", "c", "e", "g", "i"}) add(f, names, name, uint64_t(name[0]) + 256);
    for (const char* name : {"c", "e", "g"}) remove(f, names, name);
    add(f, names, "f", 400);
    // Retain stale entries while actual lookup/removal and both shift sides run.
    add(f, names, "b", 401); add(f, names, "h", 402);
    remove(f, names, "a"); remove(f, names, "i");
    add(f, names, "j", 403); add(f, names, "A", 404);
    for (const char* name : {"b", "f", "h", "j", "A"}) remove(f, names, name);
    CHECK(names == dots(ft));
}
void churn(bool ft) {
    current_case = ft ? "block-live-padding-free-tags-bestfree-churn" : "block-no-ftype-churn"; ++cases;
    Ram f(ft); auto names = dots(ft);
    for (unsigned round = 0; round < 3; ++round) {
        for (unsigned i = 0; i < 20; ++i) add(f, names, std::string(i % 7 + 1, char('a' + i)) + std::to_string(round), 1000 + i);
        for (unsigned i = 0; i < 20; i += 2) remove(f, names, std::string(i % 7 + 1, char('a' + i)) + std::to_string(round));
        for (unsigned i = 1; i < 20; i += 2) remove(f, names, std::string(i % 7 + 1, char('a' + i)) + std::to_string(round));
    }
    verify(f, names); CHECK(names == dots(ft));
}
void block_boundaries(bool ft) {
    current_case = ft ? "block-duplicate-hashes-and-boundaries" : "block-duplicate-hashes-no-ftype"; ++cases;
    Ram f(ft); auto names = dots(ft);
    std::string zero(32, 'A'), maximum(32, 'a');
    CHECK(hash(zero) == 0 && hash(maximum) == UINT32_MAX);
    CHECK(hash("A0000") == hash("Q0001"));
    for (const auto& name : {zero, std::string("A0000"), std::string("Q0001"), maximum})
        add(f, names, name, 3000 + names.size());
    remove(f, names, "A0000"); // Same hash has both stale and live entries.
    add(f, names, "b", 3010);
    remove(f, names, maximum); add(f, names, maximum, 3011);
    remove(f, names, zero); add(f, names, zero, 3012);
    // Component edge case: actual remover creates an entirely stale index.
    // Namespace callers forbid removing dots; this tests the format helpers.
    while (!names.empty()) remove(f, names, names.begin()->first);
    CHECK(be32(f.bytes.data() + BS - 8) == be32(f.bytes.data() + BS - 4));
    add(f, names, maximum, 3020); add(f, names, zero, 3021);
    remove(f, names, maximum); add(f, names, "middle", 3022);
    verify(f, names);
}
void refused(bool removal, int rc, Ram& f, const char* name, const std::vector<Op>& events) {
    auto before = f.bytes; f.reset_events(); ++block_calls;
    int actual = removal ? dir2_block_removename(&f.inode, name, uint16_t(std::strlen(name)), &f.transaction)
                         : dir2_block_addname(&f.inode, name, uint16_t(std::strlen(name)), 500, XFS_DIR3_FT_REG_FILE, &f.transaction);
    CHECK(actual == rc && f.bytes == before && f.events == events && f.buffer.refcount.load() == 0 && !f.inode.dirty);
}
void block_errors() {
    for (bool removal : {false, true}) {
        current_case = removal ? "remove-read-capture-rollback-seams" : "add-read-capture-rollback-seams"; ++cases;
        Ram f;
        f.read_error = -EIO; refused(removal, -EIO, f, removal ? "." : "a", {Op::READ}); f.read_error = 0;
        f.capture_error = -ENOMEM; refused(removal, -ENOMEM, f, removal ? "." : "a", {Op::READ, Op::CAPTURE, Op::RELEASE});
    }
    for (unsigned fault = 0; fault < 4; ++fault) {
        current_case = "block-malformed-before-capture"; ++cases; Ram f;
        if (fault == 0) put32(f.bytes.data(), 0);
        if (fault == 1) put32(f.bytes.data() + BS - 8, UINT32_MAX);
        if (fault == 2) put32(f.bytes.data() + BS - 4, 1);
        if (fault == 3) put16(f.bytes.data() + 78, 1);
        refused(false, -EINVAL, f, "a", {Op::READ, Op::RELEASE});
        if (fault < 2) refused(true, -EINVAL, f, ".", {Op::READ, Op::RELEASE});
    }
    { current_case = "block-missing-remove"; ++cases; Ram f; refused(true, -ENOENT, f, "not-here", {Op::READ, Op::RELEASE}); }
    {
        current_case = "block-real-space-exhaustion"; ++cases; Ram f; auto names = dots(true);
        // Real add path consumes space until it refuses, with exact unchanged
        // bytes at refusal. No manually fabricated full/free geometry.
        bool full = false;
        for (unsigned i = 0; i < 32; ++i) {
            std::string name = std::to_string(i) + std::string(200, 'x');
            auto before = f.bytes; f.reset_events(); ++block_calls;
            int rc = dir2_block_addname(&f.inode, name.data(), uint16_t(name.size()), 2000 + i, XFS_DIR3_FT_REG_FILE, &f.transaction);
            if (rc == -ENOSPC) {
                CHECK(f.bytes == before && (f.events == std::vector<Op>{Op::READ, Op::RELEASE}) && !f.inode.dirty);
                full = true; break;
            }
            CHECK(rc == 0); names.emplace(name, Identity{2000 + i, XFS_DIR3_FT_REG_FILE}); verify(f, names);
            CHECK((f.events == std::vector<Op>{Op::READ, Op::CAPTURE, Op::LOG_BUFFER, Op::RELEASE, Op::LOG_INODE}));
            f.retire_transaction();
        }
        CHECK(full && f.buffer.refcount.load() == 0); check_lookup(f, names);
    }
}

struct Leaf {
    alignas(8) std::array<uint8_t, BS> bytes{};
    XfsMountContext mount{};
    explicit Leaf(bool leaf1 = false) {
        mount.dir_blk_size = BS;
        hdr()->info.hdr.magic = Be16::from_cpu(leaf1 ? XFS_DIR3_LEAF_MAGIC : XFS_DIR3_LEAFN_MAGIC);
        put64(bytes.data() + 16, 0x500); put64(bytes.data() + 48, 128);
        for (size_t i = 0; i < 16; ++i) bytes[32 + i] = uint8_t(0x60 + i);
        if (leaf1) put32(bytes.data() + BS - 4, 2);
    }
    auto hdr() -> XfsDir3LeafHdr* { return reinterpret_cast<XfsDir3LeafHdr*>(bytes.data()); }
    void set(const std::vector<uint32_t>& hashes, uint32_t stale_mask) {
        CHECK(hashes.size() <= 16);
        hdr()->count = Be16::from_cpu(uint16_t(hashes.size()));
        unsigned stale = 0;
        for (size_t i = 0; i < hashes.size(); ++i) {
            put32(bytes.data() + 64 + i * 8, hashes[i]);
            bool is_stale = ((stale_mask >> i) & 1U) != 0;
            put32(bytes.data() + 64 + i * 8 + 4, is_stale ? 0 : uint32_t(i + 100)); stale += unsigned(is_stale);
        }
        hdr()->stale = Be16::from_cpu(uint16_t(stale));
        dir2_recompute_leaf_crc(bytes.data(), BS);
    }
    auto pairs() const -> Pairs {
        Pairs result; uint16_t n = be16(bytes.data() + 56); CHECK(n <= (BS - 64) / 8);
        for (uint16_t i = 0; i < n; ++i) result.emplace(be32(bytes.data() + 64 + i * 8), be32(bytes.data() + 64 + i * 8 + 4));
        return result;
    }
    void sorted() const {
        uint16_t n = be16(bytes.data() + 56), stale = be16(bytes.data() + 58); unsigned seen = 0;
        CHECK(n <= (BS - 64) / 8 && stale <= n);
        uint32_t prev = 0;
        for (uint16_t i = 0; i < n; ++i) {
            uint32_t h = be32(bytes.data() + 64 + i * 8), addr = be32(bytes.data() + 64 + i * 8 + 4);
            // The actual leaf prepare/reuse predecessor fails this oracle.
            CHECK(i == 0 || prev <= h); prev = h; seen += unsigned(addr == 0);
        }
        CHECK(seen == stale);
    }
    void checksum_and_identity() const {
        uint32_t crc; std::memcpy(&crc, bytes.data() + 12, 4);
        CHECK(crc == oracle_crc(bytes.data(), BS, 12));
        CHECK(be64(bytes.data() + 16) == 0x500 && be64(bytes.data() + 24) == 0 && be64(bytes.data() + 48) == 128);
        for (size_t i = 0; i < 16; ++i) CHECK(bytes[32 + i] == uint8_t(0x60 + i));
    }
};
void leaf_insert(Leaf& f, uint32_t h, uint32_t dataptr = 0x12345) {
    f.sorted(); auto before = f.pairs(); uint16_t count = f.hdr()->count.to_cpu(), stale = f.hdr()->stale.to_cpu();
    auto bytes_before = f.bytes;
    int s = -900, p = -900; ++leaf_calls;
    CHECK(dir2_leaf_prepare_stale_insert(f.hdr(), h, &s, &p) == 0);
    CHECK(f.bytes == bytes_before);
    CHECK(s >= 0 && s < count && p >= 0 && p <= count && be32(f.bytes.data() + 64 + size_t(s) * 8 + 4) == 0);
    Pair selected{be32(f.bytes.data() + 64 + size_t(s) * 8), 0};
    auto removed = before.find(selected); CHECK(removed != before.end()); before.erase(removed);
    before.emplace(h, dataptr);
    ++leaf_calls; dir2_leaf_reuse_stale_entry(f.hdr(), h, dataptr, s, p);
    f.sorted(); CHECK(f.pairs() == before && f.hdr()->count.to_cpu() == count && f.hdr()->stale.to_cpu() == stale - 1);
    // The real leaf caller updates CRC after reuse; compile the CRC helper too.
    dir2_recompute_leaf_crc(f.bytes.data(), BS); f.checksum_and_identity();
    for (size_t i = 0; i < BS; ++i) {
        bool mutable_byte = (i >= 12 && i < 16) || (i >= 24 && i < 32) || (i >= 58 && i < 60) ||
                            (i >= 64 && i < 64 + count * 8);
        if (!mutable_byte) CHECK(f.bytes[i] == bytes_before[i]);
    }
}
void leaf_tests() {
    current_case = "leaf-stale-c-e-g-insert-f"; ++cases;
    { Leaf f; f.set({97, 99, 101, 103, 105}, 0x0e); leaf_insert(f, 102); }
    for (bool leaf1 : {false, true}) {
        current_case = "leaf-repeated-reuse-and-deleted-hash-fixture-transitions"; ++cases;
        Leaf f(leaf1); f.set({0, 97, 99, 101, 103, 105, UINT32_MAX}, 0x1d);
        for (uint32_t h : {102U, 98U, UINT32_MAX, 0U}) {
            leaf_insert(f, h, 0x20000 + h);
            // Fixture the index half of a deletion: preserve its old hash.
            // This explicitly does not claim to execute leaf/node removename.
            bool deleted = false;
            for (size_t i = 0; i < f.hdr()->count.to_cpu(); ++i) {
                auto* entry = f.bytes.data() + 64 + i * 8;
                if (!deleted && be32(entry + 4) == 0x20000 + h) {
                    put32(entry + 4, 0); deleted = true;
                    f.hdr()->stale = Be16::from_cpu(uint16_t(f.hdr()->stale.to_cpu() + 1));
                }
            }
            CHECK(deleted); dir2_recompute_leaf_crc(f.bytes.data(), BS); f.sorted(); f.checksum_and_identity();
        }
    }
    const std::vector<std::vector<uint32_t>> hashes{{0}, {UINT32_MAX}, {0, 0, 0}, {UINT32_MAX, UINT32_MAX},
        {0, 1, 1, 2, UINT32_MAX}, {3, 9, 9, 17, 33, UINT32_MAX}};
    for (const auto& values : hashes) for (uint32_t mask = 1; mask < (1U << values.size()); ++mask)
        for (uint32_t h : std::array<uint32_t, 6>{0, 1, 9, 16, 34, UINT32_MAX}) for (bool leaf1 : {false, true}) {
            current_case = "leaf-sorted-stale-live-multiset-boundaries"; ++cases;
            Leaf f(leaf1); f.set(values, mask); leaf_insert(f, h);
        }
    for (bool leaf1 : {false, true}) for (uint32_t h : std::array<uint32_t, 2>{0, UINT32_MAX}) {
        current_case = "leaf-real-new-UINTMAX-sentinel-reservation"; ++cases;
        Leaf f(leaf1); f.set({1, 7, UINT32_MAX}, 0); auto before = f.pairs();
        ++leaf_calls; CHECK(dir2_leaf_ensure_stale_slot(&f.mount, f.hdr()) == 0);
        before.emplace(UINT32_MAX, 0); f.sorted(); CHECK(f.pairs() == before && f.hdr()->count.to_cpu() == 4 && f.hdr()->stale.to_cpu() == 1);
        auto ensured = f.bytes; ++leaf_calls; CHECK(dir2_leaf_ensure_stale_slot(&f.mount, f.hdr()) == 0 && f.bytes == ensured);
        leaf_insert(f, h);
    }
    current_case = "leaf-preflight-and-prepare-refusal"; ++cases;
    {
        Leaf f; f.set({1, 2}, 0); auto before = f.bytes; int s = 71, p = 72; ++leaf_calls;
        CHECK(dir2_leaf_prepare_stale_insert(f.hdr(), 3, &s, &p) == -ENOSPC && s == 71 && p == 72 && f.bytes == before);
        f.hdr()->stale = Be16::from_cpu(1); before = f.bytes; ++leaf_calls;
        CHECK(dir2_leaf_prepare_stale_insert(f.hdr(), 3, &s, &p) == -EINVAL && s == 71 && p == 72 && f.bytes == before);
        ++leaf_calls; CHECK(dir2_leaf_ensure_stale_slot(&f.mount, f.hdr()) == -EINVAL && f.bytes == before);
        f.hdr()->info.hdr.magic = Be16::from_cpu(0); before = f.bytes; ++leaf_calls;
        CHECK(dir2_leaf_ensure_stale_slot(&f.mount, f.hdr()) == -EINVAL && f.bytes == before);
    }
    for (bool leaf1 : {false, true}) {
        Leaf f(leaf1); size_t capacity = (BS - 64 - (leaf1 ? 4 + 2 * 2 : 0)) / 8;
        f.hdr()->count = Be16::from_cpu(uint16_t(capacity));
        for (size_t i = 0; i < capacity; ++i) { put32(f.bytes.data() + 64 + i * 8, uint32_t(i)); put32(f.bytes.data() + 68 + i * 8, uint32_t(i + 100)); }
        auto before = f.bytes; ++leaf_calls;
        CHECK(dir2_leaf_ensure_stale_slot(&f.mount, f.hdr()) == -ENOSPC && f.bytes == before);
    }
}
void tests(const char* group) {
    CHECK(util::crc32c_compute("123456789", 9) == 0xe3069283U);
    if (std::strcmp(group, "all") == 0 || std::strcmp(group, "block") == 0) {
        defect_block(true); defect_block(false); churn(true); churn(false);
        block_boundaries(true); block_boundaries(false); block_errors();
    }
    if (std::strcmp(group, "all") == 0 || std::strcmp(group, "leaf") == 0) leaf_tests();
    CHECK(active == nullptr && cases != 0);
    std::printf("PASS group=%s cases=%llu assertions=%llu block_calls=%llu leaf_calls=%llu "
                "actual_directory_bodies=1 real_filesystem_types=1 actual_crc=1 "
                "ram_capture_seams=1 real_transactions=0 wal=0 device=0 runtime_cause_proven=0\n", group,
                static_cast<unsigned long long>(cases), static_cast<unsigned long long>(assertions),
                static_cast<unsigned long long>(block_calls), static_cast<unsigned long long>(leaf_calls));
}
}
int main(int argc, char** argv) { CHECK(argc == 2); fixture::tests(argv[1]); }
'''

def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

def captures(source: str, name: str, kind: str = "function") -> list[tuple[str, dict]]:
    """Capture complete actual definitions; no instruction/body substitutions."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    if kind == "function":
        pattern = rf"^(?:inline )?(?:auto|void) {re.escape(name)}\([^{{;]*\{{"
    else:
        pattern = rf"^struct {re.escape(name)}\s*\{{"
    matches = list(re.finditer(pattern, clean, re.M))
    expected = 2 if name == "dir2_leaf_entries" else 1
    if len(matches) != expected:
        raise ValueError(f"expected {expected} complete definitions for {name}, got {len(matches)}")
    results = []
    for match in matches:
        start = match.start(); opening = clean.index("{", start); depth = 0
        for end in range(opening, len(clean)):
            if clean[end] == "{": depth += 1
            elif clean[end] == "}":
                depth -= 1
                if depth == 0:
                    end += 1
                    if kind == "struct": end = clean.index(";", end) + 1
                    body = source[start:end]
                    results.append((body, {"name": name, "start_character": start, "end_character_exclusive": end,
                                           "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}))
                    break
        else:
            raise ValueError(f"unterminated definition: {name}")
    return results

def generate(source: str, source_path: Path) -> tuple[str, list[dict]]:
    actual = []
    for name in ["BestFreeSlot", "XfsDir3LeafHdr", "XfsDir2LeafTail"]:
        actual.extend(captures(source, name, "struct"))
    for name in ["rol32", "xfs_da_hashname", "dir2_data_entsize", "dir2_data_entry_ino", "dir2_data_entry_ftype",
                 "fill_dir_entry", "dir2_data_entry_at_if_valid", "dir2_data_unused_at_if_valid", "dir2_dataptr_to_off",
                 "dir2_block_linear_lookup", "dir2_block_lookup_loaded", "dir2_leaf_entries", "dir2_leaf_layout",
                 "dir2_count_stale_leaf_entries", "dir2_rebuild_data_bestfree", "dir2_block_addname", "dir2_block_removename",
                 "dir2_leaf_prepare_stale_insert", "dir2_leaf_preflight_index_slot", "dir2_leaf_ensure_stale_slot", "dir2_leaf_reuse_stale_entry",
                 "dir2_recompute_data_crc", "dir2_recompute_leaf_crc"]:
        actual.extend(captures(source, name))
    constants = re.findall(r"^constexpr size_t XFS_DA3_CRC_OFF\s*=.*?;", source, re.M)
    if len(constants) != 1:
        raise ValueError("expected the actual XFS_DA3_CRC_OFF declaration")
    sections = [f'#line {source.count(chr(10), 0, row["start_character"]) + 1} {json.dumps(str(source_path))}\n{body}'
                for body, row in actual]
    text = PRELUDE + "\nnamespace ker::vfs::xfs {\n" + constants[0] + "\n" + "\n\n".join(sections) + "\n}\n"
    return text + TESTS, [row for _, row in actual]


SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024


def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    # No privilege is required. Compiler grandchildren remain ours to reap if
    # their parent exits during a timeout. Never wait for another process group.
    if not sys.platform.startswith("linux"):
        raise ValueError("the bounded runner requires Linux child subreaping")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Same session includes children that changed their process group.
        # Sequential execution creates no other children in this driver; a
        # reparented direct child is therefore an owned compiler descendant.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result


def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)


def reap_group(process: subprocess.Popen, deadline: float) -> None:
    # Keep the leader unreaped until its session and adopted descendants are
    # inactive. pidfds plus start times prevent PID reuse from changing targets.
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass  # Still under its dying parent; subreaper will adopt it.
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            return  # Retained leader was the final owned session member.
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session remains live; preserve its scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("120-second whole-test budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET:
                    raise ValueError("command log exceeded 1 MiB")
                if allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("private scratch exceeded 64 MiB")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("120-second whole-test budget exhausted")
                # WNOWAIT deliberately leaves the owned leader's PID reserved.
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {signum: signal.signal(signum, signal.SIG_IGN)
                            for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for signum, handler in handlers.items():
                        signal.signal(signum, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 3),
            "owned_group_reaped": True, "log_bytes": log.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir-source", "--dir2-source", type=Path, default=SOURCE)
    parser.add_argument("--only", "--group", choices=("all", "block", "leaf"), default="all")
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    parser.add_argument("--receipt", type=Path, help="optional exclusive small JSON result")
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 120
    scratch = None
    groups = []
    result = {"pass": False, "only": args.only, "commands": [], "scratch_removed": False,
              "scope": "complete actual directory bodies/real XFS types/CRC with RAM and capture/log seams",
              "limits": {"seconds": 120, "scratch_bytes": SCRATCH_BUDGET, "log_bytes": LOG_BUDGET,
                         "child_file_bytes": FILE_BUDGET, "core_bytes": 0}}
    watched = []
    before = []
    try:
        if args.receipt is not None and args.receipt.exists():
            raise ValueError("receipt already exists")
        source_path = args.dir_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        with compiler.open("rb") as executable:
            if executable.read(4) != b"\x7fELF":
                raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        subreaper()
        watched = [source_path, CRC, Path(__file__).resolve(), compiler,
                   ROOT / "tests/host/kern_shim/platform/dbg/dbg.hpp"]
        watched.extend(KERNEL / relative for relative in [
            "util/crc32c.hpp", "vfs/fs/xfs/xfs_dir2.hpp", "vfs/fs/xfs/xfs_format.hpp",
            "vfs/fs/xfs/xfs_mount.hpp", "vfs/fs/xfs/xfs_trans.hpp", "vfs/fs/xfs/xfs_inode.hpp",
            "vfs/buffer_cache.hpp", "dev/block_device.hpp", "net/endian.hpp", "vfs/stat.hpp",
            "vfs/native_coherency.hpp", "vfs/coherency.hpp", "vfs/exec_content.hpp",
        ])
        before = [pin(p) for p in watched]
        result["source"] = before[0]
        harness, definitions = generate(source_path.read_text(), source_path)
        result["actual_definitions"] = definitions
        scratch = Path(tempfile.mkdtemp(prefix="wos-xfs-stale-", dir="/tmp"))
        result["scratch"] = str(scratch)
        for relative, content in {
            "platform/sys/locks.hpp": LOCKS,
            "platform/sys/spinlock.hpp": '#pragma once\n#include "locks.hpp"\n',
            "platform/sys/mutex.hpp": '#pragma once\n#include "locks.hpp"\n',
            "bits/off_t.h": "#pragma once\n#include <sys/types.h>\n",
            "platform/dbg/dbg.hpp": (ROOT / "tests/host/kern_shim/platform/dbg/dbg.hpp").read_text(),
            "directory.cpp": harness,
        }.items():
            path = scratch / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        flags = ["-std=c++23", "-O1", "-g0", "-pipe", "-fsanitize=address,undefined",
                 "-fno-sanitize=vptr", "-fno-omit-frame-pointer", "-I", str(scratch), "-I", str(KERNEL)]
        objects = []
        for index, input_path in enumerate([scratch / "directory.cpp", CRC]):
            obj = scratch / f"input-{index}.o"
            row = command([str(compiler), *flags, "-c", str(input_path), "-o", str(obj)],
                          scratch / f"compile-{index}.log", scratch, deadline, groups)
            result["commands"].append(row)
            if row["exit"] != 0:
                print((scratch / f"compile-{index}.log").read_text(errors="replace")[-32768:])
                raise ValueError(f"actual-source compile {index} failed with exit {row['exit']}")
            objects.append(obj)
        binary = scratch / "directory-test"
        row = command([str(compiler), "-fsanitize=address,undefined", *map(str, objects), "-o", str(binary)],
                      scratch / "link.log", scratch, deadline, groups)
        result["commands"].append(row)
        if row["exit"] != 0:
            print((scratch / "link.log").read_text(errors="replace")[-32768:])
            raise ValueError(f"link failed with exit {row['exit']}")
        row = command([str(binary), args.only], scratch / "runtime.log", scratch, deadline, groups)
        result["commands"].append(row)
        log = (scratch / "runtime.log").read_text(errors="replace")
        print(log[-32768:], end="")
        result["runtime_log"] = log[-32768:]
        if row["exit"] != 0:
            raise ValueError(f"actual directory assertions failed with exit {row['exit']}")
        if not re.search(rf"^PASS group={args.only} cases=\d+ assertions=\d+ block_calls=\d+ leaf_calls=\d+ ", log, re.M):
            raise ValueError("compiled suite's terminal PASS row is missing")
        if [pin(p) for p in watched] != before:
            raise ValueError("selected directory/CRC/driver source changed during execution")
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        # command() removes a group only after its leader and descendants have
        # been reaped and the group is absent. Delete only this mkdtemp-owned
        # tree, after every command on every managed pass/failure path.
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result["scratch_removed"] = not scratch.exists()
        if groups:
            result["pass"] = False
            result["cleanup_error"] = f"owned groups {groups} could not be reaped; scratch preserved"
        if before:
            try:
                result["sources_held"] = [pin(p) for p in watched] == before
            except OSError:
                result["sources_held"] = False
            if not result["sources_held"]:
                result["pass"] = False
                result["source_error"] = "selected directory/CRC/driver source changed during execution"
        if time.monotonic() > deadline:
            result["pass"] = False
            result["deadline_error"] = "whole invocation exceeded 120 seconds including cleanup"
        result["seconds"] = round(time.monotonic() - started, 3)
        if args.receipt is not None and not args.receipt.exists():
            with args.receipt.open("x") as output:
                json.dump(result, output, indent=2)
                output.write("\n")
                output.flush()
                if time.monotonic() > deadline:
                    result["pass"] = False
                    result["deadline_error"] = "whole invocation exceeded 120 seconds including publication"
                    output.seek(0); output.truncate()
                    json.dump(result, output, indent=2); output.write("\n")
        if time.monotonic() > deadline:
            result["pass"] = False
            result["deadline_error"] = "whole invocation exceeded 120 seconds including publication"
        result["seconds"] = round(time.monotonic() - started, 3)
        print(json.dumps({key: value for key, value in result.items()
                          if key not in ("actual_definitions", "runtime_log", "scope", "limits")}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
