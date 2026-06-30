#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace memacc {

constexpr uint64_t KIB = 1024;
constexpr int DEFAULT_LIMIT = 15;

struct Row {
    std::string record;
    std::unordered_map<std::string, std::string> kv;
};

struct ProcRow {
    uint64_t pid = 0;
    uint64_t ppid = 0;
    uint64_t virt_bytes = 0;
    uint64_t rss_bytes = 0;
    uint64_t shr_bytes = 0;
    uint64_t pte_bytes = 0;
    uint64_t heap_bytes = 0;
    uint64_t mmap_bytes = 0;
    uint64_t stack_bytes = 0;
    uint64_t total_bytes = 0;
    uint64_t cpu = 0;
    std::string state;
    std::string name;
    std::string cmd;
};

struct Options {
    bool full = false;
    std::optional<uint64_t> pid;
    std::string name;
    std::string state;
    std::string sort = "rss";
    uint64_t min_kib = 0;
    int limit = DEFAULT_LIMIT;
    int interval_seconds = 1;
};

constexpr auto bytes_to_kib(uint64_t bytes) -> uint64_t { return bytes / KIB; }

}  // namespace memacc
