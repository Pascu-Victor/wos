#pragma once
// Host shim for mod/io/serial/serial.hpp — stubs serial output to stderr.

#include <cstddef>
#include <cstdint>
#include <cstdio>

namespace ker::mod::io::serial {

inline void init() {}
inline void write(const char* str) { fprintf(stderr, "%s", str); }
inline void write(const char* str, uint64_t len) { fwrite(str, 1, len, stderr); }
inline void write(char c) { fputc(c, stderr); }
inline void write(uint64_t num) { fprintf(stderr, "%lu", num); }
inline void writeHex(uint64_t num) { fprintf(stderr, "0x%lx", num); }
inline void writeBin(uint64_t) {}
inline void acquireLock() {}
inline void releaseLock() {}

}  // namespace ker::mod::io::serial
