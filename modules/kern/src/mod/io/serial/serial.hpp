#pragma once

#include <limine.h>

#include <cstddef>
#include <mod/io/port/port.hpp>

extern "C" __attribute__((noreturn)) void hcf() noexcept;

namespace ker::mod::io {
namespace serial {
void init(void);
void write(const char *str);
void write(const char *str, uint64_t len);
void write(const char c);
void write(uint64_t num);
void writeHex(uint64_t num);
void writeBin(uint64_t num);
}  // namespace serial
}  // namespace ker::mod::io
