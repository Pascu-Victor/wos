#pragma once

#include <limine.h>

#include <mod/io/port/port.hpp>
#include <std/hcf.hpp>
#include <std/mem.hpp>
#include <std/string.hpp>

namespace ker::mod::io {
namespace serial {
void init(void);
void write(const char *str);
void write(const char c);
void write(uint64_t num);
void writeHex(uint64_t num);
void writeBin(uint64_t num);
}  // namespace serial
}  // namespace ker::mod::io
