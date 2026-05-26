#pragma once

namespace ker::release {

constexpr const char* NAME = "WOS";
constexpr const char* VERSION = "0.1";
constexpr const char* SMP = "SMP";

#ifdef __clang__
constexpr const char* COMPILER = "clang " __clang_version__;
#elifdef __GNUC__
constexpr const char* COMPILER = "gcc " __VERSION__;
#else
constexpr const char* COMPILER = "unknown";
#endif

#ifdef WOS_BUILDER
constexpr const char* BUILDER = WOS_BUILDER;
#else
constexpr const char* BUILDER = "unknown@unknown";
#endif

}  // namespace ker::release
