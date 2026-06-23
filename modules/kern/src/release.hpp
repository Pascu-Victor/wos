#pragma once

namespace ker::release {

#define WOS_RELEASE_STRINGIFY_IMPL(x) #x
#define WOS_RELEASE_STRINGIFY(x) WOS_RELEASE_STRINGIFY_IMPL(x)

constexpr const char* NAME = "WOS";
constexpr const char* VERSION = "0.1";
constexpr const char* SMP = "SMP";
static constexpr const char* WOS_LOGO =
    "██╗    ██╗ ██████╗ ███████╗\n"
    "██║    ██║██╔═══██╗██╔════╝\n"
    "██║ █╗ ██║██║   ██║███████╗\n"
    "██║███╗██║██║   ██║╚════██║\n"
    "╚███╔███╔╝╚██████╔╝███████║\n"
    " ╚══╝╚══╝  ╚═════╝ ╚══════╝\n";

#ifdef __clang__
constexpr const char* COMPILER = "clang " WOS_RELEASE_STRINGIFY(__clang_major__) "." WOS_RELEASE_STRINGIFY(
    __clang_minor__) "." WOS_RELEASE_STRINGIFY(__clang_patchlevel__);
#elif defined(__GNUC__)
constexpr const char* COMPILER =
    "gcc " WOS_RELEASE_STRINGIFY(__GNUC__) "." WOS_RELEASE_STRINGIFY(__GNUC_MINOR__) "." WOS_RELEASE_STRINGIFY(__GNUC_PATCHLEVEL__);
#else
constexpr const char* COMPILER = "unknown";
#endif

#undef WOS_RELEASE_STRINGIFY
#undef WOS_RELEASE_STRINGIFY_IMPL

#ifdef WOS_BUILDER
constexpr const char* BUILDER = WOS_BUILDER;
#else
constexpr const char* BUILDER = "unknown@unknown";
#endif

}  // namespace ker::release
