#pragma once

namespace netd {

auto find_ifname_for_driver(const char* driver, const char* fallback) -> const char*;

}  // namespace netd
