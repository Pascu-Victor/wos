#pragma once

#include <sys/logging.h>

#include <format>
#include <string>
#include <utility>

namespace httpd {

using httpd_log = wos::journal<"httpd">;

template <typename... Args>
void log_message(std::format_string<Args...> fmt, Args&&... args) {
    std::string const MSG = std::format(fmt, std::forward<Args>(args)...);
    httpd_log::info("%s", MSG.c_str());
}

}  // namespace httpd
