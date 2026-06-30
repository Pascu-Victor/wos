#pragma once

#include <string_view>

namespace httpd {

void handle_request(int client_fd, std::string_view request);

}  // namespace httpd
