#pragma once

#include <bits/ssize_t.h>

#include <cstddef>
#include <string_view>

namespace httpd {

inline constexpr std::string_view HTTP_404_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>404 Not Found</title></head><body>"
    "<h1>404 Not Found</h1><p>The requested resource was not found on this server.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

inline constexpr std::string_view HTTP_403_RESPONSE =
    "HTTP/1.1 403 Forbidden\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>403 Forbidden</title></head><body>"
    "<h1>403 Forbidden</h1><p>Access to this resource is denied.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

inline constexpr std::string_view HTTP_500_RESPONSE =
    "HTTP/1.1 500 Internal Server Error\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>500 Internal Server Error</title></head><body>"
    "<h1>500 Internal Server Error</h1><p>An error occurred processing your request.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

auto send_response(int client_fd, int status_code, const char* status_text, const char* content_type, const void* body, size_t body_len)
    -> ssize_t;

}  // namespace httpd
