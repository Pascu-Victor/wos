#pragma once

#include <platform/acpi/bounded_tables.hpp>
#include <string_view>

namespace ker::mod::acpi {
[[nodiscard]] auto find_table_bounded(std::string_view) -> bounded::Result;
}
