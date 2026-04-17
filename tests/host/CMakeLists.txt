# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

wos_add_unit_test(wki_wire_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_wire_test.cpp
)

wos_add_unit_test(wki_routing_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_routing_test.cpp
)

wos_add_unit_test(data_struct_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/data_struct_test.cpp
)

wos_add_unit_test(tcp_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/tcp_test.cpp
)

wos_add_unit_test(xfs_format_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/xfs_format_test.cpp
)

wos_add_unit_test(wki_channel_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_channel_test.cpp
)

# ---------------------------------------------------------------------------
# Fuzz targets
# ---------------------------------------------------------------------------

wos_add_fuzz_target(wki_wire_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/wki_wire_fuzz.cpp
)

wos_add_fuzz_target(data_struct_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/data_struct_fuzz.cpp
)

wos_add_fuzz_target(tcp_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/tcp_fuzz.cpp
)

wos_add_fuzz_target(xfs_format_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/xfs_format_fuzz.cpp
)

wos_add_fuzz_target(wki_routing_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/wki_routing_fuzz.cpp
)
