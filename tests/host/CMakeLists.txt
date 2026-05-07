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

wos_add_unit_test(bitmap_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/bitmap_test.cpp
)

wos_add_unit_test(net_checksum_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/net_checksum_test.cpp
    KERN_SOURCES ${WOS_KERN_SRC}/net/checksum.cpp
)

wos_add_unit_test(util_list_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/util_list_test.cpp
)

wos_add_unit_test(crc32c_test
    SOURCES      ${CMAKE_CURRENT_SOURCE_DIR}/unit/crc32c_test.cpp
    KERN_SOURCES ${WOS_KERN_SRC}/util/crc32c.cpp
)

wos_add_unit_test(buffer_cache_test
    SOURCES      ${CMAKE_CURRENT_SOURCE_DIR}/unit/buffer_cache_test.cpp
    KERN_SOURCES ${WOS_KERN_SRC}/vfs/buffer_cache.cpp
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

wos_add_fuzz_target(slab_fuzz
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/slab_fuzz.cpp
)
