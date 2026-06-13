# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_test(
    NAME host_test_manifest_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/host_test_manifest_test.py
)
add_test(
    NAME ktest_cov_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/ktest_cov_test.py
)
add_test(
    NAME ktest_manifest_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/ktest_manifest_test.py
)
add_test(
    NAME runtime_test_audit_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/runtime_test_audit_test.py
)
add_test(
    NAME testd_manifest_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/testd_manifest_test.py
)
add_test(
    NAME testprog_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/testprog_source_test.py
)
add_test(
    NAME httpd_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/httpd_source_test.py
)
add_test(
    NAME top_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/top_source_test.py
)
add_test(
    NAME init_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/init_source_test.py
)
add_test(
    NAME strace_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/strace_source_test.py
)
add_test(
    NAME journal_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/journal_source_test.py
)
add_test(
    NAME wkictl_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wkictl_source_test.py
)
add_test(
    NAME perf_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/perf_source_test.py
)
add_test(
    NAME memacc_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/memacc_source_test.py
)
add_test(
    NAME renderbench_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/renderbench_source_test.py
)
add_test(
    NAME debugserver_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/debugserver_source_test.py
)
add_test(
    NAME sftp_timeout_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/sftp_timeout_source_test.py
)
add_test(
    NAME netpoll_backlog_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/netpoll_backlog_source_test.py
)
add_test(
    NAME futex_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/futex_source_test.py
)
add_test(
    NAME waitpid_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/waitpid_source_test.py
)
add_test(
    NAME phys_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/phys_source_test.py
)
add_test(
    NAME wki_timer_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_timer_source_test.py
)
add_test(
    NAME wki_wait_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_wait_source_test.py
)
add_test(
    NAME wki_dev_server_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_dev_server_source_test.py
)
add_test(
    NAME wki_dev_proxy_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_dev_proxy_source_test.py
)
add_test(
    NAME wki_remote_ipc_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_remote_ipc_source_test.py
)
add_test(
    NAME wki_remote_net_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_remote_net_source_test.py
)
add_test(
    NAME wki_roce_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_roce_source_test.py
)
add_test(
    NAME wki_remotable_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_remotable_source_test.py
)
add_test(
    NAME wki_zone_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_zone_source_test.py
)
add_test(
    NAME wki_peer_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_peer_source_test.py
)
add_test(
    NAME wki_remote_compute_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_remote_compute_source_test.py
)
add_test(
    NAME wki_remote_vfs_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_remote_vfs_source_test.py
)
add_test(
    NAME vfs_mount_lifetime_source_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/unit/vfs_mount_lifetime_source_test.py
)
set_tests_properties(
    host_test_manifest_test
    ktest_cov_test
    ktest_manifest_test
    runtime_test_audit_test
    testd_manifest_test
    testprog_source_test
    httpd_source_test
    top_source_test
    init_source_test
    strace_source_test
    journal_source_test
    wkictl_source_test
    perf_source_test
    memacc_source_test
    renderbench_source_test
    debugserver_source_test
    sftp_timeout_source_test
    netpoll_backlog_source_test
    futex_source_test
    waitpid_source_test
    phys_source_test
    wki_timer_source_test
    wki_wait_source_test
    wki_dev_server_source_test
    wki_dev_proxy_source_test
    wki_remote_ipc_source_test
    wki_remote_net_source_test
    wki_roce_source_test
    wki_remotable_source_test
    wki_zone_source_test
    wki_peer_source_test
    wki_remote_compute_source_test
    wki_remote_vfs_source_test
    vfs_mount_lifetime_source_test
    PROPERTIES LABELS "host;source"
)

wos_add_unit_test(wki_wire_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_wire_test.cpp
)

wos_add_unit_test(wki_routing_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_routing_test.cpp
)

wos_add_unit_test(data_struct_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/data_struct_test.cpp
)

wos_add_unit_test(hashtable_limit_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/hashtable_limit_test.cpp
)

wos_add_unit_test(tcp_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/tcp_test.cpp
)

wos_add_unit_test(xfs_format_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/xfs_format_test.cpp
)

wos_add_unit_test(wki_channel_test
    SOURCES      ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_channel_test.cpp
    KERN_SOURCES ${WOS_KERN_SRC}/net/wki/channel.cpp
)

wos_add_unit_test(wki_peer_liveness_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_peer_liveness_test.cpp
)

wos_add_unit_test(wki_event_test
    SOURCES     ${CMAKE_CURRENT_SOURCE_DIR}/unit/wki_event_test.cpp
)

wos_add_unit_test(renderbench_options_test
    SOURCES      ${CMAKE_CURRENT_SOURCE_DIR}/unit/renderbench_options_test.cpp
    KERN_SOURCES ${WOS_ROOT}/modules/renderbench/src/render_core.cpp
                 ${WOS_ROOT}/modules/renderbench/src/render_protocol.cpp
                 ${WOS_ROOT}/modules/testprog/src/mandelbench/lodepng.cpp
)
target_include_directories(renderbench_options_test PRIVATE
    ${WOS_ROOT}/modules/renderbench/src
    ${WOS_ROOT}/modules/testprog/src
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
target_compile_definitions(buffer_cache_test PRIVATE WOS_HOST_TEST=1)

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
