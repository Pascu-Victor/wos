cmake_minimum_required(VERSION 3.20)

add_library(wos_telemetry STATIC src/telemetry.cpp)
add_library(wos::telemetry ALIAS wos_telemetry)

target_include_directories(wos_telemetry PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(wos_telemetry PUBLIC cxx_std_23)
set_target_properties(wos_telemetry PROPERTIES POSITION_INDEPENDENT_CODE ON)
