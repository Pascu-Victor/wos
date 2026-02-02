cmake_minimum_required(VERSION 3.20)

include(ExternalProject)
ExternalProject_Add(wos_tools_build
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/tools
    BINARY_DIR ${CMAKE_BINARY_DIR}/tools
    CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        -DCMAKE_BUILD_TYPE=Release
    INSTALL_COMMAND ""
    USES_TERMINAL_CONFIGURE TRUE
    USES_TERMINAL_BUILD TRUE
)

# Always run the build step
ExternalProject_Add_StepTargets(wos_tools_build build)
add_custom_target(wos_tools_build_always ALL
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/tools
    DEPENDS wos_tools_build-build
    COMMENT "Building WOS tools..."
)

project(wos NONE)

#WOs modules
add_subdirectory(modules)
