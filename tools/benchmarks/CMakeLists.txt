find_package(MPI COMPONENTS CXX QUIET)

if(NOT MPI_CXX_FOUND)
    message(WARNING "MPI not found; skipping MPI benchmarks")
    return()
endif()
find_package(Threads REQUIRED)

set(RENDERBENCH_ROOT "${WOS_WORKSPACE_ROOT}/modules/renderbench/src")
set(MANDELBENCH_ROOT "${WOS_WORKSPACE_ROOT}/modules/testprog/src/mandelbench")

add_executable(wos_mpi_netbench
    src/mpi_netbench.cpp
)

add_executable(wos_mpi_filebench
    src/mpi_filebench.cpp
)

add_executable(wos_mpi_mandelbench
    src/mpi_mandelbench.cpp
    ${MANDELBENCH_ROOT}/util.cpp
    ${MANDELBENCH_ROOT}/lodepng.cpp
)

add_executable(wos_mpi_renderbench
    ${RENDERBENCH_ROOT}/main.cpp
    ${RENDERBENCH_ROOT}/render_core.cpp
    ${MANDELBENCH_ROOT}/lodepng.cpp
)

target_compile_features(wos_mpi_netbench PRIVATE cxx_std_20)
target_link_libraries(wos_mpi_netbench PRIVATE MPI::MPI_CXX)
target_compile_features(wos_mpi_filebench PRIVATE cxx_std_20)
target_link_libraries(wos_mpi_filebench PRIVATE MPI::MPI_CXX)
target_compile_features(wos_mpi_mandelbench PRIVATE cxx_std_23)
target_include_directories(wos_mpi_mandelbench PRIVATE
    ${WOS_WORKSPACE_ROOT}/modules/testprog/src
)
target_link_libraries(wos_mpi_mandelbench PRIVATE MPI::MPI_CXX)
target_compile_features(wos_mpi_renderbench PRIVATE cxx_std_23)
target_compile_definitions(wos_mpi_renderbench PRIVATE TRACEBENCH_ENABLE_MPI=1)
target_include_directories(wos_mpi_renderbench PRIVATE
    ${RENDERBENCH_ROOT}
    ${WOS_WORKSPACE_ROOT}/modules/testprog/src
)
target_link_libraries(wos_mpi_renderbench PRIVATE MPI::MPI_CXX Threads::Threads)
