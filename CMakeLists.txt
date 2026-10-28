cmake_minimum_required(VERSION 3.20)
project(ParQ LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)

# Common include directories and compile definitions
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)
add_compile_definitions(KXVER=3)

# Shared library: ParQ (ParQ.so)
add_library(ParQ SHARED
    src/lib/KDBPARQ.cpp
    src/lib/utils.cpp
    src/lib/parquet.cpp
    src/lib/writer.cpp
    src/lib/reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/l64/c.o
)

target_link_libraries(ParQ PRIVATE
    Threads::Threads
    parquet
    arrow
)

set_target_properties(ParQ PROPERTIES
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/install"
)

install(TARGETS ParQ DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/install")

# Test binary: k_handles_test
enable_testing()

add_executable(k_handles_test
    tests/k_handles_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/l64/c.o
)

target_link_libraries(k_handles_test PRIVATE
    Threads::Threads
)

add_test(NAME k_handles_test COMMAND k_handles_test)

# Profiler binary: parq_read_profile
add_executable(parq_read_profile
    tools/parq_read_profile.cpp
    src/lib/utils.cpp
    src/lib/parquet.cpp
    src/lib/reader.cpp
    src/lib/writer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/l64/c.o
)

target_link_libraries(parq_read_profile PRIVATE
    Threads::Threads
    parquet
    arrow
)
