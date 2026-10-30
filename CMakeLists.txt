cmake_minimum_required(VERSION 3.20)
project(ParQ LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(PARQ_ENABLE_COVERAGE "Enable GCC/gcovr coverage instrumentation" OFF)
option(PARQ_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

if(PARQ_ENABLE_COVERAGE AND PARQ_ENABLE_SANITIZERS)
    message(FATAL_ERROR "Coverage and sanitizers must use separate build directories")
endif()

if(PARQ_ENABLE_COVERAGE OR PARQ_ENABLE_SANITIZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "Coverage and sanitizer options require GCC or Clang")
    endif()
endif()

if(PARQ_ENABLE_COVERAGE)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(FATAL_ERROR "Coverage reporting currently requires GCC and gcovr")
    endif()
    add_compile_options(--coverage -fprofile-abs-path -fprofile-update=atomic)
    add_link_options(--coverage)
endif()

if(PARQ_ENABLE_SANITIZERS)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

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

# End-to-end Parquet tests use c.o to create q values and call ParQ's C API.
add_executable(parquet_roundtrip_test tests/parquet_roundtrip_test.cpp)
target_link_libraries(parquet_roundtrip_test PRIVATE ParQ)
add_test(NAME parquet_roundtrip_multi_row_groups
    COMMAND parquet_roundtrip_test "Multiple row groups, selected columns, and reader advancement")
add_test(NAME parquet_roundtrip_single_row_group
    COMMAND parquet_roundtrip_test "Single row group with all supported example column types")
add_test(NAME parquet_roundtrip_empty_row_group
    COMMAND parquet_roundtrip_test "Empty row group")
add_test(NAME parquet_roundtrip_compression_metadata
    COMMAND parquet_roundtrip_test "Compressed file with key-value metadata")
add_test(NAME parquet_roundtrip_duplicate_columns
    COMMAND parquet_roundtrip_test "Duplicate selected columns")

add_executable(parquet_api_validation_test tests/parquet_api_validation_test.cpp)
target_link_libraries(parquet_api_validation_test PRIVATE ParQ)
add_test(NAME parquet_api_validation_test COMMAND parquet_api_validation_test)

add_executable(parquet_null_column_test tests/parquet_null_column_test.cpp)
target_link_libraries(parquet_null_column_test PRIVATE ParQ parquet arrow)
add_test(NAME parquet_null_column_test COMMAND parquet_null_column_test)

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
