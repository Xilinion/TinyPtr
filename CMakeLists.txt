cmake_minimum_required(VERSION 3.20)

project(tinyptr)

# Make IPO/LTO behavior consistent in CMake
# cmake_policy(SET CMP0069 NEW) # honors INTERPROCEDURAL_OPTIMIZATION

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Use gcc-ar/gcc-ranlib so static archives with IR are plugin-aware
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    find_program(GCC_AR NAMES gcc-ar)
    find_program(GCC_RANLIB NAMES gcc-ranlib)
    if(GCC_AR AND GCC_RANLIB)
        set(CMAKE_AR "${GCC_AR}" CACHE FILEPATH "" FORCE)
        set(CMAKE_RANLIB "${GCC_RANLIB}" CACHE FILEPATH "" FORCE)
    endif()
endif()

if(COMPILE_FOR_VALGRIND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mno-avx")
endif()

# Setting defualt build type to RelWithDebInfo
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

if(COMPILE_FOR_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")
endif()

option(DISABLE_RESIZING "Whether to disable resizing functionality" ON)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include_directories(src)

# Add the core source files (excluding benchmark)
file(GLOB_RECURSE CORE_SOURCES "src/*.h" "src/*.cpp" "src/*.hpp")
file(GLOB_RECURSE BENCHMARK_SOURCES "src/benchmark/*.h" "src/benchmark/*.cpp" "src/benchmark/*.hpp")
list(REMOVE_ITEM CORE_SOURCES ${BENCHMARK_SOURCES})

# Test source files, only in test/
file(GLOB_RECURSE TEST_SRC "test/*_test.cpp" "src/*_test.cpp")
file(GLOB_RECURSE DEPRECATED_TEST_SRC "src/*_test.cpp")
list(REMOVE_ITEM CORE_SOURCES ${DEPRECATED_TEST_SRC})

# Create benchmark_lib with benchmark sources
add_library(benchmark_lib ${BENCHMARK_SOURCES})
target_include_directories(benchmark_lib PRIVATE ${SOURCE_DIR})
target_compile_options(benchmark_lib PRIVATE -march=native)

# Enable parallel STL with oneTBB backend
find_package(TBB REQUIRED)
target_link_libraries(benchmark_lib PUBLIC TBB::tbb)
target_compile_definitions(benchmark_lib PUBLIC _GLIBCXX_USE_TBB_PAR_BACKEND=1)

# Add preprocessor definition based on resizing option
if(DISABLE_RESIZING)
    target_compile_definitions(benchmark_lib PUBLIC CUCKOO_DISABLE_RESIZING)
endif()

add_executable(${PROJECT_NAME} src/main.cpp)

target_link_options(${PROJECT_NAME}
    PRIVATE
    -flto
    # -Wl,-plugin-opt=O3
    # For introspection/debugging of LTO activity (optional):
    # -Wl,-plugin-opt=save-temps
)

target_link_libraries(${PROJECT_NAME} PRIVATE benchmark_lib)
target_link_libraries(${PROJECT_NAME} PRIVATE TBB::tbb)


# Create tinyptr_lib with only core sources (no external dependencies)
add_library(tinyptr_lib ${CORE_SOURCES})
target_compile_options(tinyptr_lib PRIVATE -march=native -flto)
# set_property(TARGET tinyptr_lib PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
target_link_libraries(benchmark_lib PUBLIC tinyptr_lib)
add_dependencies(benchmark_lib tinyptr_lib)

# Add external dependencies to benchmark_lib (not tinyptr_lib)
include(CLHT)
build_CLHT()
add_dependencies(benchmark_lib CLHT)
target_link_libraries(benchmark_lib PUBLIC CLHT_lib)

include(cuckoo)
build_cuckoo()
add_dependencies(benchmark_lib cuckoo)
target_link_libraries(benchmark_lib PUBLIC cuckoo_lib pthread)

include(iceberg)
build_iceberg()
add_dependencies(benchmark_lib iceberg)
target_link_libraries(benchmark_lib PUBLIC iceberg_lib ssl crypto pthread)

# include(growt)
# build_growt()
# add_dependencies(benchmark_lib growt)
# target_link_libraries(benchmark_lib PUBLIC growt_lib pthread)

include(junction)
build_junction()
add_dependencies(benchmark_lib junction)
target_link_libraries(benchmark_lib PUBLIC junction_lib)

# Add test support
include(FetchContent)

# Options
option(BUILD_TESTING "Whether to enable unit test support" ON)

if(BUILD_TESTING)
    FetchContent_Declare(
        googletest

        # Google recommends using the latest commit
        URL https://github.com/google/googletest/archive/1ed6a8c67a0bd675149ece27bbec0ef1759854cf.zip
    )

    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    include(GoogleTest)

    foreach(TEST_FILE ${TEST_SRC})
        cmake_path(GET TEST_FILE STEM TEST_EXE)
        add_executable(${TEST_EXE} ${TEST_FILE})
        target_link_libraries(
            ${TEST_EXE}
            PUBLIC
            benchmark_lib
            GTest::gtest_main
            GTest::gmock
        )

        # target_include_directories(${TEST_EXE} PUBLIC src)
        message(STATUS ${TEST_FILE})
        gtest_discover_tests(${TEST_EXE})
    endforeach()
endif()
