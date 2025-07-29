cmake_minimum_required(VERSION 3.20)

project(tinyptr)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

# Add the source files recursively
file(GLOB_RECURSE SOURCES "src/*.h" "src/*.cpp" "src/*.hpp")

# Test source files, only in test/
file(GLOB_RECURSE TEST_SRC "test/*_test.cpp" "src/*_test.cpp")
file(GLOB_RECURSE DEPRECATED_TEST_SRC "src/*_test.cpp")
list(REMOVE_ITEM SOURCES ${DEPRECATED_TEST_SRC})

add_library(tinyptr_lib ${SOURCES})
target_include_directories(tinyptr_lib PRIVATE ${SOURCE_DIR})
target_compile_options(tinyptr_lib INTERFACE -march=native -flto)

# Add preprocessor definition based on resizing option
if(DISABLE_RESIZING)
    target_compile_definitions(tinyptr_lib PUBLIC CUCKOO_DISABLE_RESIZING)
endif()

add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE tinyptr_lib)

# Add external dependencies
include(CLHT)
build_CLHT()
add_dependencies(tinyptr_lib CLHT)
target_link_libraries(tinyptr_lib PUBLIC CLHT_lib)

include(cuckoo)
build_cuckoo()
add_dependencies(tinyptr_lib cuckoo)
target_link_libraries(tinyptr_lib PUBLIC cuckoo_lib pthread)

include(iceberg)
build_iceberg()
add_dependencies(tinyptr_lib iceberg)
target_link_libraries(tinyptr_lib PUBLIC iceberg_lib ssl crypto pthread)

# include(growt)
# build_growt()
# add_dependencies(tinyptr_lib growt)
# target_link_libraries(tinyptr_lib PUBLIC growt_lib pthread)

include(junction)
build_junction()
add_dependencies(tinyptr_lib junction)
target_link_libraries(tinyptr_lib PUBLIC junction_lib)

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
            tinyptr_lib
            GTest::gtest_main
            GTest::gmock
        )

        # target_include_directories(${TEST_EXE} PUBLIC src)
        message(STATUS ${TEST_FILE})
        gtest_discover_tests(${TEST_EXE})
    endforeach()
endif()
