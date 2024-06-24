cmake_minimum_required(VERSION 3.20)

project(tinyptr)

set(CMAKE_CXX_STANDARD 17)

# Setting defualt build type to RelWithDebInfo
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Add the source files recursively
file(GLOB_RECURSE SOURCES "src/*.h" "src/*.cpp")

# Test source files, only in test/
file(GLOB_RECURSE TEST_SRC "test/*_test.cpp")
file(GLOB_RECURSE DEPRECATED_TEST_SRC "src/*_test.cpp")
list(REMOVE_ITEM SOURCES ${DEPRECATED_TEST_SRC})

add_library(tinyptr_lib ${SOURCES})


add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE tinyptr_lib)

if (BUILD_TESTING)
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

    foreach (TEST_FILE ${TEST_SRC})
        cmake_path(GET TEST_FILE STEM TEST_EXE)
        add_executable(${TEST_EXE} ${TEST_FILE})
        target_link_libraries(
                ${TEST_EXE}
                tinyptr_lib
        )
        message(STATUS ${TEST_FILE})
        gtest_discover_tests(${TEST_EXE})
    endforeach ()
endif ()
