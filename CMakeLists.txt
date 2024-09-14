cmake_minimum_required(VERSION 3.20)

project(tinyptr)

set(CMAKE_CXX_STANDARD 17)

# Setting defualt build type to RelWithDebInfo
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include_directories(src)

# Add the source files recursively
file(GLOB_RECURSE SOURCES "src/*.h" "src/*.cpp" "src/*.hpp")

# Test source files, only in test/
file(GLOB_RECURSE TEST_SRC "test/*_test.cpp" "src/*_test.cpp")
file(GLOB_RECURSE DEPRECATED_TEST_SRC "src/*_test.cpp")
list(REMOVE_ITEM SOURCES ${DEPRECATED_TEST_SRC})

include(ExternalProject)

ExternalProject_Add(
    xxHash
    URL https://github.com/Cyan4973/xxHash/archive/3e321b4407318ac1348c0b80fb6fbae8c81ad5fa.zip
    URL_HASH SHA256=649588ecc2a61e06a46fdef263ce7ad42a4b12870d32879a499afe2f89849d2e
    PREFIX ${CMAKE_BINARY_DIR}/xxHash
    CONFIGURE_COMMAND ${CMAKE_COMMAND} -S <SOURCE_DIR>/cmake_unofficial -B <BINARY_DIR> -DBUILD_SHARED_LIBS=OFF
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(xxHash BINARY_DIR)
ExternalProject_Get_Property(xxHash SOURCE_DIR)

add_library(xxHash_lib STATIC IMPORTED)
set_target_properties(xxHash_lib PROPERTIES
    IMPORTED_LOCATION ${BINARY_DIR}/libxxhash.a
    INTERFACE_INCLUDE_DIRECTORIES ${SOURCE_DIR}
)

add_library(tinyptr_lib ${SOURCES})
target_include_directories(tinyptr_lib PRIVATE ${SOURCE_DIR})
target_link_libraries(tinyptr_lib PRIVATE xxHash_lib)

add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE tinyptr_lib xxHash_lib)

add_dependencies(tinyptr_lib xxHash)
add_dependencies(${PROJECT_NAME} xxHash)


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
            xxHash_lib
            GTest::gtest_main
            GTest::gmock
        )
        # target_include_directories(${TEST_EXE} PUBLIC src)
        message(STATUS ${TEST_FILE})
        gtest_discover_tests(${TEST_EXE})
    endforeach()
endif()




