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

add_library(tinyptr_lib ${SOURCES})


add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE tinyptr_lib)