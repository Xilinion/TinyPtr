function(build_junction)
    include(FetchContent)

    if(DISABLE_RESIZING)
        set(JUNCTION_REPO "https://github.com/BlackJackTone/junction.git")
        set(JUNCTION_TAG "a89f8bc7f6b5b6cc775d8b49701f2e7d159caea5")
    else()
        set(JUNCTION_REPO "https://github.com/preshing/junction.git")
        set(JUNCTION_TAG "5ad3be7ce1d3f16b9f7ed6065bbfeacd2d629a08")
    endif()
    
    # Set up the repositories
    FetchContent_Declare(
        turf
        GIT_REPOSITORY https://github.com/preshing/turf.git
        GIT_TAG 9ae0d4b984fa95ed5f823274b39c87ee742f6650
    )
    
    FetchContent_Declare(
        junction
        GIT_REPOSITORY ${JUNCTION_REPO}
        GIT_TAG ${JUNCTION_TAG}
    )
    
    # Download the repositories during configuration
    FetchContent_GetProperties(turf)
    if(NOT turf_POPULATED)
        message(STATUS "Downloading Turf...")
        FetchContent_Populate(turf)
    endif()
    
    FetchContent_GetProperties(junction)
    if(NOT junction_POPULATED)
        message(STATUS "Downloading Junction...")
        FetchContent_Populate(junction)
    endif()
    
    # Make sure turf is in the correct relative location to junction
    # Junction expects turf to be in a ../turf directory relative to itself
    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/junction_combined)
    
    # Check if turf directory exists at junction_combined/turf
    if(NOT EXISTS ${CMAKE_BINARY_DIR}/junction_combined/turf)
        # Create a symbolic link to ensure proper directory structure
        file(CREATE_LINK ${turf_SOURCE_DIR} ${CMAKE_BINARY_DIR}/junction_combined/turf SYMBOLIC)
    endif()
    
    # Check if junction directory exists at junction_combined/junction
    if(NOT EXISTS ${CMAKE_BINARY_DIR}/junction_combined/junction)
        # Create a symbolic link to ensure proper directory structure
        file(CREATE_LINK ${junction_SOURCE_DIR} ${CMAKE_BINARY_DIR}/junction_combined/junction SYMBOLIC)
    endif()
    
    # Patch Junction CMakeLists.txt to use modern CMake version
    file(READ ${CMAKE_BINARY_DIR}/junction_combined/junction/CMakeLists.txt JUNCTION_CMAKE_CONTENT)
    string(REPLACE "cmake_minimum_required(VERSION 2.8.5)" "cmake_minimum_required(VERSION 3.5)" JUNCTION_CMAKE_CONTENT "${JUNCTION_CMAKE_CONTENT}")
    file(WRITE ${CMAKE_BINARY_DIR}/junction_combined/junction/CMakeLists.txt "${JUNCTION_CMAKE_CONTENT}")
    
    # Patch Turf CMakeLists.txt to use modern CMake version
    file(READ ${CMAKE_BINARY_DIR}/junction_combined/turf/CMakeLists.txt TURF_CMAKE_CONTENT)
    string(REPLACE "cmake_minimum_required(VERSION 2.8.5)" "cmake_minimum_required(VERSION 3.5)" TURF_CMAKE_CONTENT "${TURF_CMAKE_CONTENT}")
    file(WRITE ${CMAKE_BINARY_DIR}/junction_combined/turf/CMakeLists.txt "${TURF_CMAKE_CONTENT}")
    
    # Add Junction as a subdirectory
    add_subdirectory(${CMAKE_BINARY_DIR}/junction_combined/junction ${CMAKE_BINARY_DIR}/junction_combined/junction_build)
    
    # Create a junction_lib target for consistent linking format
    add_library(junction_lib INTERFACE)
    
    if(DEFINED JUNCTION_ALL_INCLUDE_DIRS)
        target_include_directories(junction_lib INTERFACE ${JUNCTION_ALL_INCLUDE_DIRS})
    else()
        # Fallback in case JUNCTION_ALL_INCLUDE_DIRS isn't defined
        target_include_directories(junction_lib INTERFACE 
            ${junction_SOURCE_DIR}
            ${turf_SOURCE_DIR}
        )
    endif()
    
    if(DEFINED JUNCTION_ALL_LIBRARIES)
        target_link_libraries(junction_lib INTERFACE ${JUNCTION_ALL_LIBRARIES})
    endif()
endfunction()