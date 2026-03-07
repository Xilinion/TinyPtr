function(build_third_party_tinypointers)
    include(FetchContent)

    # Fetch the third-party TinyPointers library
    FetchContent_Declare(
        third_party_tinypointers
        GIT_REPOSITORY https://github.com/rodrigch18/TinyPointers.git
        GIT_TAG main
    )

    FetchContent_MakeAvailable(third_party_tinypointers)

    # Get the source directory
    set(TINYPTR_SOURCE_DIR ${third_party_tinypointers_SOURCE_DIR})

    # Collect all source files
    set(TINYPTR_SOURCES
        ${TINYPTR_SOURCE_DIR}/src/tiny_ptr_simple.c
        ${TINYPTR_SOURCE_DIR}/src/tiny_ptr_fixed.c
        ${TINYPTR_SOURCE_DIR}/src/tiny_ptr_variable.c
        ${TINYPTR_SOURCE_DIR}/src/tiny_ptr_unified.c
    )

    # Create the static library directly with CMake
    add_library(third_party_tinypointers_lib STATIC ${TINYPTR_SOURCES})

    target_include_directories(third_party_tinypointers_lib PUBLIC 
        ${TINYPTR_SOURCE_DIR}/include
    )

    target_compile_options(third_party_tinypointers_lib PRIVATE 
        -Wall -O2 -pthread -march=native
    )

    # Set C standard
    set_target_properties(third_party_tinypointers_lib PROPERTIES
        C_STANDARD 99
        C_STANDARD_REQUIRED ON
    )

endfunction()
