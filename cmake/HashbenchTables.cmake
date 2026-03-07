include(FetchContent)

function(build_hashbench_tables)
    FetchContent_Declare(
        hashbench
        GIT_REPOSITORY https://github.com/koeppl/hashbench.git
        GIT_TAG 2c769579c06b59d2b30775ba670c603dfaa94d42
        GIT_SUBMODULES ""
        GIT_SUBMODULES_RECURSE ON
    )

    FetchContent_GetProperties(hashbench)
    if(NOT hashbench_POPULATED)
        message(STATUS "Downloading hashbench ${hashbench_GIT_TAG}")
        FetchContent_Populate(hashbench)
        execute_process(
            COMMAND git submodule update --init --recursive
            WORKING_DIRECTORY ${hashbench_SOURCE_DIR}
        )
    endif()

    add_subdirectory(
        "${hashbench_SOURCE_DIR}/external/glog"
        "${CMAKE_BINARY_DIR}/hashbench/glog"
        EXCLUDE_FROM_ALL)

    add_library(hashbench_tables INTERFACE)

    target_include_directories(hashbench_tables INTERFACE
        "${hashbench_SOURCE_DIR}/external/separate_chaining/include"
        "${hashbench_SOURCE_DIR}/external/separate_chaining/external/bit_span/include"
        "${hashbench_SOURCE_DIR}/external/broadwordsearch/include"
        "${hashbench_SOURCE_DIR}/external/compact_sparse_hash/include"
        "${hashbench_SOURCE_DIR}/external/compact_sparse_hash/submodules/bit_span/include"
        "${hashbench_SOURCE_DIR}/external/tudostats/include"
        "${hashbench_SOURCE_DIR}/external/glog/src"
    )

    target_compile_definitions(hashbench_tables INTERFACE STATS_DISABLED=1)
    target_link_libraries(hashbench_tables INTERFACE glog::glog)
    set_target_properties(hashbench_tables PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)
endfunction()

