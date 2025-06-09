function(build_growt)
    include(ExternalProject)

    ExternalProject_Add(
        growt
        GIT_REPOSITORY https://github.com/TooBiased/growt.git
        GIT_TAG master
        GIT_SUBMODULES ""
        GIT_SUBMODULES_RECURSE ON
        PREFIX ${CMAKE_BINARY_DIR}/growt
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>

        # -DGROWT_BUILD_ALL_THIRD_PARTIES=ON
        BUILD_IN_SOURCE 0
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        UPDATE_COMMAND git submodule update --init --recursive
        INSTALL_COMMAND ""
    )

    ExternalProject_Get_Property(growt SOURCE_DIR)

    add_library(growt_lib INTERFACE)
    target_include_directories(growt_lib INTERFACE ${SOURCE_DIR})
endfunction()