function(build_cuckoo)

    include(ExternalProject)

    if(DISABLE_RESIZING)
        message(STATUS "Building Cuckoo with NORESIZE")
        set(CUCKOO_REPO "https://github.com/BlackJackTone/libcuckoo.git")
        set(CUCKOO_TAG "3311ad91910910fa799dae89c437dbd04475546d")
        # Use GIT_REPOSITORY for git repos
        ExternalProject_Add(
            cuckoo
            GIT_REPOSITORY ${CUCKOO_REPO}
            GIT_TAG ${CUCKOO_TAG}
            PREFIX ${CMAKE_BINARY_DIR}/cuckoo
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
            BUILD_IN_SOURCE 1
            BUILD_COMMAND ${CMAKE_COMMAND} --build . --target install
        )
    else()
        message(STATUS "Building Cuckoo with RESIZE")
        set(CUCKOO_URL "https://github.com/efficient/libcuckoo/archive/88bb6e2ca1b68572f66e3e2bea071829652afeb2.zip")
        set(CUCKOO_HASH "34a94b59737705ed39effb64081c23e9c4c0f198addf37789ee45445a94c167e")
        # Use URL for zip downloads
        ExternalProject_Add(
            cuckoo
            URL ${CUCKOO_URL}
            URL_HASH SHA256=${CUCKOO_HASH}
            PREFIX ${CMAKE_BINARY_DIR}/cuckoo
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
            BUILD_IN_SOURCE 1
            BUILD_COMMAND ${CMAKE_COMMAND} --build . --target install
        )
    endif()

    ExternalProject_Get_Property(cuckoo INSTALL_DIR)


    add_library(cuckoo_lib INTERFACE)
    target_include_directories(cuckoo_lib INTERFACE ${INSTALL_DIR}/include)

endfunction()
