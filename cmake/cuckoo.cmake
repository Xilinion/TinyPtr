function(build_cuckoo)

    include(ExternalProject)

    ExternalProject_Add(
        cuckoo
        URL https://github.com/efficient/libcuckoo/archive/88bb6e2ca1b68572f66e3e2bea071829652afeb2.zip
        URL_HASH SHA256=34a94b59737705ed39effb64081c23e9c4c0f198addf37789ee45445a94c167e
        PREFIX ${CMAKE_BINARY_DIR}/cuckoo
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${CMAKE_COMMAND} --build . --target install
    )

    ExternalProject_Get_Property(cuckoo INSTALL_DIR)


    add_library(cuckoo_lib INTERFACE)
    target_include_directories(cuckoo_lib INTERFACE ${INSTALL_DIR}/include)

endfunction()
