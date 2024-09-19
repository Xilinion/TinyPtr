function(build_iceberg)


    include(ExternalProject)

    # Add ExternalProject for iceberg
    ExternalProject_Add(
        iceberg
        URL https://github.com/splatlab/iceberghashtable/archive/a502848e3872a7b59a32f401bea963922e2c3146.zip
        URL_HASH SHA256=86200bf9480397eaff708b876d409da60ad3c2166ad32c7848ef154b98681065
        PREFIX ${CMAKE_BINARY_DIR}/iceberg
        BUILD_IN_SOURCE 1
        BUILD_COMMAND
        COMMAND make CC=gcc CPP=g++ all
        COMMAND ${CMAKE_COMMAND} -E env sh -c "cp <BINARY_DIR>/obj/*.o <BINARY_DIR>/"
        COMMAND ${CMAKE_COMMAND} -E env sh -c "ar -rs libiceberg.a <BINARY_DIR>/obj/*.o"
        INSTALL_COMMAND ""
        CONFIGURE_COMMAND ""
    )

    ExternalProject_Get_Property(iceberg SOURCE_DIR BINARY_DIR)


    add_library(iceberg_lib STATIC IMPORTED)
    add_dependencies(iceberg_lib iceberg)

    set_target_properties(iceberg_lib PROPERTIES
        IMPORTED_LOCATION ${BINARY_DIR}/libiceberg.a
        INTERFACE_INCLUDE_DIRECTORIES ${SOURCE_DIR}
    )

endfunction()
