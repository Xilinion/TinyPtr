function(build_CLHT)

    include(ExternalProject)

    ExternalProject_Add(
        ssmem
        URL https://github.com/LPD-EPFL/ssmem/archive/68f0dad3e1be58e2d845f3e028039edb448812f0.zip
        URL_HASH SHA256=42abd1d6a21d36aa72e0bf4325f32cd2b8f1bbbed443263ca69d297c41205b52
        PREFIX ${CMAKE_BINARY_DIR}/ssmem
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} libssmem.a
        INSTALL_COMMAND ""
        CONFIGURE_COMMAND ""
    )

    ExternalProject_Get_Property(ssmem BINARY_DIR)
    ExternalProject_Get_Property(ssmem SOURCE_DIR)

    add_library(ssmem_lib STATIC IMPORTED)
    set_target_properties(ssmem_lib PROPERTIES
        IMPORTED_LOCATION ${BINARY_DIR}/libssmem.a
        INTERFACE_INCLUDE_DIRECTORIES ${SOURCE_DIR}
    )


    ExternalProject_Add(
        sspfd
        URL https://github.com/trigonak/sspfd/archive/b65ff418379b110ec3fd2e027fb0a522b13ca0f6.zip
        URL_HASH SHA256=e0cb2c54b45b73d3619406120dc85f96981c1024eb9e7e851bcbfb25148ecee9
        PREFIX ${CMAKE_BINARY_DIR}/sspfd
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM}
        INSTALL_COMMAND ""
        CONFIGURE_COMMAND ""
    )

    ExternalProject_Get_Property(sspfd BINARY_DIR)
    ExternalProject_Get_Property(sspfd SOURCE_DIR)

    add_library(sspfd_lib STATIC IMPORTED)
    set_target_properties(sspfd_lib PROPERTIES
        IMPORTED_LOCATION ${BINARY_DIR}/libsspfd.a
        INTERFACE_INCLUDE_DIRECTORIES ${SOURCE_DIR}
    )

    ExternalProject_Add(
        CLHT
        URL https://github.com/LPD-EPFL/CLHT/archive/b86ea2c6e6eb178d8d0b0abc38c1ce4386c4ad94.zip
        URL_HASH SHA256=9996b9b6945a7e2b931f03610112637d8154c72adce5b2769563129ccddcb071
        PREFIX ${CMAKE_BINARY_DIR}/CLHT
        BUILD_IN_SOURCE 1
        BUILD_COMMAND
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/ssmem/src/ssmem/libssmem.a <BINARY_DIR>/external/lib/libssmem.a
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/ssmem/src/ssmem/include/ssmem.h <BINARY_DIR>/external/include/ssmem.h
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/sspfd/src/sspfd/libsspfd.a <BINARY_DIR>/external/lib/libsspfd.a
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/sspfd/src/sspfd/sspfd.h <BINARY_DIR>/external/include/sspfd.h
        COMMAND ${CMAKE_COMMAND} -E echo "#define __GNUC_MINOR__ 3" | cat - <BINARY_DIR>/include/clht.h > temp && mv temp <BINARY_DIR>/include/clht.h
        # COMMAND /bin/sh -c "sed -i 's/^CFLAGS = .*/CFLAGS = -D__GNUC_MINOR__=3 -D_GNU_SOURCE/' <BINARY_DIR>/Makefile"
        COMMAND ${CMAKE_COMMAND} -E copy <BINARY_DIR>/include/clht.h <BINARY_DIR>/clht.h
        # COMMAND ${CMAKE_MAKE_PROGRAM} libclht_lb_res.a
        # COMMAND ${CMAKE_MAKE_PROGRAM} dependencies
        # COMMAND ${CMAKE_MAKE_PROGRAM} all
        COMMAND ${CMAKE_MAKE_PROGRAM} libclht_lb_res.a
        INSTALL_COMMAND ""
        CONFIGURE_COMMAND ""
    )

    ExternalProject_Get_Property(CLHT BINARY_DIR)
    ExternalProject_Get_Property(CLHT SOURCE_DIR)

    add_library(CLHT_lib STATIC IMPORTED)
    add_dependencies(CLHT_lib CLHT)

    set_target_properties(CLHT_lib PROPERTIES
        IMPORTED_LOCATION "${BINARY_DIR}/libclht.a"
        INTERFACE_INCLUDE_DIRECTORIES ${SOURCE_DIR}
    )

    add_dependencies(CLHT ssmem)
    add_dependencies(CLHT sspfd)
    add_dependencies(CLHT_lib ssmem_lib sspfd_lib)

    target_link_libraries(CLHT_lib INTERFACE ssmem_lib sspfd_lib)

endfunction()
