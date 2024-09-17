function(build_xxHash)

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

endfunction()
