function(build_iceberg)
    include(ExternalProject)

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(ICEBERG_DEBUG_DEFINES "D=1 NO_AVX=1")
    else()
        set(ICEBERG_DEBUG_DEFINES "")
    endif()

    # Set NORESIZE based on DISABLE_RESIZING option
    if(DISABLE_RESIZING)
        set(ICEBERG_RESIZE_FLAG "NORESIZE=1")
        set(ICEBERG_INTERFACE "ENABLE_BLOCK_LOCKING")
    else()
        set(ICEBERG_RESIZE_FLAG "")
        set(ICEBERG_INTERFACE "ENABLE_RESIZE;ENABLE_BLOCK_LOCKING")  # Use semicolon for list
    endif()

    # Add ExternalProject for iceberg
    ExternalProject_Add(
        iceberg
        URL https://github.com/Xilinion/iceberghashtable/archive/8e053f3c3051a1b904c77e139badd7597b2e8251.zip
        URL_HASH SHA256=ab252570c4aedc75f901f3a178c0b1ccbcf99507d95e443652b1da8f1d3d6eec
        PREFIX ${CMAKE_BINARY_DIR}/iceberg
        BUILD_IN_SOURCE 1
        BUILD_COMMAND
        COMMAND ${CMAKE_MAKE_PROGRAM} ${ICEBERG_RESIZE_FLAG} CC=gcc CPP=g++ ${ICEBERG_DEBUG_DEFINES} main
        COMMAND ${CMAKE_COMMAND} -E env sh -c "cp <SOURCE_DIR>/include/* <SOURCE_DIR>/"
        COMMAND ${CMAKE_COMMAND} -E env sh -c "cp <BINARY_DIR>/obj/*.o <BINARY_DIR>/"
        COMMAND ${CMAKE_COMMAND} -E env sh -c "gcc-ar -rs libiceberg.a <BINARY_DIR>/obj/*.o"
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

    target_compile_definitions(iceberg_lib INTERFACE ${ICEBERG_INTERFACE})
    target_compile_options(iceberg_lib INTERFACE -march=native)
endfunction()
