cmake_minimum_required(VERSION 2.8.12)
project(swipl-zlib)

include("../cmake/PrologPackage.cmake")

find_package(ZLIB)
if(ZLIB_FOUND)

swipl_plugin(
    zlib
    MODULE zlib4pl
    C_SOURCES zlib4pl.c
    C_LIBS ${ZLIB_LIBRARIES}
    PL_LIBS zlib.pl)
target_include_directories(
    plugin_zlib4pl BEFORE PRIVATE
    ${ZLIB_INCLUDE_DIR})

test_libs(
    zlib
    PACKAGES clib plunit)

pkg_doc(zlib)

endif(ZLIB_FOUND)
