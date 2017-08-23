dnl Process this file with autoconf to produce a configure script.

AC_INIT(install-sh)
AC_PREREQ([2.50])
AC_CONFIG_HEADER(config.h)

AC_SUBST(ZLIB_TARGETS)
AC_SUBST(ZLIB_PLTARGETS)

m4_include([../ac_swi_c.m4])

AC_CHECK_HEADERS(zlib.h zutil.h)
AC_CHECK_LIB(z, zlibVersion,
	     LIBS="$LIBS -lz"
	     ZLIB=yes,
	     ZLIB=no)

if test "$ZLIB" = yes; then
  ZLIB_TARGETS="zlib4pl.$SO"
  ZLIB_PLTARGETS=zlib.pl
else
  ZLIB_TARGETS=nolib
  cat << EOF
##################################################################
# ERROR: Could not find library zlib (-lz).  Dropped library(zlib)
# Library zlib is available from http://www.zlib.net/
# Most Unix/Linux distributions are shipped with binaries.  Make
# sure to have the development library installed.
##################################################################
EOF
fi

AC_OUTPUT(Makefile)
