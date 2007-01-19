################################################################
# Build the SWI-Prolog zlib package for MS-Windows
#
# Author: Jan Wielemaker
# 
# Use:
#	nmake /f Makefile.mak
#	nmake /f Makefile.mak install
################################################################

PLHOME=..\..
!include $(PLHOME)\src\rules.mk
CFLAGS=$(CFLAGS) /D__SWI_PROLOG__

!IF "$(MD)" == "WIN64"
ZLIB=ZLIBWAPI
CFLAGS=$(CFLAGS) /DZLIB_WINAPI
!ELSE
ZLIB=ZLIB1
!ENDIF

OBJ=		zlib4pl.obj

all:		zlib4pl.dll

zlib4pl.dll:	$(OBJ)
		$(LD) /dll /out:$@ $(LDFLAGS) $(OBJ) $(ZLIB).lib $(PLLIB) $(LIBS)

!IF "$(CFG)" == "rt"
install:	idll
!ELSE
install:	idll ilib
!ENDIF

################################################################
# Testing
################################################################

check::

################################################################
# Installation
################################################################

idll::
		copy "$(EXTRALIBDIR)\$(ZLIB).dll" "$(BINDIR)"
		copy zlib4pl.dll "$(BINDIR)"
!IF "$(PDB)" == "true"
		copy zlib4pl.pdb "$(BINDIR)"
!ENDIF

ilib::
		copy zlib.pl "$(PLBASE)\library"
		$(MAKEINDEX)

uninstall::
		del "$(BINDIR)\zlib4pl.dll"
		del "$(PLBASE)\library\zlib.pl"
		$(MAKEINDEX)

html-install::
		copy zlib.html "$(PKGDOC)"

xpce-install::

clean::
		if exist *.obj del *.obj
		if exist *~ del *~

distclean:	clean
		-DEL *.dll *.lib *.exp *.ilk *.pdb 2>nul


