/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2011, University of Amsterdam
			      VU University Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
*/

:- module(zlib,
	  [ zopen/3,			% +Stream, -ZStream, +Option
	    gzopen/3,			% +File, +Mode, -Stream
	    gzopen/4			% +File, +Mode, -Stream, +Options
	  ]).
:- use_module(library(shlib)).
:- use_module(library(error)).


/** <module> Zlib wrapper for SWI-Prolog

Read/write compressed data based on the zlib library.

@author Jan Wielemaker
@see	http://www.zlib.net/
@see	http://www.swi-prolog.org/packages/zlib.html
*/

:- use_foreign_library(foreign(zlib4pl)).
:- public zdebug/1.			% Set debug level

%%	gzopen(+File, +Mode, -Stream) is det.
%%	gzopen(+File, +Mode, -Stream, +Options) is det.
%
%	Open a file compatible with the  gzip   program.  Note that if a
%	file is opened in =append= mode,  a   second  gzip image will be
%	added to the end of the file.
%
%	@tbd	Later versions may actually append to an existing
%		compressed file.

gzopen(File, Mode, Stream) :-
	gzopen(File, Mode, Stream, []).

gzopen(File, Mode, Stream, Options) :-
	must_be(oneof([read,write,append]), Mode),
	zoptions(Options, ZOptions, OpenOptions),
	open(File, Mode, Stream0, OpenOptions),
	zopen(Stream0, Stream,
	      [ format(gzip),
		close_parent(true)
	      | ZOptions
	      ]).

zoptions([], [], []).
zoptions([H|T], [H|TZ], TO) :-
	zoption(H), !,
	zoptions(T, TZ, TO).
zoptions([H|T], TZ, [H|TO]) :-
	zoptions(T, TZ, TO).

zoption(format(_)).
zoption(level(_)).
