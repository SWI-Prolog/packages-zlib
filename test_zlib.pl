:- module(test_zlib,
          [ test_zlib/0
          ]).
:- asserta(user:file_search_path(foreign, '.')).
:- asserta(user:file_search_path(foreign, '../clib')).
:- asserta(user:file_search_path(library, '.')).
:- asserta(user:file_search_path(library, '../plunit')).
:- asserta(user:file_search_path(library, '../clib')).

:- use_module(library(zlib)).
:- use_module(library(plunit)).
:- use_module(library(readutil)).
:- use_module(library(socket)).
:- use_module(library(debug)).

test_zlib :-
    run_tests([ zlib
              ]).

reference_file(Name, Path) :-
    source_file(test_zlib, MyFile),
    file_directory_name(MyFile, MyDir),
    atomic_list_concat([MyDir, tests, Name], /, Path).

:- begin_tests(zlib).

%       gunzip: can we read a file compressed with gzip

test(gunzip_ascii) :-
    reference_file('ascii-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn, [type(text), encoding(ascii)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 127, ReferenceCodes),
    Codes == ReferenceCodes.

test(gunzip_utf8) :-
    reference_file('utf8-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn, [type(text), encoding(utf8)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 2047, ReferenceCodes),
    Codes == ReferenceCodes.

test(gunzip_binary) :-
    reference_file('binary-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn, [type(binary)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 255, ReferenceCodes),
    Codes == ReferenceCodes.

test(gunzip_empty) :-
    reference_file('empty-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    Codes == [].

test(gunzip_low_compression) :-
    reference_file('low-compression.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 127, ReferenceCodes),
    Codes == ReferenceCodes.

test(gunzip_high_compression) :-
    reference_file('high-compression.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 127, ReferenceCodes),
    Codes == ReferenceCodes.

test(gunzip_multipart) :-
    reference_file('multipart-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    atom_codes('Part1\nPart2\n', Codes).

test(gunzip_eof) :-
    reference_file('ascii-file.gz', ReferenceFile),
    gzopen(ReferenceFile, read, ZIn),
    call_cleanup(eof_read_codes(ZIn, Codes), close(ZIn)),
    numlist(0, 127, ReferenceCodes),
    Codes == ReferenceCodes.

eof_read_codes(In, List) :-
    (   at_end_of_stream(In)
    ->  List = []
    ;   get_code(In, C),
        List = [C|Rest],
        eof_read_codes(In, Rest)
    ).



%       gzip: can we write (and read back) a compressed file

test(gzip_ascii,
     [ cleanup(delete_file('plunit-tmp.gz'))
     ]) :-
    numlist(0, 127, ReferenceCodes),
    gzopen('plunit-tmp.gz', write, ZOut, [type(text), encoding(ascii)]),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    gzopen('plunit-tmp.gz', read, ZIn, [type(text)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    Codes = ReferenceCodes.

test(gzip_utf8,
     [ cleanup(delete_file('plunit-tmp.gz'))
     ]) :-
    numlist(0, 2047, ReferenceCodes),
    gzopen('plunit-tmp.gz', write, ZOut, [type(text), encoding(utf8)]),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    gzopen('plunit-tmp.gz', read, ZIn, [type(text), encoding(utf8)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    Codes = ReferenceCodes.

test(gzip_binary,
     [ cleanup(delete_file('plunit-tmp.gz'))
     ]) :-
    numlist(0, 255, ReferenceCodes),
    gzopen('plunit-tmp.gz', write, ZOut, [type(binary)]),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    gzopen('plunit-tmp.gz', read, ZIn, [type(binary)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    Codes = ReferenceCodes.

test(gzip_empty,
     [ cleanup(delete_file('plunit-tmp.gz'))
     ]) :-
    gzopen('plunit-tmp.gz', write, ZOut),
    close(ZOut),
    gzopen('plunit-tmp.gz', read, ZIn, [type(text)]),
    call_cleanup(read_stream_to_codes(ZIn, Codes1), close(ZIn)),
    Codes1 = [].

test(gzip_multipart,
     [ cleanup(delete_file('plunit-tmp.gz'))
     ]) :-
    gzopen('plunit-tmp.gz', write, ZOut1),
    format(ZOut1, 'Part1\n', []),
    close(ZOut1),
    gzopen('plunit-tmp.gz', append, ZOut2),
    format(ZOut2, 'Part2\n', []),
    close(ZOut2),
    gzopen('plunit-tmp.gz', read, ZIn),
    call_cleanup(read_stream_to_codes(ZIn, Codes), close(ZIn)),
    atom_codes('Part1\nPart2\n', Codes).



%       deflate: test read/write of deflate format

test(deflate_ascii,
     [ cleanup(delete_file('plunit-tmp.z'))
     ]) :-
    numlist(0, 127, ReferenceCodes),
    open('plunit-tmp.z', write, Out, [type(text), encoding(ascii)]),
    zopen(Out, ZOut, []),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    open('plunit-tmp.z', read, In, [type(text), encoding(ascii)]),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == ReferenceCodes.

test(deflate_utf8,
     [ cleanup(delete_file('plunit-tmp.z'))
     ]) :-
    numlist(0, 2047, ReferenceCodes),
    open('plunit-tmp.z', write, Out, [type(text), encoding(utf8)]),
    zopen(Out, ZOut, []),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    open('plunit-tmp.z', read, In, [type(text), encoding(utf8)]),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == ReferenceCodes.

test(deflate_binary,
     [ cleanup(delete_file('plunit-tmp.z'))
     ]) :-
    numlist(0, 255, ReferenceCodes),
    open('plunit-tmp.z', write, Out, [type(binary)]),
    zopen(Out, ZOut, []),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    open('plunit-tmp.z', read, In, [type(binary)]),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == ReferenceCodes.

test(deflate_empty,
     [ cleanup(delete_file('plunit-tmp.z'))
     ]) :-
    open('plunit-tmp.z', write, Out),
    zopen(Out, ZOut, []),
    close(ZOut),
    open('plunit-tmp.z', read, In),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == [].

test(deflate_low_compression) :-
    numlist(0, 127, ReferenceCodes),
    open('plunit-tmp.z', write, Out),
    zopen(Out, ZOut, [level(0)]),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    open('plunit-tmp.z', read, In),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == ReferenceCodes.

test(deflate_high_compression) :-
    numlist(0, 127, ReferenceCodes),
    open('plunit-tmp.z', write, Out),
    zopen(Out, ZOut, [level(9)]),
    format(ZOut, '~s', [ReferenceCodes]),
    close(ZOut),
    open('plunit-tmp.z', read, In),
    zopen(In, ZIn, []),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    Codes == ReferenceCodes.

test(deflate_multipart,
     [ cleanup(delete_file('plunit-tmp.z'))
     ]) :-
    open('plunit-tmp.z', write, Out),
    zopen(Out, ZOut1, [close_parent(false)]),
    format(ZOut1, 'Part1\n', []),
    close(ZOut1),
    zopen(Out, ZOut2, []),
    format(ZOut2, 'Part2\n', []),
    close(ZOut2),
    open('plunit-tmp.z', read, In),
    zopen(In, ZIn, [multi_part(true)]),
    read_stream_to_codes(ZIn, Codes),
    close(ZIn),
    atom_codes('Part1\nPart2\n', Codes).



%       zstream: test compressed stream flushing and processing

test(zstream, Exit == true) :-
    server(Port),
    debug(server, 'Server at ~w~n', [Port]),
    client(Port),
    thread_join(server, Exit).

server(Port) :-
    tcp_socket(S),
    tcp_bind(S, Port),
    tcp_listen(S, 5),
    tcp_open_socket(S, AcceptFd, _),
    thread_create(process(AcceptFd), _, [alias(server)]).

process(AcceptFd) :-
    tcp_accept(AcceptFd, S2, _Peer),
    tcp_open_socket(S2, ZIn, ZOut),
    zopen(ZIn, In, []),
    zopen(ZOut, Out, []),
    loop(In, Out),
    read(In, X),
    assertion(X==end_of_file),
    close(In), close(Out).

loop(In, Out) :-
    read(In, Term),
    debug(server, 'Read ~w', [Term]),
    (   Term == quit
    ->  true
    ;   format(Out, '~q.~n', [Term]),
        flush_output(Out),
        debug(server, 'Replied', [Term]),
        loop(In, Out)
    ).

client(Port) :-
    integer(Port),
    !,
    client(localhost:Port).
client(Address) :-
    tcp_socket(S),
    tcp_connect(S, Address),
    tcp_open_socket(S, ZIn, ZOut),
    zopen(ZIn, In, []),
    zopen(ZOut, Out, []),
    process_client(In, Out),
    close(Out),
    debug(server, 'Client: closed Out', []),
    read(In, X),
    assertion(X==end_of_file),
    close(In).

process_client(In, Out) :-
    forall(between(0, 50, X),
           (   format(Out, '~q.~n', [X]),
               flush_output(Out),
               read(In, Term),
               debug(server, 'Client: got ~q', [Term]),
               (   X == Term
               ->  true
               ;   format('Wrong reply~n'),
                   fail
               )
           )),
    format(Out, 'quit.~n', []),
    flush_output(Out),
    debug(server, 'Client: sent quit', []).


                 /*******************************
                 *            BIG DATA          *
                 *******************************/

test(big) :-
    forall(between(1, 5, I),
           (   Max is 10**I,
               big(_, Max))).

big(Port, N):-
    tcp_socket(SockFd),
    tcp_setopt(SockFd, reuseaddr),
    tcp_bind(SockFd, Port),
    tcp_listen(SockFd, 5),
    thread_create(client_test(Port, N), Client, []),
    tcp_accept(SockFd, ClientFd, _Peer),
    tcp_open_socket(ClientFd, InStream, OutStream),
    zopen(OutStream, ZOut, [close_parent(false), format(deflate)]),
    send_data(1, N, ZOut),
    close(InStream),
    character_count(ZOut, RawCnt),
    close(ZOut),
    character_count(OutStream, CompressedCnt),
    debug(zlib, 'compressed ~d into ~d bytes~n',
          [RawCnt, CompressedCnt]),
    close(OutStream),
    tcp_close_socket(SockFd),
    thread_join(Client, Status),
    assertion(Status == true).

send_data(I, N, ZOut) :-
    I =< N,
    !,
    format(ZOut, '~d.~n', [I]),
    I2 is I + 1,
    send_data(I2, N, ZOut).
send_data(_, _, _).


client_test(Port, N) :-
    tcp_socket(SockFd),
    tcp_connect(SockFd, localhost:Port),
    tcp_open_socket(SockFd, In, Out),
    zopen(In, ZIn, [format(deflate)]),
    get_data(ZIn, N),
    close(ZIn),
    close(Out).

get_data(ZIn, _) :-
    debugging(data),
    !,
    between(0, inf, X),
    get_byte(ZIn, C),
    (   C == -1
    ->  !,
        format('EOF at ~w~n', [X])
    ;   put_byte(C),
        fail
    ).
get_data(ZIn, N) :-
    between(1, inf, X),
    read(ZIn, Term),
    (   Term == end_of_file
    ->  !,
        assertion(X =:= N + 1)
    ;   assertion(Term == X),
        fail
    ).

:- end_tests(zlib).
