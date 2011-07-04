-module(protobuffs_generic_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).


simple_test() ->
    compile_pb("simple.proto"),
    check_decode_loop(simple_pb, "simple.proto", 'Person',
		      fun() -> {person,string(),string(),string(),sint32(),location()} end,
		      100).

empty_test() ->
    DataGen = fun() -> {empty,default(undefined, real()),
			default(undefined, real()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, uint32()),
			default(undefined, uint64()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, uint32()),
			default(undefined, uint64()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, bool()),
			default(undefined, string()),
			default(undefined, binary())}
	      end,
    compile_pb("empty.proto"),
    check_decode_loop(empty_pb, "empty.proto", 'Empty', DataGen, 100).


default_test() ->
    DataGen = fun() -> {requiredwithdefault,default(undefined, real()),
			default(undefined, real()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, uint32()),
			default(undefined, uint64()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, uint32()),
			default(undefined, uint64()),
			default(undefined, sint32()),
			default(undefined, sint64()),
			default(undefined, bool()),
			default(undefined, string())}
	      end,
    compile_pb("hasdefault.proto"),
    check_decode_loop(hasdefault_pb, "hasdefault.proto", 'RequiredWithDefault', DataGen, 100).
    
%%====================
compile_pb(ProtoFile) ->
    protobuffs_compile:scan_file(protofile_path(ProtoFile)).

check_decode_loop(_Module, _ProtoFile, _MsgName, _DataGen, N) when N==0 ->
    ok;
check_decode_loop(Module, ProtoFile, MsgName, DataGen, N) when N>0 ->
    Data = DataGen(),
    check_decode(Module, ProtoFile, MsgName, Data),
    check_decode_loop(Module, ProtoFile, MsgName, DataGen, N-1).

check_decode(Module, ProtoFile, MsgName, Record) ->
    MsgNameLower = element(1, Record),
    Encoded = Module:encode(Record),
    DecodedA = Module:decode(MsgNameLower, Encoded),
    DecodedB = generic_decode_message(Encoded, ProtoFile, MsgName),
    ?assertEqual({a,DecodedA}, {a,DecodedB}).
     
    

generic_decode_message(Bytes, ProtoFile, MsgName) ->
    PBSpec = protobuffs_generic:proto_file_to_spec(protofile_path(ProtoFile)),
    MSpec = protobuffs_generic:msg_spec_for(PBSpec, MsgName),
    Decoded = protobuffs_generic:decode(MSpec, Bytes),
    Decoded.

protofile_path(ProtoFile) ->
    DataDir = "../test/erlang_protobuffs_SUITE_data",
    filename:join(DataDir, ProtoFile).

%%====================

location() ->
    Str = string(),
    default(undefined,{location,Str,Str}).
%%====================

uint32() ->
    choose(0, 16#ffffffff).

sint32() ->
    choose(-16#80000000, 16#7fffffff).

uint64() ->
    choose(0,16#ffffffffffffffff).

sint64() ->
    choose(-16#8000000000000000,16#7fffffffffffffff).

string() ->
    [choose(0,255) || _ <- lists:seq(1,choose(0,32))].

binary() ->
    list_to_binary([choose(0,255) || _ <- lists:seq(1,choose(0,32))]).

real() ->
    case random:uniform(6) of
	1 -> math:exp(1/random:uniform());
	2 -> - math:exp(1/random:uniform());
	3 -> 0.0;
	4 -> infinity;
	5 -> '-infinity';
	6 -> float(random:uniform(1 bsl 32) - (1 bsl 31))
    end.

bool() ->
    random:uniform(2)==0.

choose(From, To) ->
    random:uniform(To-From)+From.

default(Default, Value) ->
    case random:uniform(2) of
	1 -> Default;
	2 -> Value
    end.
