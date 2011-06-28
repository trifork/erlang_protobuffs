-module(protobuffs_generic).

%%% General (data-driven) Protocol Buffer encoder/decoder.
%%% Conversion works without having to create a dedicated module per
%%% protobuf format specification.

-export([proto_file_to_spec/1, proto_text_to_spec/1]).
-export([decode/2]).

%%%==================== Type definitions ==============================

-type(pb_primitive_type() :: protobuffs:field_type()).

-type(msg_nr() :: integer()).
-type(msg_name() :: atom()).
-type(field_tag() :: integer()).
-type(field_name() :: atom()).
-type(field_kind() :: required | optional | repeated | repeated_packed).
-type(field_type() :: pb_primitive_type() | {struct, msg_nr()}).

-record(field_type_spec, {
	  tag  :: field_tag(),
	  kind :: field_kind(),
	  name :: field_name(),
	  type :: field_type(),
	  default :: number() | string() | none
	 }).

-type(msg_type_spec() :: {msg_name(), [field_type_spec()]}).
-type(field_type_spec() :: #field_type_spec{}).
-type(pbspec() :: tuple(msg_type_spec())). % Of msg_type_spec().
-type(msgspec() :: {pbspec(), msg_nr()}).

%%%==================== Parsing and translation =========================

proto_file_to_spec(ProtoFileName) ->
    {ok,Parsed} = protobuffs_compile:parse(ProtoFileName),
    parsed_to_pbspec(Parsed).

proto_text_to_spec(ProtoText) ->
    {ok,Parsed} = protobuffs_compile:parse_text(ProtoText),
    parsed_to_pbspec(Parsed).

parsed_to_pbspec(Parsed) ->
    Checked = protobuffs_compile:resolve(Parsed),
    to_pbspec(Checked).

%%%==================== Translation:

%%@hidden
to_pbspec(Resolved) ->
    MsgNameTable = [list_to_atom(N) || {N,_} <- Resolved],
    MsgTypeSpecs = [to_msg_type_spec(Msg, MsgNameTable) || Msg <- Resolved],
    MsgNameTuple = list_to_tuple(MsgNameTable),
    {MsgTypeSpecs, MsgNameTuple}.

to_msg_type_spec({MsgNameStr, Fields}, MsgNameTable) ->
    {list_to_atom(MsgNameStr),
     [to_field_type_spec(F, MsgNameTable) || F <- Fields]}.

to_field_type_spec({Tag, Kind, Type, Name, Default}, MsgNameTable) ->
    TypeAtm = list_to_atom(Type),
    BoundType = case protobuffs_compile:is_scalar_type(Type) of
		    true -> TypeAtm;
		    false -> {struct, index_of_name(TypeAtm, MsgNameTable)}
		end,
    #field_type_spec{tag=Tag,
		     kind=Kind,
		     type=BoundType,
		     name=list_to_atom(Name),
		     default=Default}.

%%%==================== Helpers:

%%@hidden
index_of_name(Name, NameList) ->
    index_of_name(Name, NameList, 1).

%%@hidden
index_of_name(Name, [], _) -> error({message_type_not_found, Name});
index_of_name(Name, [Name|_], N) -> N;
index_of_name(Name, [_|Rest], N) -> index_of_name(Name, Rest, N+1).


-ifdef(TEST).
index_of_name_test() ->
    1 = index_of_name(foo, [foo]),
    1 = index_of_name(foo, [foo,bar,baz,quux]),
    3 = index_of_name(baz, [foo,bar,baz,quux]),
    4 = index_of_name(quux,[foo,bar,baz,quux]).
-endif.
    
%%%==================== Decoding ========================================

decode(_MsgSpec={PBSpec,MsgIndex}, Bin) ->
    MsgSpec = element(MsgIndex,PBSpec),
    decode_msg(PBSpec, MsgSpec, Bin, []).

%%====
decode_msg(_PBSpec, _MsgSpec, <<>>, Acc) ->
    %% TODO: Verify that required fields are present.
    Acc;
decode_msg(PBSpec, MsgSpec={MsgName,FieldsSpec}, Bin, Acc) ->
    {ok, Tag} = protobuffs:next_field_num(Bin),
    case lists:keyfind(Tag, #field_type_spec.tag, FieldsSpec) of
	FieldSpec=#field_type_spec{} ->
	    {FieldValue,RestBin} = decode_field(FieldSpec, Bin),
	    decode_msg(PBSpec, MsgSpec, RestBin, [FieldValue|Acc]);
	false ->
	    error({unexpected_tag, Tag, MsgName})
    end.

decode_field(_FieldSpec, _Bin) ->
    'TODO'.

