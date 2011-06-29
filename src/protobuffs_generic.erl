-module(protobuffs_generic).

%%% General (data-driven) Protocol Buffer encoder/decoder.
%%% Conversion works without having to create a dedicated module per
%%% protobuf format specification.

-export([proto_file_to_spec/1, proto_text_to_spec/1]).
-export([msg_spec_for/2]).
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

-record(pbspec, {
	  msg_name_table :: tuple(atom()),
	  msg_type_table :: tuple(msg_type_spec())
	 }).
-type(pbspec() :: #pbspec{}). % Of msg_type_spec().
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

-spec(msg_spec_for/2 :: (pbspec(), msg_name()) -> msgspec()).
msg_spec_for(PBSpec, MsgName) ->
    Index = index_of_name(MsgName, PBSpec#pbspec.msg_name_table),
    {PBSpec, Index}.

%%%==================== Translation:

%%@hidden
to_pbspec(Resolved) ->
    MsgNameTable = list_to_tuple([list_to_atom(N) || {N,_} <- Resolved]),
    MsgTypeSpecs = list_to_tuple([to_msg_type_spec(Msg, MsgNameTable) || Msg <- Resolved]),
    #pbspec{msg_name_table=MsgNameTable,
	    msg_type_table=MsgTypeSpecs}.

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
index_of_name(Name, Table, N) ->
    if N > tuple_size(Table) ->
	    error({message_type_not_found, Name});
       Name == element(N,Table) ->
	    N;
       true ->
	    index_of_name(Name, Table, N+1)
    end.

-ifdef(TEST).
index_of_name_test() ->
    1 = index_of_name(foo, {foo}),
    1 = index_of_name(foo, {foo,bar,baz,quux}),
    3 = index_of_name(baz, {foo,bar,baz,quux}),
    4 = index_of_name(quux,{foo,bar,baz,quux}).
-endif.
    
%%%==================== Decoding ========================================

decode(_MsgSpec={PBSpec,MsgIndex}, Bin) ->
    MsgSpec = element(MsgIndex,PBSpec#pbspec.msg_type_table),
    decode_msg(PBSpec, MsgSpec, Bin, []).

%%====
decode_msg(_PBSpec, _MsgSpec, <<>>, Acc) ->
    %% TODO: Verify that required fields are present.
    %% TODO: Collect repeated fields.
    Acc;
decode_msg(PBSpec, MsgSpec={MsgName,FieldsSpec}, Bin, Acc) ->
    {ok, Tag} = protobuffs:next_field_num(Bin),
    case lists:keyfind(Tag, #field_type_spec.tag, FieldsSpec) of
	FieldSpec=#field_type_spec{} ->
	    {FieldValue,RestBin} = decode_field(PBSpec, FieldSpec, Bin),
	    decode_msg(PBSpec, MsgSpec, RestBin, [FieldValue|Acc]);
	false ->
	    error({unexpected_tag, Tag, MsgName})
    end.

decode_field(PBSpec, #field_type_spec{tag=Tag, name=Name, type=Type, kind=Kind},
	     Bin) ->
    case Type of
	{struct, Index} ->
	    SubSpec = element(Index, #pbspec.msg_type_table),
	    {{Tag, SubBytes}, Rest} = protobuffs:decode(Bin, bytes),
	    Value = decode_msg(PBSpec, SubSpec, SubBytes, []);
	_ when is_atom(Type) -> % Primitive type
	    case Kind of
		repeated_packed ->
		    {{Tag, Value}, Rest} = protobuffs:decode_packed(Bin, Type);
		_ ->
		    {{Tag, Value}, Rest} = protobuffs:decode(Bin, Type)
	    end
    end,
    {{Tag, Name, Value}, Rest}.
