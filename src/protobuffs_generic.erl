-module(protobuffs_generic).

%%% General (data-driven) Protocol Buffer encoder/decoder.
%%% Conversion works without having to create a dedicated module per
%%% protobuf format specification.

-export([proto_file_to_spec/1, proto_text_to_spec/1]).
-export([msg_spec_for/2]).
-export([decode/2, decode/3]).

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
index_of_name(Name, NameTable) ->
    index_of_name(Name, NameTable, 1).

%%@hidden
index_of_name(Name, Table, N) ->
    if N > tuple_size(Table) ->
	    error({message_type_not_found, Name});
       Name == element(N,Table) ->
	    N;
       true ->
	    index_of_name(Name, Table, N+1)
    end.

%%@hidden
index_of_name_in_list(Name, NameList) ->
    index_of_name_in_list(Name, NameList, 1).

%%@hidden
index_of_name_in_list(Name, NameList, N) ->
    case NameList of
	[] ->
	    error({message_type_not_found, Name});
	[Name|_] ->
	    N;
	[_|Rest] ->
	    index_of_name_in_list(Name, Rest, N+1)
    end.

-ifdef(TEST).
index_of_name_test() ->
    1 = index_of_name(foo, {foo}),
    1 = index_of_name(foo, {foo,bar,baz,quux}),
    3 = index_of_name(baz, {foo,bar,baz,quux}),
    4 = index_of_name(quux,{foo,bar,baz,quux}).

index_of_name_in_list_test() ->
    1 = index_of_name(foo, [foo]),
    1 = index_of_name(foo, [foo,bar,baz,quux]),
    3 = index_of_name(baz, [foo,bar,baz,quux]),
    4 = index_of_name(quux,[foo,bar,baz,quux]).
-endif.
    
%%%==================== Decoding ========================================

decode(MsgSpec, Bin) ->
    decode(MsgSpec, Bin, fun(_) -> default end).
				  
decode(_MsgSpec={PBSpec,MsgIndex}, Bin, RecordInfoFun) ->
    MsgSpec = element(MsgIndex,PBSpec#pbspec.msg_type_table),
    decode_msg(PBSpec, MsgSpec, Bin, [], RecordInfoFun).

%%====
decode_msg(PBSpec, _MsgTypeSpec={MsgName,_}, <<>>, Acc, RecordInfoFun) ->
    to_record(get_record_info(RecordInfoFun, MsgName, PBSpec), Acc);
decode_msg(PBSpec, MsgTypeSpec={MsgName,FieldsSpec}, Bin, Acc, RecordInfoFun) ->
    {ok, Tag} = protobuffs:next_field_num(Bin),
    case lists:keyfind(Tag, #field_type_spec.tag, FieldsSpec) of
	FieldSpec=#field_type_spec{} ->
	    {FieldValue,RestBin} = decode_field(PBSpec, FieldSpec, Bin, RecordInfoFun),
	    decode_msg(PBSpec, MsgTypeSpec, RestBin, [FieldValue|Acc],
		       RecordInfoFun);
	false ->
	    error({unexpected_tag, Tag, MsgName})
    end.

decode_field(PBSpec, #field_type_spec{tag=Tag, name=Name, type=Type, kind=Kind},
	     Bin,
	     RecordInfoFun) ->
    case Type of
	{struct, Index} ->
	    SubSpec = element(Index, PBSpec#pbspec.msg_type_table),
	    {{Tag, SubBytes}, Rest} = protobuffs:decode(Bin, bytes),
	    Value = decode_msg(PBSpec, SubSpec, SubBytes, [], RecordInfoFun);
	_ when is_atom(Type) -> % Primitive type
	    case Kind of
		repeated_packed ->
		    {{Tag, Value}, Rest} = protobuffs:decode_packed(Bin, Type);
		_ ->
		    {{Tag, Value}, Rest} = protobuffs:decode(Bin, Type)
	    end
    end,
    if Kind=:=repeated;
       Kind=:=repeated_packed ->
	    {{Tag, Name, Value, repeated}, Rest};
       true ->
	    {{Tag, Name, Value}, Rest}
    end.

get_record_info(RecordInfoFun, MsgName, PBSpec) ->
    case RecordInfoFun(MsgName) of
	default ->
	    default_record_info(MsgName, PBSpec);
        field_list ->
	    field_list;
	RecordInfo={_,_} ->
	    RecordInfo
    end.

default_record_info(MsgName, PBSpec) ->
    Index = index_of_name(MsgName, PBSpec#pbspec.msg_name_table),
    {_,FieldsSpec} = element(Index,PBSpec#pbspec.msg_type_table),
    RecordName = atomize(atom_to_list(MsgName)),
    RecordFields = [atomize(atom_to_list(N)) || #field_type_spec{name=N} <- FieldsSpec],
    {RecordName, RecordFields}.
    
%% @hidden
atomize(String) ->
    list_to_atom(string:to_lower(String)).

    
to_record(RecordInfo, FieldList) ->
    %% TODO: Verify that required fields are present?
    %% TODO: Handle field defaults & empty 'repeated' fields.
    case RecordInfo of
	field_list ->
	    FieldList;
	{RecordName, RecordFields} ->
	    RecordLen = length(RecordFields) + 1,
	    Record0 = setelement(1, erlang:make_tuple(RecordLen, undefined), RecordName),
	    lists:foldl(fun(F,R)-> add_record_field(F,R,RecordFields) end,
			Record0,
			FieldList)
    end.

add_record_field(Field, Record, RecordFields) ->
    Name = element(2, Field),
    Index = 1 + index_of_name_in_list(Name, RecordFields),
    NewValue =
	case Field of
	    {_Tag,_Name,Value} ->
		Value;
	    {_Tag,_Name,Value, repeated} ->
		OldList = case element(Index,Record) of
			      undefined -> [];
			      L -> L
			  end,
		[Value|OldList]
	end,
    setelement(Index, Record, NewValue).
