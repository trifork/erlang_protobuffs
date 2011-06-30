-module(xpath_json).

-export([select/2]).

-define(TEST,true).
-export([test/0]).

-record(node, {root     :: #node{} | self,
	       parent   :: #node{},
	       value    :: { term(), term() },
	       position :: integer(),
	       last     :: integer() }).

%% 
%% Given `XPathExpr' (from xmerl_xpath_parse:parse/1) and `JSON' (from mochijson2:decode/1), 
%% select the objects matching the expression.
%%
select(XPathExpr, JSON) ->
    Results = eval(XPathExpr, #node{ root=self, value={root,JSON} }),
    [ value_of(Result) || Result <- Results ].



eval({comp, OP, Exp1, Exp2}, Node) ->
    case eval(Exp1,Node) of
	[] -> [];
	M1 ->
	    M2 = eval(Exp2,Node),
	    fold_join(fun(N1,N2,Acc) ->
			      V1 = value_of(N1),
			      V2 = value_of(N2),
			      
			      Bool = 
				  case OP of
				      '<' -> V1 < V2;
				      '>' -> V1 > V2;
				      '>=' -> V1 >= V2;
				      '<=' -> V1 =< V2;
				      '=' -> V1 == V2;
				      '!=' -> V1 /= V2
				  end,
			      [{bool, Bool}|Acc]
		      end,
		      [],
		      M1, M2)
    end;

eval({bool, OP, Exp1, Exp2}, Node) ->
    M1 = eval(Exp1,Node),
    M2 = eval(Exp2,Node),
    fold_join(fun(N1,N2,Acc) ->
		  V1 = value_of(N1),
		  V2 = value_of(N2),
		  Bool = 
		      case OP of
			  'and' when is_boolean(V1), is_boolean(V2) ->
			      V1 and V2;
			  'or' when is_boolean(V1), is_boolean(V2) ->
			      V1 or V2;
			  _ ->
			      false
		      end,
		  [#node{value={bool,Bool}}|Acc]
	  end,
	  [],
	  M1, M2);

eval({arith, OP, Exp1, Exp2}, Node) ->
    M1 = eval(Exp1,Node),
    M2 = eval(Exp2,Node),
    fold_join(fun(N1,N2,Acc) ->
		  V1 = value_of(N1),
		  V2 = value_of(N2),

		  Result = case OP of
			       '+' ->
				   V1 + V2;
			       '-' ->
				   V1 - V2;
			       'mod' ->
				   V1 rem V2;
			       'div' ->
				   V1 / V2;
			       '*' ->
				   V1 * V2
			   end,

		  [{number,Result}|Acc]
	  end,
	  [], M1, M2);

eval({number, _}=Number, _) ->
    [Number];

eval({path,union,{P1,P2}}, Node) ->
    eval(P1,Node) ++ eval(P2,Node);

eval({path,abs,'/'}, Node) ->
    [root(Node)];

eval({path,abs,Exp}, Node) ->
    eval(Exp, root(Node));

eval({path,rel,Exp}, Node) ->    
    eval(Exp, Node);

eval({step, How}, Node) -> 
    fold_step(fun cons/2, [], How, Node);

eval({refine, Q1, Q2}, Node) ->
    Candidates = eval(Q1, Node),
    lists:foldl(fun(Candidate=#node{},Result) ->
			eval(Q2, Candidate) ++ Result
		end,
		[],
		Candidates);


eval({function_call, Fun, Args}, Node) ->
    eval_function(Fun,Args,Node).

eval_predicates([], _) ->
    true;
eval_predicates([{pred, {number, N}}|Rest], Node=#node{ position=N }) ->
    eval_predicates(Rest,Node);
eval_predicates([{pred, {number, _}}|_], _) ->
    false;
eval_predicates([{pred, X}|Rest], Node) ->
    lists:any(fun value_of/1, eval(X, Node)) 
	andalso eval_predicates(Rest,Node).

%%
%% this is fairly slow, because xmerl uses "name" and mochijson2 uses <<"name">>
%%
eval_nodetest({name, {AName,[],SName}}, Node) ->
    Name = node_name(Node),
    Name =:= AName
	orelse Name =:= SName
	orelse atom_to_binary(AName, latin1) =:= Name;

eval_nodetest({node_type, T}, Node) ->
    type_of(Node) =:= T;

eval_nodetest({wildcard,wildcard}, _) ->
    true.


eval_function(last,[],#node{ last=Last }) ->
    [{number, Last}];

eval_function(position,[],#node{ position=Position }) ->
    [{number, Position}];

eval_function('node-name',[Node],_) ->
    case node_name(Node) of
	undefined -> [];
	Name -> [{literal, Name}]
    end;



%%
%%
eval_function(true, [], _) ->
    [{bool, true}];

eval_function(false, [], _) ->
    [{bool, false}]

.


fold_step(Fun, Acc, {Axis, NodeTest, PredicateList}, Node ) ->
    fold_axis(Axis, 
	      fun(Node0,Acc0) ->
		      case eval_nodetest(NodeTest, Node0) 
			  andalso eval_predicates(PredicateList, Node0) of
			  true ->
			      Fun(Node0,Acc0);
			  false ->
			      Acc0
		      end
	      end,
	      Acc,
	      Node).


%% fold_axis(Axis,Fun,Acc,Current) does a fold from Current along Axis

fold_axis(attribute, Fun, Acc, Node) ->
    fold_axis(child, Fun, Acc, Node);
				
fold_axis(child, Fun, Acc, Node=#node{ value={_, {struct, Members}}}) ->
    %Last = length(Members),
    {Acc2,_} = lists:foldl(fun(Member, {Acc0, Idx}) ->
				   Acc1 = Fun(#node{ value=Member, 
						     parent=Node, 
						     root=root(Node)
						  }, Acc0),
				   {Acc1, Idx+1}
			   end,
			   {Acc, 0},
			   Members),
    Acc2;

fold_axis(child, Fun, Acc, Node=#node{ value={Name, Members}}) when is_list(Members) ->
    Last = length(Members),
    {Acc2,_} = lists:foldl(fun(Member, {Acc0, Idx}) ->
				   Acc1 = Fun(#node{ value={Name, Member}, 
						     parent=Node,
						     position=Idx, 
						     last=Last, 
						     root=root(Node)
						  }, Acc0),
				   {Acc1, Idx+1}
			   end,
			   {Acc, 0},
			   Members),
   Acc2;

fold_axis(child, _, Acc, #node{ value={_, Value}}) when not is_list(Value)->
    Acc;

fold_axis(descendant, Fun, Acc, Node) ->
    fold_axis(child, 
	      fun(C,Acc0) ->
		      Acc1=Fun(C,Acc0),
		      fold_axis(descendant, Fun,Acc1,C)
	      end,
	      Acc,
	      Node);

fold_axis(descendant_or_self, Fun, Acc, Node) ->
    Acc1 = fold_axis(descendant, Fun, Acc, Node),
    fold_axis(self, Fun, Acc1, Node);

fold_axis(self, Fun, Acc, Node) ->
    Fun(Node, Acc);

fold_axis(parent, _, Acc, #node{ parent=undefined }) ->
    Acc;
fold_axis(parent, Fun, Acc, #node{ parent=Parent }) ->
    Fun(Parent, Acc);

fold_axis(ancestor, _, Acc, #node{ parent=undefined }) ->
    Acc;
fold_axis(ancestor, Fun, Acc, #node{ parent=ParentNode }) ->
    fold_axis(ancestor, Fun, Fun(ParentNode,Acc), ParentNode);

fold_axis(ancestor_or_self, Fun, Acc, Node) ->
    fold_axis(ancestor, Fun, Fun(Node,Acc), Node).


type_of(T) when is_tuple(T) ->
    element(1, T);
type_of(_) ->
    undefined.

node_name(#node{value={Name,_Value}}) ->
    Name;
node_name(_) ->
    undefined.

value_of(#node{value={_Name,Value}}) ->
    Value;
value_of({number, N}) ->
    N;
value_of({bool, B}) when is_boolean(B) ->
    B;
value_of({literal, L}) when is_list(L) ->
    L.


root(Node=#node{root=self}) ->
    Node;
root(#node{root=Node=#node{}})->
    Node.

%% utility cons function
cons(H,T) ->
    [H|T].

-ifdef(TEST).

test() ->

    In = mochijson2:decode(" { \"a\": [\"zero\",\"one\",2], \"b\": { \"j\":7, \"l\":true, \"k\": { \"foo\": 3, \"t\" : {\"vals\":[ 45,6,7 ], \"j\":4} }  }} "),
    io:format("~s~n", [mochijson2:encode(In)]),

    {ok, Expr} = xmerl_xpath_parse:parse(xmerl_xpath_scan:tokens("//k[//j > foo and foo = 3]/t")),
    Out = select(Expr, In),
    
    io:format("~s~n", [mochijson2:encode(Out)]).

-endif.

%%
%% Utility 2-way fold (join)
%%

fold_join(_,Acc,[],_) ->
    Acc;
fold_join(_,Acc,_,[]) ->
    Acc;
fold_join(Fun,Acc,[E1],[E2]) -> 
    Fun(E1,E2,Acc);
fold_join(Fun,Acc,List1,List2) ->
    lists:foldl(fun(E1,Acc0) ->
			lists:foldl(fun(E2,Acc1) ->
					    Fun(E1,E2,Acc1)
				    end,
				    Acc0,
				    List2)
		end,
		Acc,
		List1).
