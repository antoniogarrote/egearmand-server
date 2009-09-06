-module(lists_extensions).

%% @doc
%% Extra functions for manipulation of lists.

-author("Antonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl") .

-export([compact/1, unique/1, splice/2, eachWithIndex/2, eachWithIndexP/2, interleave/1, mapWithIndex/2, detect/2]) .
-export([index/2, update_and_detect/2]) .

%% @doc
%% Remove undefined occurences from a list.
-spec(compact([any()]) -> [any()]) .

compact(List) ->
    FilterList = fun(_,[],Acum) -> lists:reverse(Acum) ;
                    (F,[H|T],Acum) ->
                         case H == undefined of
                             true -> F(F,T,Acum) ;
                             false -> F(F,T,[H | Acum])
                         end
                 end,
    FilterList(FilterList,List,[]).


%% @doc
%% Removes duplicates from a list.
-spec(unique([any()]) -> [any()]) .

unique(List) ->
    FilterList = fun(_,[],Acum) -> lists:reverse(Acum) ;
                    (F,[H|T],Acum) ->
                         case lists:member(H,Acum) of
                             true -> F(F,T,Acum) ;
                             false -> F(F,T,[H | Acum])
                         end
                 end,
    FilterList(FilterList,List,[]).


%% @doc
%% Dived a List in sublists of Max elements maximum
-spec(splice(integer(),[any()]) -> [[any()]]) .

splice(Max,List) ->
    DoSplice = fun(_,_,[],Acum) -> lists:reverse(Acum);
                  (F,M,L,Acum) ->
                       case length(L) < Max of
                           true -> F(F,M,[],[L|Acum]);
                           false -> {H,T} = lists:split(M,L),
                                    F(F,M,T,[H|Acum])
                       end
               end,
    DoSplice(DoSplice,Max,List,[]).


%% @doc
%% Iterates using the proficed Fun over a list L passing
%% as an argument the items of the list and an index for the
%% item.
eachWithIndex(Fun,L) ->
    DoPagination= fun(_,_,[]) -> undefined ;
                     (F,I,[H|T]) -> Fun(I,H),
                                    F(F,I+1,T)
                  end,
    DoPagination(DoPagination,0,L).


%% @doc
%% Applies the map iterator to a list passing as arguments to the
%% mapping function the element of the list and an index.
mapWithIndex(Fun,L) ->
    DoPagination= fun(_,_,[],Acum) -> lists:reverse(Acum) ;
                     (F,I,[H|T],Acum) -> F(F,I+1,T,[Fun(I,H) | Acum])
                  end,
    DoPagination(DoPagination,0,L,[]).


%% @doc
%% Version of @see eachWithIndex that runs in parallel.
eachWithIndexP(Fun,L) ->
    DoPagination= fun(_,_,[]) -> undefined ;
                     (F,I,[H|T]) -> spawn(fun() -> Fun(I,H) end),
                                    F(F,I+1,T)
                  end,
    DoPagination(DoPagination,0,L).


%% @doc
%% Mix a set of lists inserting the nth element of each list
%% as nth, nth +1, nth + (number of lists - 1) elements of the
%% mixed lists .
-spec(interleave([[any()]]) -> [any()]) .

interleave([]) -> [];
interleave(Lists) ->
    DoInterleave = fun(_F,[[]],Acum,_Level) ->
                           lists:reverse(Acum);
                      (F,[L|T],Acum,Level) ->
                           case length(L) < Level of
                               true -> F(F,[L|T],Acum, Level - 1);
                               false -> case L of
                                            [H|Hs] -> F(F,T ++ [Hs],[H|Acum],Level);
                                            []  -> F(F,T,Acum,Level)
                                        end
                           end
                   end,
    DoInterleave(DoInterleave,Lists,[],length(lists:nth(1,Lists))).


%% @doc
%% Detects if an element is present in a list applying the predicate P to
%% test the presence .
detect(_P,[]) ->
    {error, not_found} ;

detect(P, [H | R]) ->
    case P(H) of
        true  -> {ok, H} ;
        false -> detect(P,R)
    end .


%% @doc
%% traverses a list applying a predicate, updates the value of the element 
%% in the list where the application result is {true, NewValue} and returns 
%% the modified list and the updated element.
update_and_detect(P, L) ->
    update_and_detect(P, L, {not_found, []}) .

update_and_detect(_P, [], Acum) -> Acum ;
update_and_detect(P, [H | R], {not_found, Acum} ) ->
    case (P(H)) of
       {true,  Hp} -> {Hp , lists:reverse(Acum) ++ [Hp | R]} ;
       {false, Hp} -> update_and_detect(P, R, {not_found, [Hp | Acum]})
    end .


%% @doc
%% Returns the index of an Element in a List
-spec(index(any(), [any()]) -> integer()) .

index(Element, List)                  -> index(Element, List, 1) .
index(_Element, [], _Index)           -> not_found ;
index(Element, [Element | _T], Index) -> Index ;
index(Element, [_H | T], Index)       -> index(Element, T, (Index + 1)) .


%% tests

update_and_detect_test() ->
    ?assertEqual({here, [1,here,3,4]},
                 update_and_detect(fun(X) -> if X =:= 2 -> {true, here} ;
                                                X =/= 2 -> {false, X}
                                             end
                                   end, [1,2,3,4])) .


detect_test() ->
    ?assertEqual({ok, 2}, detect(fun(X) -> X =:= 2 end, [1,2,3,4])),
    ?assertEqual({error, not_found}, detect(fun(X) -> X =:= 8 end, [1,2,3,4])) .


index_test() ->
    ?assertEqual(index(test,[1,2,test,4,t]), 3),
    ?assertEqual(index(test,[1,2]), not_found),
    ?assertEqual(index(test,[]), not_found),
    ?assertEqual(index(test,[test]), 1),
    ?assertEqual(index(test,[test,2]), 1),
    ?assertEqual(index(test,[1,2,test]), 3) .
