%
% @doc extra functions for manipulation of lists
%
-module(lists_extensions).

-author("Antonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl").

-export([compact/1, unique/1, splice/2, eachWithIndex/2, eachWithIndexP/2, interleave/1, mapWithIndex/2, detect/2]).


compact(List) ->
    FilterList = fun(_,[],Acum) -> lists:reverse(Acum) ;
                    (F,[H|T],Acum) ->
                         case H == undefined of
                             true -> F(F,T,Acum) ;
                             false -> F(F,T,[H | Acum])
                         end
                 end,
    FilterList(FilterList,List,[]).

unique(List) ->
    FilterList = fun(_,[],Acum) -> lists:reverse(Acum) ;
                    (F,[H|T],Acum) ->
                         case lists:member(H,Acum) of
                             true -> F(F,T,Acum) ;
                             false -> F(F,T,[H | Acum])
                         end
                 end,
    FilterList(FilterList,List,[]).

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

eachWithIndex(Fun,L) ->
    DoPagination= fun(_,_,[]) -> undefined ;
                     (F,I,[H|T]) -> Fun(I,H),
                                    F(F,I+1,T)
                  end,
    DoPagination(DoPagination,0,L).

mapWithIndex(Fun,L) ->
    DoPagination= fun(_,_,[],Acum) -> lists:reverse(Acum) ;
                     (F,I,[H|T],Acum) -> F(F,I+1,T,[Fun(I,H) | Acum])
                  end,
    DoPagination(DoPagination,0,L,[]).

eachWithIndexP(Fun,L) ->
    DoPagination= fun(_,_,[]) -> undefined ;
                     (F,I,[H|T]) -> spawn(fun() -> Fun(I,H) end),
                                    F(F,I+1,T)
                  end,
    DoPagination(DoPagination,0,L).

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

detect(_P,[]) ->
    {error, not_found} ;

detect(P, [H | R]) ->
    case P(H) of
        true  -> {ok, H} ;
        false -> detect(P,R)
    end .


%% tests


detect_test() ->
    ?assertEqual({ok, 2}, detect(fun(X) -> X =:= 2 end, [1,2,3,4])),
    ?assertEqual({error, not_found}, detect(fun(X) -> X =:= 8 end, [1,2,3,4])) .

