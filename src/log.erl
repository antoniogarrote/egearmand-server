%%
%% @doc functions for tracing and logging
%%
-module(log).

-author("Antonio Garrote Hernandez").

-import(io).

-export([t/1,p/1,error/1]).

% data

t(Msg) ->
    io:format("~n*** trace: ~p~n",[Msg]),
    Msg.

p(Msg) ->
    io:format("~n*** print: ~p~n",[Msg]).

error(Msg) ->
    io:format("~n*** ERROR: ~n~p~n***~n",[Msg]).
