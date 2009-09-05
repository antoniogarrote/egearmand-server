%%
%% @doc functions for tracing and logging
%%
-module(log).

%% @doc
%% Logging functions

-author("Antonio Garrote Hernandez").

-import(io).

-export([t/1,p/1,error/1, info/1, warning/1]).


%% @doc
%% Traces code Msg returning Msg.
-spec(t(any()) -> any()) .

t(Msg) ->
    io:format("~n*** trace: ~p~n",[Msg]),
    Msg.


%% @doc
%% Prints the message Msg.
p(Msg) ->
    io:format("~n*** print: ~p~n",[Msg]).


%% @doc
%% Logs message Msg with error log level.
error(Msg) ->
    io:format("~n*** error: ~n~p~n***~n",[Msg]).


%% @doc
%% Logs message Msg with error info level.
info(Msg) ->
    io:format("~n*** info: ~n~p~n***~n",[Msg]).


%% @doc
%% Logs message Msg with error warning level.
warning(Msg) ->
    io:format("~n*** warning: ~n~p~n***~n",[Msg]).
