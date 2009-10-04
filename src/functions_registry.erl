-module(functions_registry) .

%% @doc
%% A registry of all the functions in the server
%% and the workers associated to each function.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start_link/0, register_function/2, unregister_from_function/2, workers_for_function/1]) .


%% Public API


%% @doc
%% Establishes the connection.
start_link() ->
    gen_server:start_link({local, functions_registry}, functions_registry, [], []) .


%% @doc
%% Register a worker proxy associated to the function FunctionName.
register_function(Ref, FunctionName) ->
    gen_server:call(functions_registry,{register, Ref, FunctionName}) .


%% @doc
%% Unregisters a worker proxy from a function.
unregister_from_function(Ref, FunctionName) ->
    gen_server:call(functions_registry,{unregister, Ref, FunctionName}) .


%% @doc
%% Retrieves all the workers associated to a function.
workers_for_function(FunctionName) ->
    gen_server:call(functions_registry, {workers_for, FunctionName}) .


%% Callbacks


init(State) ->
    {ok, State} .


handle_call({register, Ref, FunctionName}, _From, _Store) ->
    {reply, ok, mnesia_store:insert(#function_register{ function_name = FunctionName, table_key = {FunctionName, Ref}, reference = Ref },
                             function_register)} ;

handle_call({unregister, Ref, FunctionName}, _From, _Store) ->
    {reply, ok, mnesia_store:delete({FunctionName, Ref}, function_register)} ;

handle_call({workers_for, FunctionName}, _From, _Store) ->
    log:debug(["Looking workers for function",FunctionName]),
    Registers = mnesia_store:all(fun(X) -> X#function_register.function_name == FunctionName end, function_register),
    Workers = lists:map(fun(FR) -> FR#function_register.reference end, Registers),
    log:debug(["Found number of workers:",length(Workers)]),
    {reply, Workers, function_register} .


%% dummy callbacks so no warning are shown at compile time
handle_cast(_Msg, State) ->
    {noreply, State} .

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(shutdown, State) ->
    ok.
