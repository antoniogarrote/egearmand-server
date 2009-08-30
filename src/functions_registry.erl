-module(functions_registry) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3]) .
-export([start_link/0, register_function/2, unregister_from_function/2, workers_for_function/1]) .


%% Public API


start_link() ->
    gen_server:start_link({local, functions_registry}, functions_registry, [], []) .


register_function(Ref, FunctionName) ->
    gen_server:call(functions_registry,{register, Ref, FunctionName}) .


unregister_from_function(Ref, FunctionName) ->
    gen_server:call(functions_registry,{unregister, Ref, FunctionName}) .


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
    log:t(1),
    Registers = mnesia_store:all(fun(X) -> X#function_register.function_name == FunctionName end, function_register),
    log:t(Registers),
    Workers = lists:map(fun(FR) -> FR#function_register.reference end, Registers),
    {reply, Workers, function_register} .
