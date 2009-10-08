-module(administration) .

%% @doc
%% A registry of all the functions in the server
%% and the workers associated to each function.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start_link/0, status/0]) .


%% Public API


%% @doc
%% Establishes the connection.
start_link() ->
    gen_server:start_link({local, administration}, administration, [], []) .


%% @doc
%% Retrieves de admin data for the status of the server
status() ->
    gen_server:call(administration, {status}) .


%% Callbacks


init(State) ->
    {ok, State} .


handle_call({status}, _From, State) ->
    Functions = mnesia_store:all(function_register),
    CountedFunctions = count_functions(Functions, []),
    Queues = mnesia_store:all(job_request),
    CountedFunctionsQueue = add_queues(CountedFunctions, Queues, []),
    StrFs = lists:map(fun({N,Q,C}) -> 
                              lists:flatten(io_lib:format("~s" ++ [$\t] ++"~p"++ [$\t] ++"~p"++ [$\t] ++"~p",[N,Q,0,C])) end,
                      CountedFunctionsQueue),
    Res = lists:foldl(fun(X,Acum) -> Acum ++ X ++ "\n" end, "", StrFs),
    {reply, Res ++ ".", State} .


%% dummy callbacks so no warning are shown at compile time
handle_cast(_Msg, State) ->
    {noreply, State} .


handle_info(_Msg, State) ->
    {noreply, State}.


terminate(shutdown, _State) ->
    ok.


%% private API

count_functions([], Acum) -> Acum ;

count_functions([F | Fs], Acum) ->
    count_functions(Fs, update_function_count_acum(F, Acum, [])) .


update_function_count_acum(F, [], Acum) ->
    [{F#function_register.function_name, 1} | Acum] ;
update_function_count_acum(F, [{RN, C} = R | Rs], Acum) ->
    if
        F#function_register.function_name =:= RN ->
            [{RN, (C + 1)} | (Rs ++ Acum)] ;
        true ->
            update_function_count_acum(F, Rs, [R | Acum])
    end .


add_queues([],_Qs,Acum) -> Acum ;

add_queues([R | Rs], Qs, Acum) ->
    add_queues(Rs, Qs, [update_function_queues(R,Qs) | Acum]) .

update_function_queues({Name,C}, Qs) ->
    {Name, length(lists:filter(fun(X) -> X#job_request.function =:= Name end, Qs)), C} .

%% tests

update_function_queues_test() ->
    R = {"test", 1},
    Jrs = [#job_request{function ="test"}, #job_request{function = "other"}, #job_request{function="test"}],
    {N,Q,C} = update_function_queues(R,Jrs),
    ?assertEqual(N,"test"),
    ?assertEqual(Q, 2),
    ?assertEqual(C,1) .

update_function_count_acum_a_test() ->
    Tmp = [{"test2", 2}, {"test", 1}],
    F = #function_register{ function_name = "test" },
    Result = update_function_count_acum(F, Tmp, []),
    {ok, {"test",C}} = lists_extensions:detect(fun({Name,_C}) -> Name =:= "test" end, Result),
    ?assertEqual(C, 2) .


update_function_count_acum_b_test() ->
    Tmp = [],
    F = #function_register{ function_name = "test" },
    Result = update_function_count_acum(F, Tmp, []),
    {ok, {"test",C}} = lists_extensions:detect(fun({Name,_C}) -> Name =:= "test" end, Result),
    ?assertEqual(C, 1) .

count_functions_test() ->
    Fs = [#function_register{ function_name = "test2" }, #function_register{ function_name = "test" }, #function_register{ function_name = "test2" }],
    Res = count_functions(Fs, []),
    {ok, {"test",C}} = lists_extensions:detect(fun({Name,_C}) -> Name =:= "test" end, Res),
    ?assertEqual(C, 1),
    {ok, {"test2",D}} = lists_extensions:detect(fun({Name,_C}) -> Name =:= "test2" end, Res),
    ?assertEqual(D, 2),
    ?assertEqual(length(Res),2) .
