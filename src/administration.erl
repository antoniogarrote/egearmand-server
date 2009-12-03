-module(administration) .

%% @doc
%% A server handling some requests from the administrative
%% protocol of Gearman.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start_link/0, status/0, workers/0, version/0]) .


%% Public API


%% @doc
%% Establishes the connection.
start_link() ->
    gen_server:start_link({local, administration}, administration, [], []) .


%% @doc
%% Retrieves admin data for the status of the server
status() ->
    gen_server:call(administration, {status}) .


%% @doc
%% Retrives admin data for the workers registered in the server
workers() ->
    gen_server:call(administration, {workers}) .


%% @doc
%% Retrieves the version of the server
version() ->
    egearmand_app:version().

%% Callbacks


init(State) ->
    {ok, State} .


handle_call({status}, _From, State) ->
    Functions = mnesia_store:all(function_register),
    CountedFunctions = count_functions(Functions, []),
    Queues = mnesia_store:all(job_request),
    CountedFunctionsQueue = add_queues(CountedFunctions, Queues, []),
    AllDataFunctions = add_current_info(CountedFunctionsQueue, workers_registry:worker_functions()),
    StrFs = lists:map(fun({N,Q,Cr,C}) ->
                              lists:flatten(io_lib:format("~s" ++ [$\t] ++"~p"++ [$\t] ++"~p"++ [$\t] ++"~p",[N,Q,Cr,C])) end,
                      AllDataFunctions),
    Res = lists:foldl(fun(X,Acum) -> Acum ++ X ++ "\n" end, "", StrFs),
    {reply, Res ++ ".", State} ;

handle_call({workers}, _From, State) ->
    Workers = mnesia_store:all(worker_proxy_info),
    StrFs = lists:map(fun(#worker_proxy_info{identifier = _Id, ip= {N1,N2,N3,N4}, worker_id=WorkerId, functions = Fs}) ->
                              StrFunctions = lists:foldl(fun(F,Acum) -> F ++ " " ++ Acum end, "", Fs),
                              lists:flatten(io_lib:format("0 ~p.~p.~p.~p ~s : ",[N1,N2,N3,N4,WorkerId])) ++ StrFunctions
                      end,
                      Workers),
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
    {Name, length(lists:filter(fun(X) -> X#job_request.function =:= Name end, Qs)), 0, C} .

add_current_info(Ds, []) -> Ds ;
add_current_info(Ds, [F | Fs]) ->
    add_current_info(update_current_info(F,Ds,[]), Fs) .

update_current_info(_F, [], Acum) -> Acum ;

update_current_info(F, [{F,Q,Cr,C},Ds],Acum) ->
    update_current_info(F, Ds, [{F,Q,(Cr+1),C} | Acum]) ;

update_current_info(F, [D | Ds], Acum) ->
    update_current_info(F, Ds, [D | Acum]) .


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
