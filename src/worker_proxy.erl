-module(worker_proxy) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([start_link/2, gearman_message/3, cast_gearman_message/3, worker_process_connection/2
, error_in_worker/2]).
-export([init/1, handle_call/3, handle_cast/2]).

%% Public API


start_link(Id, WorkerSocket) ->
    log:t(["registering globally worker_proxy: ",Id]),
    gen_server:start_link({global, Id}, worker_proxy, #worker_proxy_state{ identifier = Id , socket = WorkerSocket }, []) .

gearman_message(WorkerProxy,Msg,Arguments) ->
    log:t(["routing to worker_proxy: ",WorkerProxy, Msg]),
    gen_server:call({global, WorkerProxy}, {Msg, Arguments}) .

error_in_worker(WorkerProxy, Error) ->
    log:t(["routing error to worker_proxy: ",WorkerProxy, error, Error]),
    gen_server:call({global, WorkerProxy}, {error_in_worker, Error}) .
    %TODO kill the proxy

cast_gearman_message(WorkerProxy,Msg,Arguments) ->
    log:t(["cast routing to worker_proxy: ",WorkerProxy, Msg]),
    gen_server:cast({global, WorkerProxy}, {Msg, Arguments}) .


%% Callbacks


init(#worker_proxy_state{ identifier = Id, socket = WorkerSocket} = State) ->
    % this thread will read from the worker connection
    % notifying the proxy with requests
    spawn(worker_proxy, worker_process_connection, [Id, WorkerSocket]),
    {ok, State} .


handle_call({set_client_id, Identifier}, _From, State) ->
    %log:t(["set_client_id worker", State]) ,
    case Identifier =:= none of
        false -> {reply, ok, State#worker_proxy_state{identifier = Identifier}} ;
        true  -> {reply, ok, State }
    end ;

handle_call({can_do, FunctionName}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    %log:t(["can_do worker", State]) ,
    {Outcome, FunctionsP} = update_functions(FunctionName, Functions),
    case Outcome of
        not_in_list -> enable_function(Identifier,FunctionName)
    end,
    {reply, ok, State#worker_proxy_state{functions = FunctionsP}} ;

handle_call({cant_do, FunctionName}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    log:t(["cant_do worker", State]) ,
    {Outcome, FunctionsP} = log:t(remove_function(FunctionName, Functions)),
    log:t(1),
    case Outcome of
        removed_from_list -> disable_function(Identifier,FunctionName)
    end,
    {reply, ok, State#worker_proxy_state{functions = FunctionsP}} ;

handle_call({reset_abilities, []}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    lists:foreach(fun(F) -> disable_function(Identifier,F) end, Functions),
    {reply, ok, State#worker_proxy_state{ functions = [] }} ;

handle_call({grab_job, none}, _From, #worker_proxy_state{functions = Functions, socket = WorkerSocket} = State) ->
    log:t(["grab_job", State]) ,
    log:t(["looking for job for", Functions]) ,
    Job = check_queues_for(Functions),
    log:t(["Found jobs for worker :",Job]),
    case Job of
        not_found ->
            Response = protocol:pack_response(no_job, {}),
            gen_tcp:send(WorkerSocket,Response),
            {reply, ok, State} ;

        #job_request{identifier = Identifier, function = FunctionName, opaque_data = Opaque} ->
            Request = protocol:pack_response(job_assign, {Identifier, FunctionName, Opaque}),
            gen_tcp:send(WorkerSocket,Request),
            {reply, ok, State#worker_proxy_state{current = Job}}
    end ;

handle_call({error_in_worker, [_Error]}, _From, State) ->
    case configuration:on_worker_failure() of
        reeschedule -> jobs_queue_server:reeschedule_job(State) ;
        none        -> notify_error(State)
    end,
    {reply, ok, State } ;

handle_call({work_status, [JobIdentifier, Numerator,Denominator]}, _From, #worker_proxy_state{ current = Job } = State) ->
    UpdatedJob = Job#job_request{ status = { Numerator, Denominator } },
    if UpdatedJob#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(work_status, {JobIdentifier, Numerator, Denominator}),
            client_proxy:send(Job#job_request.client_socket_id, Response)
    end,
    {reply, ok, State#worker_proxy_state{ current = UpdatedJob } } ;

handle_call({work_data, [JobIdentifier, OpaqueData]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(work_data, {JobIdentifier, OpaqueData}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_warning, [JobIdentifier, OpaqueData]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(work_warning, {JobIdentifier, OpaqueData}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_exception, [JobIdentifier, Reason]}, _From, #worker_proxy_state{ current = Job } = State) ->
    ExceptionsEnabled = jobs_queue_server:check_option_for_job(<<"exceptions">>, Job),
    if
        (Job#job_request.client_socket_id =/= no_socket) and ExceptionsEnabled ->
            Response = protocol:pack_response(work_exception, {JobIdentifier, Reason}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_fail, [JobIdentifier]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(work_fail, {JobIdentifier}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = none }} ;

handle_call({work_complete, [JobIdentifier, Result]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(work_complete, {JobIdentifier, Result}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = none}} .


handle_cast({noop, []}, #worker_proxy_state{ socket = WorkerSocket } = State) ->
    log:t(["worker noop"]) ,
    Request = protocol:pack_response(noop,{}),
    gen_tcp:send(WorkerSocket, Request),
    {noreply, State} .


%% private functions

notify_error(#worker_proxy_state{ current = none }) -> nothing_to_do ;
notify_error(#worker_proxy_state{ current = Job })  ->
    if
        Job#job_request.client_socket_id =/= no_socket ->
            Response = protocol:pack_response(error,1,"remote worker fatal error while executing job"),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end .

enable_function(Identifier, FunctionName) ->
    functions_registry:register_function(Identifier, FunctionName) .

disable_function(Identifier, FunctionName) ->
    functions_registry:unregister_from_function(Identifier, FunctionName) .

%% @doc
%% stores a function in the functions array if it 
%% wasn't already stored
-spec(update_functions(atom(), [atom()]) -> {(not_in_list | already_in_list), [atom()]}) .

update_functions(FunctionName, Functions) ->
    do_update_functions(FunctionName, Functions, []) .

do_update_functions(FunctionName, [], Acum) ->
    {not_in_list, [FunctionName | Acum]} ;
do_update_functions(FunctionName, [FunctionName | Rest], Acum) ->
    {already_in_list, [FunctionName | Rest] ++ Acum} ;
do_update_functions(FunctionName, [Other | Rest], Acum) ->
    do_update_functions(FunctionName, Rest, [Other | Acum]) .


%% @doc
%% removes the function if it was stored in the functions array
-spec(remove_function(atom(), [atom()]) -> {(not_in_list | removed_from_list), [atom()]}) .

remove_function(FunctionName, Functions) ->
    do_remove_function(FunctionName, Functions, []) .

do_remove_function(_FunctionName, [], Acum) ->
    {not_in_list, Acum} ;
do_remove_function(FunctionName, [FunctionName | Rest], Acum) ->
    {removed_from_list, Rest ++ Acum} ;
do_remove_function(FunctionName, [Other | Rest], Acum) ->
    do_remove_function(FunctionName, Rest, [Other | Acum]) .


worker_process_connection(ProxyIdentifier, WorkerSocket) ->
    log:t("IN WORKER PROCESS CONNECTION"),
    Read = connections:do_recv(WorkerSocket),
    case Read of

        {ok, Bin} ->

            log:t(["Received from worker to proxy ", ProxyIdentifier, Bin]),
            Msgs = protocol:process_request(Bin, []),
            log:t(["!!!!MGSG:", Msgs]),
            case Msgs of
                {error, _DonCare} ->
                    log:t(["Error reading from worker proxy socket", Msgs]),
                    worker_process_connection(ProxyIdentifier, WorkerSocket) ;
                Msgs ->
                    % TODO: instead of foreach is better to use some recursion that can
                    %        be stopped when an error has been found.
                    lists:foreach(fun(Msg) ->
                                          case Msg of

                                              {grab_job, FunctionName} ->
                                                  log:t(["worker proxy LLega grab_job",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, grab_job, FunctionName);

                                              {can_do, FunctionName} ->
                                                  log:t(["worker proxy LLega can_do",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, can_do, FunctionName);

                                              {cant_do, FunctionName} ->
                                                  log:t(["worker proxy LLega cant_do",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, cant_do, FunctionName) ;

                                              {work_complete, [JobIdentifier, Response]} ->
                                                  log:t([" worker proxy LLega work_complete",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_complete, [JobIdentifier, Response]);

                                              {work_data, [JobIdentifier, OpaqueData]} ->
                                                  log:t([" worker proxy LLega work_data",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_data, [JobIdentifier, OpaqueData]);

                                              {work_warning, [JobIdentifier, OpaqueData]} ->
                                                  log:t([" worker proxy LLega work_warning",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_exception, [JobIdentifier, OpaqueData]);

                                              {work_status, [JobIdentifier, Numerator, Denominator]} ->
                                                  log:t([" worker proxy LLega work_status",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_status, [JobIdentifier, Numerator, Denominator]) ;

                                              {work_exception, [JobIdentifier, Reason]} ->
                                                  log:t([" worker proxy LLega work_exception",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_exception, [JobIdentifier, Reason]) ;

                                              {work_fail, [JobIdentifier]} ->
                                                  log:t([" worker proxy LLega work_fail",JobIdentifier]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_fail, [JobIdentifier]);

                                              reset_abilities ->
                                                  log:t([" worker proxy LLega reset_abilities"]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, reset_abilities, []);

                                              Other ->
                                                  log:t(["worker proxy LLega unknown",Other])
                                          end
                                  end,
                                  Msgs),
                    %% Let's check if some error was found while processing messages
                    log:t(["!!!!MGSG AGAIN:", Msgs]),
                    {FoundError, Error} = lists_extensions:detect(fun(Msg) -> case log:t(Msg) of 
                                                                                  {error,_Kind} -> true ;
                                                                                  _Other        -> false
                                                                              end
                                                                  end, Msgs),
                    case FoundError of
                        error -> worker_process_connection(ProxyIdentifier, WorkerSocket) ;
                        ok    -> worker_proxy:error_in_worker(ProxyIdentifier, log:t(Error))
                    end
            end ;

        Error ->

            % log for now
            log:t(["Error in worker proxy", Error])
    end .

check_queues_for([]) ->
    not_found ;
check_queues_for([F | Fs]) ->
    log:t(["worker proxy check_queues_for:", F]),
    Found = log:t(jobs_queue_server:lookup_job(F)),
    case Found of
        {ok, not_found} -> check_queues_for(Fs) ;
        {ok, Job} -> Job
    end .


%% tests


update_functions_test() ->
    Result = update_functions(test,[]),
    ?assertEqual(Result,{not_in_list, [test]}),
    ResultB = update_functions(test,[test]),
    ?assertEqual(ResultB,{already_in_list, [test]}) .


remove_function_test() ->
    Result = update_functions(test,[]),
    ?assertEqual(Result,{not_in_list, [test]}),
    ResultB = update_functions(test,[test]),
    ?assertEqual(ResultB,{already_in_list, [test]}),
    {_St, Fs} = ResultB,
    ?assertEqual(remove_function(test,Fs),{removed_from_list, []}),
    ?assertEqual(remove_function(item_not_in_list,Fs),{not_in_list, [test]}) .

