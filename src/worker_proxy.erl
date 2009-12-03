-module(worker_proxy) .

%% @doc
%% A server that redirects messages to a connected worker.
%% It also listens for additional messages from the worker
%% through the worker connection socket.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([start_link/2, gearman_message/3, cast_gearman_message/3, worker_process_connection/3, error_in_worker/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, worker_disconnection/2]).


%% Public API


%% @doc
%% Creates the worker proxy from a given worker identifier and worker socket.
start_link(Id, WorkerSocket) ->
    log:info(["worker_proxy start_link: ",Id]),
    gen_server:start_link({global, Id}, worker_proxy, #worker_proxy_state{ identifier = Id , socket = WorkerSocket }, []) .


%% @doc
%% Redirects a Gearman protocol message to the worker proxy.
gearman_message(WorkerProxy,Msg,Arguments) ->
    gen_server:call({global, WorkerProxy}, {Msg, Arguments}) .


%% @doc
%% Reports an error inthe worker connection to the worker proxy.
error_in_worker(WorkerProxy, Error) ->
    log:error(["worker_proxy error_in_worker: ",WorkerProxy, error, Error]),
    gen_server:call({global, WorkerProxy}, {error_in_worker, Error}) .
    %TODO kill the proxy


%% @doc
%% Terminates the execution of the worker proxy unregistering the worker
%% from the worker and functions registries.
worker_disconnection(WorkerProxy, Error) ->
    log:info(["worker_proxy disconnecting: ",WorkerProxy, error, Error]),
    gen_server:call({global, WorkerProxy}, {worker_disconnection, Error}) .

%% @doc
%% Redirects a Gearman protocol message to the worker proxy.
cast_gearman_message(WorkerProxy,Msg,Arguments) ->
    gen_server:cast({global, WorkerProxy}, {Msg, Arguments}) .


%% Callbacks


init(#worker_proxy_state{ identifier = Id, socket = WorkerSocket} = State) ->
    % this thread will read from the worker connection
    % notifying the proxy with requests
    spawn(worker_proxy, worker_process_connection, [Id, WorkerSocket, true]),
    {ok, State} .


handle_call({set_client_id, WorkerId}, _From, State) ->
    case WorkerId =:= none of
        false -> workers_registry:update_worker_id(State#worker_proxy_state.identifier, WorkerId),
                 {reply, ok, State#worker_proxy_state{worker_id = WorkerId}} ;
        true  -> {reply, ok, State }
    end ;

handle_call({can_do, FunctionName}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    {Outcome, FunctionsP} = update_functions(FunctionName, Functions),
    case Outcome of
        not_in_list -> enable_function(Identifier,FunctionName),
                       log:info(["worker_proxy : can_do : ", Identifier, " Adding workers_registry info for function ", FunctionName]),
                       workers_registry:add_worker_function(Identifier, FunctionName)
    end,
    {reply, ok, State#worker_proxy_state{functions = FunctionsP}} ;

handle_call({cant_do, FunctionName}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    {Outcome, FunctionsP} = remove_function(FunctionName, Functions),
    case Outcome of
        removed_from_list -> disable_function(Identifier,FunctionName),
                             log:info(["worker_proxy : cant_do : ", Identifier, " removing workers_registry info for function ", FunctionName]),
                             workers_registry:remove_worker_function(Identifier, FunctionName)
    end,
    {reply, ok, State#worker_proxy_state{functions = FunctionsP}} ;

handle_call({reset_abilities, []}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    lists:foreach(fun(F) -> disable_function(Identifier,F) end, Functions),
    {reply, ok, State#worker_proxy_state{ functions = [] }} ;

handle_call({grab_job, none}, _From, #worker_proxy_state{functions = Functions, socket = WorkerSocket, identifier = ProxyIdentifier} = State) ->
    Job = check_queues_for(Functions),
    case Job of
        not_found ->
            Response = protocol:pack_response(no_job, {}),
            gen_tcp:send(WorkerSocket,Response),
            {reply, ok, State} ;

        #job_request{identifier = Identifier, function = FunctionName, opaque_data = Opaque} ->
            Request = protocol:pack_response(job_assign, {Identifier, FunctionName, Opaque}),
            gen_tcp:send(WorkerSocket,Request),
            workers_registry:update_worker_current(ProxyIdentifier, Job),
            {reply, ok, State#worker_proxy_state{current = Job}}
    end ;

handle_call({grab_job_uniq, none}, _From, #worker_proxy_state{functions = Functions, socket = WorkerSocket} = State) ->
    Job = check_queues_for(Functions),
    case Job of
        not_found ->
            Response = protocol:pack_response(no_job, {}),
            gen_tcp:send(WorkerSocket,Response),
            {reply, ok, State} ;

        #job_request{identifier = Identifier, function = FunctionName, opaque_data = Opaque, unique_id = Unique} ->
            Request = protocol:pack_response(job_assign_uniq, {Identifier, FunctionName, Unique, Opaque}),
            gen_tcp:send(WorkerSocket,Request),
            workers_registry:update_worker_current(Identifier, Job),
            {reply, ok, State#worker_proxy_state{current = Job}}
    end ;

handle_call({error_in_worker, [_Error]}, _From, State) ->
    case configuration:on_worker_failure() of
        reeschedule -> case State#worker_proxy_state.current =/= none of
                           true  -> jobs_queue_server:reeschedule_job(State#worker_proxy_state.current) ;
                           false -> notify_error(State)
                       end ;
        none        -> notify_error(State)
    end,
    {reply, ok, State } ;


handle_call({worker_disconnection, _Error}, _From, State) ->
    % Should we reeschedule the current task?
    log:info(["worker_proxy : worker_disconnection"]),
    case configuration:on_worker_failure() of
        reeschedule -> case State#worker_proxy_state.current =/= none of
                           true  -> jobs_queue_server:reeschedule_job(State#worker_proxy_state.current) ;
                           false -> notify_error(State)
                       end ;
        none        -> notify_error(State)
    end,
    % Unregistering functions
     functions_registry:unregister_from_function_multi(State#worker_proxy_state.identifier,
                                                       State#worker_proxy_state.functions),
    % Removing worker_proxy info
    workers_registry:unregister_worker_proxy(State#worker_proxy_state.identifier),
    {stop, normal, exit, State} ;

handle_call({work_status, [JobIdentifier, Numerator,Denominator]}, _From, #worker_proxy_state{ current = Job } = State) ->
    UpdatedJob = Job#job_request{ status = { Numerator, Denominator } },
    if UpdatedJob#job_request.background =:= false ->
            Response = protocol:pack_response(work_status, {JobIdentifier, Numerator, Denominator}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
       true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = UpdatedJob } } ;

handle_call({work_data, [JobIdentifier, OpaqueData]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.background =:= false ->
            Response = protocol:pack_response(work_data, {JobIdentifier, OpaqueData}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_warning, [JobIdentifier, OpaqueData]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.background =:= false ->
            Response = protocol:pack_response(work_warning, {JobIdentifier, OpaqueData}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_exception, [JobIdentifier, Reason]}, _From, #worker_proxy_state{ identifier = Identifier, current = Job } = State) ->
    workers_registry:update_worker_current(Identifier, none),
    if Job#job_request.background =:= false ->
            Response = protocol:pack_response(work_exception, {JobIdentifier, Reason}),
            client_proxy:forward_exception(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_fail, [JobIdentifier]}, _From, #worker_proxy_state{ identifier = Identifier, current = Job } = State) ->
    workers_registry:update_worker_current(Identifier, none),
    if
        Job#job_request.background =:= false ->
            Response = protocol:pack_response(work_fail, {JobIdentifier}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = none }} ;

handle_call({work_complete, [JobIdentifier, Result]}, _From, #worker_proxy_state{ identifier = Identifier, current = Job } = State) ->
    %log:debug(["About to set the current job to none due to work complete", Identifier, Job]),
    workers_registry:update_worker_current(Identifier, none),
    if
        Job#job_request.background =:= false ->
            %error_logger:info_msg("SENDING WORK COMPLETE CAUSE NO BACKGROUND: ~p",[Job]),
            Response = protocol:pack_response(work_complete, {JobIdentifier, Result}),
            client_proxy:send(Job#job_request.client_socket_id, Response) ;
        true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = none}} .


handle_cast({noop, []}, #worker_proxy_state{ socket = WorkerSocket } = State) ->
    %log:debug(["Sending NOOP at",State]),
    Request = protocol:pack_response(noop,{}),
    %log:debug(["NOOP Packaged as",binary_to_list(Request)]),
    gen_tcp:send(WorkerSocket, Request),
    %log:debug(["NOOP Sent"]),
    {noreply, State} .


handle_info(_Msg, State) ->
    {noreply, State}.


terminate(normal, #worker_proxy_state{identifier = Identifier}) ->
    log:info(["worker_proxy : terminate : About to terminate with normal state ", Identifier]),
    ok ;
terminate(shutdown, #worker_proxy_state{identifier = Identifier, socket = WorkerSocket}) ->
    gen_tcp:close(WorkerSocket),
    log:info(["worker_proxy : terminate : About to shutdown worker proxy connection ", Identifier]),
    workers_registry:unregister_worker_proxy(Identifier),
    ok.


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


worker_process_connection(ProxyIdentifier, WorkerSocket, ShouldRegister) ->
    if
        ShouldRegister =:= true ->
            %log:debug(["worker_proxy worker_process_connection", ProxyIdentifier]),
            {ok, {IP, _Port}} = inet:peername(WorkerSocket),
            workers_registry:register_worker_proxy(#worker_proxy_info{identifier = ProxyIdentifier, current = none, ip = IP}) ;
        true -> none
    end,

    Read = connections:do_recv(WorkerSocket),
    case Read of

        {ok, Bin} ->

            Msgs = protocol:process_request(Bin, []),
            case Msgs of
                {error, _DonCare} ->
                    %log:error(["worker_proxy worker_proxy_connection : Error reading from worker proxy socket", Msgs]),
                    worker_process_connection(ProxyIdentifier, WorkerSocket, false) ;
                Msgs ->
                    % @todo: instead of foreach is better to use some recursion that can
                    %        be stopped when an error has been found.
                    lists:foreach(fun(Msg) ->
                                          case Msg of

                                              {pre_sleep, none} ->
                                                  none;

                                              {grab_job, FunctionName} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, grab_job, FunctionName) ;

                                              {grab_job_uniq, FunctionName} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, grab_job_uniq, FunctionName) ;

                                              {can_do, FunctionName} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, can_do, FunctionName) ;

                                              {cant_do, FunctionName} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, cant_do, FunctionName) ;

                                              {work_complete, [JobIdentifier, Response]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_complete, [JobIdentifier, Response]) ;

                                              {work_data, [JobIdentifier, OpaqueData]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_data, [JobIdentifier, OpaqueData]) ;

                                              {work_warning, [JobIdentifier, OpaqueData]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_warning, [JobIdentifier, OpaqueData]) ;

                                              {work_status, [JobIdentifier, Numerator, Denominator]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_status, [JobIdentifier, Numerator, Denominator]) ;

                                              {work_exception, [JobIdentifier, Reason]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_exception, [JobIdentifier, Reason]) ;

                                              {work_fail, JobIdentifier} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_fail, [JobIdentifier]) ;

                                              {echo_req, Opaque} ->
                                                  gen_tcp:send(WorkerSocket, protocol:pack_response(echo_res, {Opaque})) ;

                                              reset_abilities ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, reset_abilities, []) ;

                                              Other ->
                                                  log:error(["worker_proxy : worker_proxy_connection : unknown message",Other])
                                          end
                                  end,
                                  Msgs),
                    %% Let's check if some error was found while processing messages
                    {FoundError, Error} = lists_extensions:detect(fun(Msg) -> case Msg of
                                                                                  {error,_Kind} -> true ;
                                                                                  _Other        -> false
                                                                              end
                                                                  end, Msgs),
                    case FoundError of
                        error -> worker_process_connection(ProxyIdentifier, WorkerSocket, false) ;
                        ok    -> worker_proxy:error_in_worker(ProxyIdentifier, Error)
                    end
            end ;

        Error ->

            % log for now
            % @todo Notify to proxy, empty queues and finish execution of the server
            log:error(["worker_proxy : worker_proxy_connection : Error in worker proxy, about to notify", Error]),
            worker_proxy:worker_disconnection(ProxyIdentifier, Error)
    end .

check_queues_for([]) ->
    not_found ;
check_queues_for([F | Fs]) ->
    Found = jobs_queue_server:lookup_job(F),
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

