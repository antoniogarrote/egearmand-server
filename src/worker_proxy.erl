-module(worker_proxy) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([start_link/2, gearman_message/3, cast_gearman_message/3, worker_process_connection/2]).
-export([init/1, handle_call/3, handle_cast/2]).

%% Public API


start_link(Id, WorkerSocket) ->
    gen_server:start_link({local, Id}, worker_proxy, #worker_proxy_state{ identifier = Id , socket = WorkerSocket }, []) .

gearman_message(WorkerProxy,Msg,Arguments) ->
    gen_server:call(WorkerProxy, {Msg, Arguments}) .

cast_gearman_message(WorkerProxy,Msg,Arguments) ->
    gen_server:cast(WorkerProxy, {Msg, Arguments}) .

%% Callbacks


init(#worker_proxy_state{ identifier = Id, socket = WorkerSocket} = State) ->
    % this thread will read from the worker connection
    % notifying the proxy with requests
    spawn(worker_proxy, worker_process_connection, [Id, WorkerSocket]),
    {ok, State} .


handle_call({can_do, FunctionName}, _From, #worker_proxy_state{functions = Functions, identifier = Identifier} = State) ->
    %log:t(["can_do worker", State]) ,
    {Outcome, FunctionsP} = update_functions(FunctionName, Functions),
    case Outcome of
        not_in_list -> enable_function(Identifier,FunctionName)
    end,
    {reply, ok, State#worker_proxy_state{functions = FunctionsP}} ;

handle_call({set_client_id, Identifier}, _From, State) ->
    %log:t(["set_client_id worker", State]) ,
    case Identifier =:= none of
        false -> {reply, ok, State#worker_proxy_state{identifier = Identifier}} ;
        true  -> {reply, ok, State }
    end ;

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

handle_call({work_status, [JobIdentifier, Numerator,Denominator]}, _From, #worker_proxy_state{ current = Job } = State) ->
    UpdatedJob = Job#job_request{ status = { Numerator, Denominator } },
    if UpdatedJob#job_request.socket =/= no_socket ->
            Response = protocol:pack_response(work_status, {JobIdentifier, Numerator, Denominator}),
            gen_tcp:send(UpdatedJob#job_request.socket,Response)
    end,
    {reply, ok, State#worker_proxy_state{ current = UpdatedJob } } ;

handle_call({work_data, [JobIdentifier, OpaqueData]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.socket =/= no_socket ->
            Response = protocol:pack_response(work_data, {JobIdentifier, OpaqueData}),
            gen_tcp:send(Job#job_request.socket, Response) ;
        true -> dont_care
    end,
    {reply, ok, State} ;

handle_call({work_complete, [JobIdentifier, Result]}, _From, #worker_proxy_state{ current = Job } = State) ->
    if
        Job#job_request.socket =/= no_socket ->
            Response = protocol:pack_response(work_complete, {JobIdentifier, Result}),
            gen_tcp:send(Job#job_request.socket,Response) ;

        true -> dont_care
    end,
    {reply, ok, State#worker_proxy_state{ current = none}} .


handle_cast({noop, []}, #worker_proxy_state{ socket = WorkerSocket } = State) ->
    log:t(["worker noop"]) ,
    Request = protocol:pack_response(noop,{}),
    gen_tcp:send(WorkerSocket, Request),
    {noreply, State} .


%% private functions


enable_function(Identifier, FunctionName) ->
    functions_registry:register_function(Identifier, FunctionName) .


update_functions(FunctionName, Functions) ->
    do_update_functions(FunctionName, Functions, []) .


do_update_functions(FunctionName, [], Acum) ->
    {not_in_list, [FunctionName | Acum]} ;
do_update_functions(FunctionName, [FunctionName | Rest], Acum) ->
    {already_in_list, [FunctionName | Rest] ++ Acum} ;
do_update_functions(FunctionName, [Other | Rest], Acum) ->
    do_update_functions(FunctionName, Rest, [Other | Acum]) .


worker_process_connection(ProxyIdentifier, ClientSocket) ->
    %log:t("IN WORKER PROCESS CONNECTION"),
    Read = connections:do_recv(ClientSocket),
    case Read of

        {ok, Bin} ->

            %log:t(["Received from worker to proxy ", ProxyIdentifier, Bin]),
            Msgs = protocol:process_request(Bin, []),
            case Msgs of
                {error, _DonCare} ->
                    %log:t(["Error reading from worker proxy socket", Msgs]),
                    worker_process_connection(ProxyIdentifier, ClientSocket) ;
                Msgs ->
                    %log:t(["MSGS EN QUEUE ",Msgs]),
                    lists:foreach(fun(Msg) ->
                                          log:t(1),
                                          case Msg of

                                              {grab_job, FunctionName} ->
                                                  %log:t(["worker proxy LLega grab_job",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, grab_job, FunctionName);

                                              {can_do, FunctionName} ->
                                                  %log:t([" worker proxy LLega can_do",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, can_do, FunctionName);

                                              {work_complete, [JobIdentifier, Response]} ->
                                                  %log:t([" worker proxy LLega can_do",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_complete, [JobIdentifier, Response]);

                                              {work_data, [JobIdentifier, OpaqueData]} ->
                                                  %log:t([" worker proxy LLega can_do",FunctionName]),
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_data, [JobIdentifier, OpaqueData]);

                                              {work_status, [JobIdentifier, Numerator, Denominator]} ->
                                                  worker_proxy:gearman_message(ProxyIdentifier, work_status, [JobIdentifier, Numerator, Denominator]) ;

                                              Other ->
                                                  log:t(["worker proxy LLega unknown",Other])
                                          end
                                  end,
                                  Msgs),
                    worker_process_connection(ProxyIdentifier, ClientSocket)
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
