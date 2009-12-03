-module(client_proxy) .

%% @doc
%% A proxy redirecting messages to a client connection. It
%% also listen for additional messages from the client.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").

-export([start_link/2, send/2, client_process_connection/2, close/1, gearman_message/3]) .
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, forward_exception/2]).


%% Public API


%% @doc
%% Starts a new client for socket Socket and global identifier Identifier.
start_link(Identifier, Socket) ->
    log:debug(["client_proxy : start_link : ",Identifier]),
    gen_server:start_link({global, Identifier}, client_proxy, [Identifier,Socket, []], []) .


%% @doc
%% Sends data to the client socket with proxy identified globally by Identifier.
send(Identifier,Data) ->
    gen_server:call({global, Identifier}, {send, Data}) .


%% @doc
%% Sends data to the client socket with proxy identified globally by Identifier.
forward_exception(Identifier, ExceptionResponse) ->
    gen_server:call({global, Identifier}, {forward_exception, ExceptionResponse}) .



%% @doc
%% Closes the socket associated to proxy Identifier and finishes the execution
%% of the proxy.
close(Identifier) ->
    gen_server:call({global, Identifier}, stop) .


%% @doc
%% Sends a generic Msg to the client proxy.
%% The message will be processed in a gen_server handle_call function.
gearman_message(ClientProxy,Msg,Arguments) ->
    gen_server:call({global, ClientProxy}, {Msg, Arguments}) .


%% Callbacks


init([Identifier, Socket, Options]) ->
    spawn(client_proxy, client_process_connection, [Identifier, Socket]),
    {ok, [Identifier, Socket, Options]} .


handle_call({option_req, [Option]}, _From, [Identifier, Socket, Options]) ->
    %jobs_queue_server:update_job_options(Handle, Option),
    Response = protocol:pack_response(option_res, Option),
    gen_tcp:send(Socket, Response),
    {reply, ok, [Identifier, Socket, [Options | Options]]} ;


handle_call({get_status, [Handle]}, _From, [_Identifier, Socket, _Options] = State) ->
    case workers_registry:check_worker_for_job(Handle) of
        false        -> Jobs = workers_registry:check_handle_for_worker(Handle),
                        case Jobs of
                            []     -> Response = protocol:pack_response(status_res, {Handle, 0, 0, 1, 1}),
                                      gen_tcp:send(Socket, Response) ;

                            _Other ->
                                Response = protocol:pack_response(status_res, {Handle, 1, 0, 1, 1}),
                                gen_tcp:send(Socket, Response)
                        end ;
        _WorkerProxy -> Response = protocol:pack_response(status_res, {Handle, 1, 1, 1, 1}),
                        gen_tcp:send(Socket, Response)
    end,
    {reply, ok, State} ;

handle_call({send, Data}, _From, [_Identifier, Socket, _Options] = State) ->
    Res = gen_tcp:send(Socket, Data),
    {reply, Res, State} ;

handle_call({forward_exception, ExceptionResponse}, _From, [_Identifier, Socket, Options] = State) ->
    ForwardExceptionsP = lists:member(<<"exceptions">>, Options),
    if ForwardExceptionsP =:= true ->
            gen_tcp:send(Socket, ExceptionResponse) ;
       true -> dont_care
    end,
    {reply, ok, State} ;

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State} .


%% dummy callbacks so no warning are shown at compile time
handle_cast(_Msg, State) ->
    {noreply, State} .


handle_info(_Msg, State) ->
    {noreply, State}.

terminate(shutdown, _State) ->
    ok.


%% private API

%% doc
%% Common logic for dispatching background jobs of different priority level
process_job_bg(Level, FunctionName, Arguments, ClientProxyId, ClientSocket) ->
    {ok, Handle} = jobs_queue_server:submit_job_from_client_proxy(ClientProxyId, true, FunctionName, Arguments, Level),
    Response = protocol:pack_response(job_created, {Handle}),
    gen_tcp:send(ClientSocket, Response),
    WorkerProxies = functions_registry:workers_for_function(FunctionName),
    lists:foreach(fun(WorkerProxy) ->
                          spawn(fun() -> worker_proxy:cast_gearman_message(WorkerProxy, noop, []) end)
                  end,
                  WorkerProxies)  .

%% doc
%% Common logic for dispatching jobs of different priority level
process_job(Level, FunctionName, Arguments, ClientProxyId, ClientSocket) ->
    {ok, Handle} = jobs_queue_server:submit_job_from_client_proxy(ClientProxyId, false, FunctionName, Arguments, Level),
    Response = protocol:pack_response(job_created, {Handle}),
    gen_tcp:send(ClientSocket, Response),
    WorkerProxies = functions_registry:workers_for_function(FunctionName),
    lists:foreach(fun(WorkerProxy) ->
                          spawn(fun() -> worker_proxy:cast_gearman_message(WorkerProxy, noop, []) end)
                  end,
                  WorkerProxies)  .


client_process_connection(ProxyIdentifier, ClientSocket) ->
    Read = connections:do_recv(ClientSocket),
    case Read of

        {ok, Bin} ->

            Msgs = protocol:process_request(Bin, []),
            case Msgs of
                {error, _DonCare} ->
                    log:error(["client_proxy client_process_connection : reading from client proxy socket", Msgs]),
                    client_process_connection(ProxyIdentifier, ClientSocket) ;
                Msgs ->
                    % TODO: instead of foreach is better to use recursion that can
                    %        be stopped when an error has been found.
                    lists:foreach(fun(Msg) ->
                                          case Msg of

                                              {option_req, [Option]} ->
                                                  client_proxy:gearman_message(ProxyIdentifier, option_req, [Option]);

                                              {get_status, Handle} ->
                                                  client_proxy:gearman_message(ProxyIdentifier, get_status, [Handle]);

                                              {echo_req, Opaque} ->
                                                     gen_tcp:send(ClientSocket, protocol:pack_response(echo_res, {Opaque})) ;

                                              {submit_job, [FunctionName | Arguments]} ->
                                                  process_job(normal, FunctionName, Arguments, ProxyIdentifier, ClientSocket) ;

                                              {submit_job_high, [FunctionName | Arguments]} ->
                                                  process_job(high, FunctionName, Arguments, ProxyIdentifier, ClientSocket) ;

                                              {submit_job_low, [FunctionName | Arguments]} ->
                                                  process_job(low, FunctionName, Arguments, ProxyIdentifier, ClientSocket) ;

                                              {submit_job_bg, [FunctionName | Arguments]} ->
                                                  process_job_bg(normal, FunctionName, Arguments, ProxyIdentifier, ClientSocket);

                                              {submit_job_high_bg, [FunctionName | Arguments]} ->
                                                  process_job_bg(high, FunctionName, Arguments, ProxyIdentifier, ClientSocket);

                                              {submit_job_low_bg, [FunctionName | Arguments]} ->
                                                  process_job_bg(low, FunctionName, Arguments, ProxyIdentifier, ClientSocket) ;

                                              Other ->
                                                  log:error(["client_proxy client_process_connection : unknown message",Other])
                                          end
                                  end,
                                  Msgs),
                    %% Let's check if some error was found while processing messages
                    {FoundError, _Error} = lists_extensions:detect(fun(Msg) -> case Msg of
                                                                                   {error,_Kind} -> true ;
                                                                                   _Other        -> false
                                                                               end
                                                                   end, Msgs),
                    case FoundError of
                        error -> client_process_connection(ProxyIdentifier, ClientSocket) ;
                        ok    -> log:error(["client_proxy client_process_connection : ", FoundError]) % temporal
                    end
            end ;

        Error ->

            % log for now
            log:error(["client_proxy client_process_connection : error in client proxy", Error])
    end .
