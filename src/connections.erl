-module(connections) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1, start_link/2, close_connection/0, check_worker_proxy_for/2, do_recv/2, do_recv/1]).
-export([init/1]).

%% Public API


start_link(Configuration) ->
    gen_server:start_link({local, connections}, connections, #connections_state{ configuration = Configuration}, []) .

start_link(Host, Port) ->
    gen_server:start_link({local, connections}, connections, #connections_state{ configuration = [{host, Host},{port, Port}]}, []) .

close_connection() ->
    gen_server:call(connections,close_connection) .

check_worker_proxy_for(Ref,Socket) ->
    %log:t([6,inet:peername(Socket)]),
    gen_server:call(connections,{check_worker_proxy, Ref, Socket}) .


%% Callbacks


init(#connections_state{ configuration = Configuration} = State) ->
    {ok,Ip} = inet:getaddr(proplists:get_value(host,Configuration),inet),
    {ok, ServerSock} = gen_tcp:listen(proplists:get_value(port,Configuration),
                                      [binary,
                                       {ip,Ip},
                                       {packet, 0},
                                       {active, false}]),

    spawn(connections, server_socket_process, [ServerSock, connections]),
    {ok, State#connections_state{ socket = ServerSock } }.


handle_call(close_connection, _From, #connections_state{ socket = S} = State) ->
    Result = gen_tcp:close(S),
    {reply, Result, State#connections_state{ socket = nil }} ;

handle_call({check_worker_proxy, Ref, Socket}, _From, #connections_state{ worker_proxies = Ws} = State) ->
    case lists:any(fun(R) -> R =:= ref end, Ws) of
        true ->
            {reply, Ref, State} ;
        false ->
            %log:t(["pre linking", Ref, Socket]) ,
            worker_proxy:start_link(Ref, Socket),
            {reply, Ref, State#connections_state{ worker_proxies = [Ref | Ws] }}
    end .

%% private functions

%% @doc
%% Main thread accepting connections from the gearmand port.
%% For each request accepted a new process handling the request
%% is spawned.
-spec(server_socket_process(gen_tcp:socket(), atom()) -> undefined) .

server_socket_process(ServerSocket,ConnectionsServer) ->
    ClientConnectionResult = gen_tcp:accept(ServerSocket),
    case ClientConnectionResult of

        {ok, ClientSocket} ->
            spawn(connections, process_connection, [ClientSocket]) ,
            server_socket_process(ServerSocket,ConnectionsServer) ;

        {error, Reason} ->
            log:t(["Error",Reason])
    end .


%% @doc
%% Handles a connection request to the main Gearman port.
%% It parses the request and decides what kind of processement
%% is required: registering a worker proxy, storing a client's job request, etc.
-spec(process_connection(gen_tcp:socket()) -> undefined) .

process_connection(ClientSocket) ->
    {ok, {Adress,Port}} = inet:peername(ClientSocket),
    NewIdentifier = list_to_atom(lists:flatten(io_lib:format("worker@~p:~p",[Adress,Port]))),
    %log:t([1,inet:peername(ClientSocket)]),
    %{ok, Bin} = do_recv(ClientSocket, []),
    {ok, Bin} = do_recv(ClientSocket),
    log:t(["Received from client",Bin]),
    % this thread will process the request and notify us with the message
    Msgs = protocol:process_request(Bin, []),
    log:t(["MSGS EN QUEUE ",Msgs]),
    lists:foreach(fun(Msg) ->

                          case Msg of
                              {set_client_id, []} ->
                                  log:t(["LLega set_client_id",[]]),
                                  log:t([2,inet:peername(ClientSocket)]),
                                  check_worker_proxy_for(NewIdentifier, ClientSocket),
                                  worker_proxy:gearman_message(NewIdentifier, set_client_id, none) ;

                              {set_client_id, Identifier} ->
                                  log:t(["LLega set_client_id",Identifier]),
                                  log:t([3,inet:peername(ClientSocket)]),
                                  check_worker_proxy_for(NewIdentifier, ClientSocket),
                                  worker_proxy:gearman_message(NewIdentifier, set_client_id, Identifier) ;

                              {can_do, FunctionName} ->
                                  log:t(["LLega can_do",FunctionName]),
                                  log:t([4,inet:peername(ClientSocket)]),
                                  check_worker_proxy_for(NewIdentifier, ClientSocket),
                                  worker_proxy:gearman_message(NewIdentifier, can_do, FunctionName) ;

                              {grab_job, none} ->
                                  log:t(["LLega grab_job"]),
                                  log:t([5,inet:peername(ClientSocket)]),
                                  check_worker_proxy_for(NewIdentifier, ClientSocket),
                                  worker_proxy:gearman_message(NewIdentifier, grab_job, none) ;

                              {submit_job, [FunctionName | Arguments]} ->
                                  process_job(normal, FunctionName, Arguments, ClientSocket) ;

                              {submit_job_high, [FunctionName | Arguments]} ->
                                  process_job(high, FunctionName, Arguments, ClientSocket) ;

                              {submit_job_low, [FunctionName | Arguments]} ->
                                  process_job(low, FunctionName, Arguments, ClientSocket) ;

                              Other ->
                                  log:t(["LLega unknown",Other])
                          end
                  end,
                  Msgs) .


%% private functions


%% doc
%% Common logic for dispatching jobs of different priority level
process_job(Level, FunctionName, Arguments, ClientSocket) ->
    log:t(["LLega submit_job", Level, FunctionName, Arguments]),
    {ok, Handle} = jobs_queue_server:submit_job(FunctionName, Arguments, ClientSocket, Level),
    Response = protocol:pack_response(job_created, {Handle}),
    log:t(["Sending job_created",Response]),
    gen_tcp:send(ClientSocket, Response),
    WorkerProxies = functions_registry:workers_for_function(FunctionName),
    log:t(["Found Server for Function:", length(WorkerProxies)]),
    lists:foreach(fun(WorkerProxy) ->
                          spawn(fun() -> worker_proxy:cast_gearman_message(WorkerProxy, noop, []) end)
                  end,
                  WorkerProxies)  .


%% @doc
%% Function for reading from a socket until it closes
%% its connection.
-spec(do_recv(gen_tcp:socket(), binary()) -> binary()) .

do_recv(Sock, Bs) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
            do_recv(Sock, [Bs, B]);
        {error, closed} ->
            {ok, list_to_binary(Bs)}
    end.


%% @doc
%% request a data packet from a socket.
-spec(do_recv(gen_tcp:socket()) -> binary()) .

do_recv(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
           {ok, B} ;
        {error, closed} ->
            {error, socket_closed}
    end.
