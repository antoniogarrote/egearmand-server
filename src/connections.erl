-module(connections) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1, start_link/2, close_connection/0, check_worker_proxy_for/2, do_recv/1, do_recv/2, server_socket_process/2, process_connection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Public API


start_link(Configuration) ->
    gen_server:start_link({local, connections}, connections, #connections_state{ configuration = Configuration}, []) .


start_link(Host, Port) ->
    gen_server:start_link({local, connections}, connections, #connections_state{ configuration = [{host, Host},{port, Port}]}, []) .


close_connection() ->
    gen_server:call(connections,close_connection) .


check_worker_proxy_for(Ref,Socket) ->
    gen_server:call(connections,{check_worker_proxy, Ref, Socket}) .


%% Callbacks


init(#connections_state{ configuration = Configuration} = State) ->
    {ok,Ip} = inet:getaddr(proplists:get_value(host,Configuration),inet),
    {ok, ServerSock} = gen_tcp:listen(proplists:get_value(port,Configuration),
                                      [binary,
                                       {packet, raw},
                                       {nodelay, true},
                                       {reuseaddr, true},
                                       {ip,Ip},
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
            worker_proxy:start_link(Ref, Socket),
            {reply, Ref, State#connections_state{ worker_proxies = [Ref | Ws] }}
    end .


%% dummy callbacks so no warning are shown at compile time
handle_cast(_Msg, State) ->
    {noreply, State} .

handle_info(_Msg, State) ->
    {noreply, State}.


%% @doc
%% Closes the socket when stoping the server
terminate(shutdown, #connections_state{ socket = ServerSock }) ->
    gen_tcp:close(ServerSock),
    ok.


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
            %spawn(connections, server_socket_process, [ServerSocket, ConnectionsServer]),
            %process_connection(ClientSocket) ;
            spawn(connections, process_connection, [ClientSocket]) ,
            server_socket_process(ServerSocket,ConnectionsServer) ;

        {error, Reason} ->
            log:error(["connections server_socket_process",Reason])
    end .


%% @doc
%% Handles a connection request to the main Gearman port.
%% It parses the request and decides what kind of processement
%% is required: registering a worker proxy, storing a client's job request, etc.
-spec(process_connection(gen_tcp:socket()) -> undefined) .

process_connection(ClientSocket) ->
    {ok, Bin} = do_recv(ClientSocket),
    % this thread will process the request and notify us with the message
    Msgs = protocol:process_request(Bin, []),
    lists:foreach(fun(Msg) ->
                          case check_extensions(Msg) of
                              %% No extension registered, just follow common gearman flow
                              {error, not_found} ->
                                  process_connection(Msg, ClientSocket) ;
                              %% There is an extension registered for this message.
                              %% We pass the control to the extension
                              {ok, Extension} ->
                                  log:info(["connections process_connection : Loading extension", Extension]),
                                  apply_extension(Extension, Msg, ClientSocket)
                          end
                  end,
                  Msgs) .


%% private functions


process_connection(Msg, ClientSocket) ->
    {ok, {Adress,Port}} = inet:peername(ClientSocket),
    NewIdentifier = list_to_atom(lists:flatten(io_lib:format("worker@~p:~p:~p",[node(),Adress,Port]))),

    case Msg of
        {pre_sleep, none} ->
            none;

        {set_client_id, []} ->
            check_worker_proxy_for(NewIdentifier, ClientSocket),
            worker_proxy:gearman_message(NewIdentifier, set_client_id, none) ;

        {set_client_id, Identifier} ->
            check_worker_proxy_for(NewIdentifier, ClientSocket),
            worker_proxy:gearman_message(NewIdentifier, set_client_id, Identifier) ;

        {can_do, FunctionName} ->
            check_worker_proxy_for(NewIdentifier, ClientSocket),
            worker_proxy:gearman_message(NewIdentifier, can_do, FunctionName) ;

        {grab_job, none} ->
            check_worker_proxy_for(NewIdentifier, ClientSocket),
            worker_proxy:gearman_message(NewIdentifier, grab_job, none) ;

        {grab_job_uniq, none} ->
            check_worker_proxy_for(NewIdentifier, ClientSocket),
            worker_proxy:gearman_message(NewIdentifier, grab_job_uniq, none) ;


        {echo_req, Opaque} ->
            gen_tcp:send(ClientSocket, protocol:pack_response(echo_res, {Opaque})),
            process_connection(ClientSocket);

        {submit_job, [FunctionName | Arguments]} ->
            process_job(normal, FunctionName, Arguments, ClientSocket) ;

        {submit_job_high, [FunctionName | Arguments]} ->
            process_job(high, FunctionName, Arguments, ClientSocket) ;

        {submit_job_low, [FunctionName | Arguments]} ->
            process_job(low, FunctionName, Arguments, ClientSocket) ;

        {submit_job_bg, [FunctionName | Arguments]} ->
            process_job_bg(normal, FunctionName, Arguments, ClientSocket);

        {submit_job_high_bg, [FunctionName | Arguments]} ->
            process_job_bg(high, FunctionName, Arguments, ClientSocket);

        {submit_job_low_bg, [FunctionName | Arguments]} ->
            process_job_bg(low, FunctionName, Arguments, ClientSocket);


        %% administrative requests
        {status, none} ->
            gen_tcp:send(ClientSocket, administration:status()),
            process_connection(ClientSocket);

        {workers, none} ->
            gen_tcp:send(ClientSocket, administration:workers()),
            process_connection(ClientSocket);

        {version, none} ->
            gen_tcp:send(ClientSocket, administration:version()),
            process_connection(ClientSocket);

        {maxqueue, none} ->
            gen_tcp:send(ClientSocket, "OK"),
            process_connection(ClientSocket);

        {shutdown, none} ->
            log:info("shutdown requested"),
            % @todo
            gen_tcp:close(ClientSocket),
            %gen_server:cast(non_otp_egearmand_controller, quit) ;
            application:stop(egearmand) ;

        Other ->
            log:error(["connections process_connection : unknown",Other])
    end  .


%% doc
%% Common logic for dispatching background jobs of different priority level
process_job_bg(Level, FunctionName, Arguments, ClientSocket) ->
    {ok, Handle} = jobs_queue_server:submit_job(true, FunctionName, Arguments, ClientSocket, Level),
    Response = protocol:pack_response(job_created, {Handle}),
    gen_tcp:send(ClientSocket, Response),
    WorkerProxies = functions_registry:workers_for_function(FunctionName),
    lists:foreach(fun(WorkerProxy) ->
                          spawn(fun() -> worker_proxy:cast_gearman_message(WorkerProxy, noop, []) end)
                  end,
                  WorkerProxies)  .

%% doc
%% Common logic for dispatching jobs of different priority level
process_job(Level, FunctionName, Arguments, ClientSocket) ->
    {ok, Handle} = jobs_queue_server:submit_job(false, FunctionName, Arguments, ClientSocket, Level),
    Response = protocol:pack_response(job_created, {Handle}),
    gen_tcp:send(ClientSocket, Response),
    WorkerProxies = functions_registry:workers_for_function(FunctionName),
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
            do_recv(Sock, [Bs | B]);
        {error, closed} ->
            {ok, list_to_binary(Bs)}
    end.


%% @doc
%% request a data packet from a socket.
-spec(do_recv(gen_tcp:socket()) -> binary()) .
%% do_recv(Socket) ->
%%     receive
%%         {tcp, Socket, Bin} ->
%%             Bin ;
%%         {tcp_closed, _Socket} ->
%%             io:format("Server socket closed~n"),
%%             {error, socket_closed}
%%     end.
do_recv(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
            {ok, B} ;
        {error, closed} ->
            {error, socket_closed} ;
        {error, Reason} ->
            log:error(["connections do_recv: error reading from client socket ", Reason]),
            {error, socket_closed}
    end.


%% @doc
%% Checks if any extension has hooks for
%% this connection
check_extensions(Msg) ->
    lists_extensions:detect(fun(Elem) ->
                                    erlang:apply(Elem, connection_hook_for, [Msg])
                            end,
                            configuration:extensions()) .


%% @doc
%% Applies one of the extensions using the
%% received message and the client socket
apply_extension(Extension, Msg, ClientSocket) ->
    erlang:apply(Extension, entry_point, [Msg, ClientSocket]) .
