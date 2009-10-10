-module(client_proxy) .

%% @doc
%% A proxy redirecting messages to a client connection. It
%% also listen for additional messages from the client.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-export([start_link/2, send/2, client_process_connection/2]) .
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).


%% Public API


%% @doc
%% Starts a new client for socket Socket and global identifier Identifier.
start_link(Identifier, [Socket, JobHandle]) ->
    log:debug(["client_proxy start_link",Identifier]),
    gen_server:start_link({global, Identifier}, client_proxy, [Identifier,Socket, JobHandle], []) .


%% @doc
%% Sends data to the client socket with proxy identified globally by Identifier.
send(Identifier,Data) ->
    log:debug(["client_proxy send", Identifier, Data]),
    gen_server:call({global, Identifier}, {send, Data}) .


%% @doc
%% Closes the socket associated to proxy Identifier and finishes the execution
%% of the proxy.
close(Identifier) ->
    log:debug(["client_proxy close", Identifier]),
    gen_server:call({global, Identifier}, stop) .


gearman_message(ClientProxy,Msg,Arguments) ->
    log:debug(["client_proxy gearman_message", ClientProxy, Msg, Arguments]),
    gen_server:call({global, ClientProxy}, {Msg, Arguments}) .


%% Callbacks


init([Identifier, Socket, JobHandle]) ->
    spawn(client_proxy, client_process_connection, [Identifier, Socket]),
    {ok, [Socket, JobHandle]} .


handle_call({option_req, [Option]}, _From, [Socket, JobHandle] = State) ->
    jobs_queue_server:update_job_options(JobHandle, Option),
    Response = protocol:pack_response(option_res, Option),
    gen_tcp:send(Socket, Response),
    {reply, ok, State} ;

handle_call({send, Data}, _From, [Socket, _JobHandle] = State) ->
    Res = gen_tcp:send(Socket, Data),
    {reply, Res, State} ;

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


client_process_connection(ProxyIdentifier, ClientSocket) ->
    log:debug(["client_proxy client_process_connection", ProxyIdentifier]),
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

                                              Other ->
                                                  log:info(["client_proxy client_process_connection : unknown message",Other])
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
