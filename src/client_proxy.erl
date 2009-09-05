-module(client_proxy) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-export([start_link/2, send/2, client_process_connection/2]) .
-export([init/1, handle_call/3]).


%% Public API


%% @doc
%% Starts a new client for socket Socket and global identifier Identifier.
start_link(Identifier, [Socket, JobHandle]) ->
    log:t(["About to start client proxy",Identifier]),
    gen_server:start_link({global, Identifier}, client_proxy, [Identifier,Socket, JobHandle], []) .


%% @doc
%% Sends data to the client socket with proxy identified globally by Identifier.
send(Identifier,Data) ->
    gen_server:call({global, Identifier}, {send, Data}) .


%% @doc
%% Closes the socket associated to proxy Identifier and finishes the execution
%% of the proxy.
close(Identifier) ->
    gen_server:call({global, Identifier}, stop) .


gearman_message(ClientProxy,Msg,Arguments) ->
    log:t(["routing to client_proxy: ",ClientProxy, Msg]),
    gen_server:call({global, ClientProxy}, {Msg, Arguments}) .


%% Callbacks


init([Identifier, Socket, JobHandle]) ->
    log:t(["INIT...",Socket]),
    spawn(client_proxy, client_process_connection, [Identifier, Socket]),
    {ok, [Socket, JobHandle]} .


handle_call({option_req, [Option]}, _From, [Socket, JobHandle] = State) ->
    jobs_queue_server:update_job_options(JobHandle, Option),
    Response = protocol:pack_response(option_res, Option),
    gen_tcp:send(Socket, Response),
    {reply, ok, State} ;

handle_call({send, Data}, _From, [Socket, JobHandle] = State) ->
    Res = gen_tcp:send(Socket, Data),
    {reply, Res, State} ;

handle_call(stop, _From, [Socket, JobHandle] = State) ->
    {stop, normal, stopped, State} .


%% private API


client_process_connection(ProxyIdentifier, ClientSocket) ->
    log:t("IN CLIENT PROCESS CONNECTION"),
    Read = connections:do_recv(ClientSocket),
    case Read of

        {ok, Bin} ->

            log:t(["Received from client proxy ", ProxyIdentifier, Bin]),
            Msgs = protocol:process_request(Bin, []),
            log:t(["!!!!MGSG:", Msgs]),
            case Msgs of
                {error, _DonCare} ->
                    log:t(["Error reading from client proxy socket", Msgs]),
                    client_process_connection(ProxyIdentifier, ClientSocket) ;
                Msgs ->
                    % TODO: instead of foreach is better to use recursion that can
                    %        be stopped when an error has been found.
                    lists:foreach(fun(Msg) ->
                                          case Msg of

                                              {option_req, [Option]} ->
                                                  log:t([" client proxy LLega option_req",Option]),
                                                  client_proxy:gearman_message(ProxyIdentifier, option_req, [Option]);

                                              Other ->
                                                  log:t(["client proxy LLega unknown",Other])
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
                        error -> client_process_connection(ProxyIdentifier, ClientSocket) ;
                        ok    -> log:t(["Error in client proxy", FoundError]) % temporal
                    end
            end ;

        Error ->

            % log for now
            log:t(["Error in client proxy", Error])
    end .
