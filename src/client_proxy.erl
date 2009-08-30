-module(client_proxy) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-export([start_link/2, send/2]) .
-export([init/1, handle_call/3]).


%% Public API

%% @doc
%% Starts a new client for socket Socket and global identifier Identifier.
start_link(Identifier, Socket) ->
    log:t(["About to start client proxy",Identifier]),
    gen_server:start_link({global, Identifier}, client_proxy, Socket, []) .


%% @doc
%% Sends data to the client socket with proxy identified globally by Identifier.
send(Identifier,Data) ->
    gen_server:call({global, Identifier}, {send, Data}) .


%% @doc
%% Closes the socket associated to proxy Identifier and finishes the execution
%% of the proxy.
close(Identifier) ->
    gen_server:call({global, Identifier}, stop) .


%% Callbacks


init(Socket) ->
    log:t(["INIT...",Socket]),
    {ok, Socket} .


handle_call({send, Data}, _From, Socket) ->
    Res = gen_tcp:send(Socket, Data),
    {reply, Res, Socket} ;

handle_call(stop, _From, Socket) ->
    {stop, normal, stopped, Socket} .
