-module(workers_registry) .

%% @doc
%% A registry of all the workers in the cluster.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start_link/0, register_worker_proxy/1, unregister_worker_proxy/1, update_worker_current/2]) .


%% Public API


%% @doc
%% Establishes the connection.
start_link() ->
    gen_server:start_link({global, workers_registry}, workers_registry, [], []) .


%% @doc
%% Register a worker proxy associated to the function FunctionName.
register_worker_proxy(WorkerProxyInfo) ->
    log:debug(["workers_registry : registering worker", WorkerProxyInfo]),
    gen_server:call({global,workers_registry},{register, WorkerProxyInfo}) .


%% @doc
%% Unregisters a worker proxy from a function.
unregister_worker_proxy(ProxyIdentifier) ->
    gen_server:call({global,workers_registry},{unregister, ProxyIdentifier}) .

%% @doc
%% Updates the state of a worker proxy setting the current function being
%% executed by the worker.
update_worker_current(ProxyIdentifier, CurrentFunction) ->
    gen_server:cast({global,workers_registry},{update_current, ProxyIdentifier, CurrentFunction}) .


%% Callbacks


init(State) ->
    {ok, State} .


handle_call({register, WorkerProxyInfo}, _From, _Store) ->
    {reply, ok, mnesia_store:insert(WorkerProxyInfo,
                                    worker_proxy_info)} ;

handle_call({unregister, ProxyIdentifier}, _From, _Store) ->
    {reply, ok, mnesia_store:delete(ProxyIdentifier, worker_proxy_info)} .


%% dummy callbacks so no warning are shown at compile time
handle_cast({update_current, ProxyIdentifier, Current}, State) ->
    mnesia_store:update(fun(OldState) -> OldState#worker_proxy_info{ current = Current } end,
                        ProxyIdentifier,
                        worker_proxy_info),
    {noreply, State} .

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(shutdown, State) ->
    ok.
