-module(workers_registry) .

%% @doc
%% A registry of all the workers in the cluster.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl") .
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start_link/0, register_worker_proxy/1, unregister_worker_proxy/1, update_worker_current/2, worker_functions/0]) .
-export([add_worker_function/2, remove_worker_function/2, update_worker_id/2, check_worker_for_job/1, check_handle_for_worker/1]).

%% Public API


%% @doc
%% Establishes the connection.
start_link() ->
    gen_server:start_link({global, workers_registry}, workers_registry, [], []) .


%% @doc
%% Register a worker proxy associated to the function FunctionName.
register_worker_proxy(WorkerProxyInfo) ->
    %log:debug(["workers_registry : registering worker", WorkerProxyInfo]),
    gen_server:call({global,workers_registry},{register, WorkerProxyInfo}) .


%% @doc
%% Unregisters a worker proxy from a function.
unregister_worker_proxy(ProxyIdentifier) ->
    gen_server:call({global,workers_registry},{unregister, ProxyIdentifier}) .

%% @doc
%% Checks if any worker in the registry has the handle in their current jobs.
check_handle_for_worker(Handle) ->
    gen_server:call({global,workers_registry},{check_handle_for_worker, Handle}) .

%% @doc
%% Updates the state of a worker proxy setting the current function being
%% executed by the worker.
update_worker_current(ProxyIdentifier, CurrentFunction) ->
    gen_server:cast({global,workers_registry},{update_current, ProxyIdentifier, CurrentFunction}) .

%% @doc
%% Sets the client id info of a certain worker.
update_worker_id(ProxyIdentifier, Id) ->
    gen_server:cast({global,workers_registry},{update_worker_id, ProxyIdentifier, Id}) .


%% @doc
%% Adds an additional function to the supported functions of a workers.
add_worker_function(ProxyIdentifier, Function) ->
    %log:debug(["workers_registry add_worker_function", ProxyIdentifier, Function]),
    gen_server:cast({global,workers_registry},{add_worker_function, ProxyIdentifier, Function}) .


%% @doc
%% Removes an additional function to the supported functions of a workers.
remove_worker_function(ProxyIdentifier, Function) ->
    gen_server:cast({global,workers_registry},{remove_worker_function, ProxyIdentifier, Function}) .


%% @doc
%% Returns the worker associated to a job handle
check_worker_for_job(JobHandle) ->
    gen_server:call({global,workers_registry},{check_worker_for_job, JobHandle}) .


%% @doc
%% Retuns all the registered workers at a certain moment
worker_functions() ->
    gen_server:call({global,workers_registry},{worker_functions}) .


%% Callbacks


init(State) ->
    {ok, State} .


handle_call({register, WorkerProxyInfo}, _From, _Store) ->
    {reply, ok, mnesia_store:insert(WorkerProxyInfo,
                                    worker_proxy_info)} ;

handle_call({unregister, ProxyIdentifier}, _From, _Store) ->
    {reply, ok, mnesia_store:delete(ProxyIdentifier, worker_proxy_info)} ;

handle_call({check_worker_for_job, JobHandle}, _From, St) ->
    WorkerProxies = mnesia_store:all(fun(State) ->
                                             Current = State#worker_proxy_info.current,
                                             case Current of
                                                 #job_request{ identifier = JobHandle } ->
                                                     true ;
                                                 _Other  ->
                                                     false
                                             end
                                     end,
                                     worker_proxy_info),
    case WorkerProxies of
        [WorkerProxy] -> {reply, WorkerProxy, St} ;
        []            -> {reply, false, St}
    end ;

handle_call({worker_functions}, _From, Store) ->
    ProxyInfos = mnesia_store:all(worker_proxy_info),
    FilteredInfos = lists:filter(fun(#worker_proxy_info{current = Job}) -> Job =/= none end, ProxyInfos),
    Functions = lists:map(fun(#worker_proxy_info{current = C}) -> C#job_request.function end,FilteredInfos),
    {reply, Functions, Store} ;


handle_call({check_handle_for_worker, Handle}, _From, Store) ->
    Res = mnesia_store:all(fun(S) ->
                                   %log:debug(["Comparing with ", S#job_request.identifier, S#job_request.unique_id]),
                                   (S#job_request.identifier =:= Handle) or (S#job_request.unique_id =:= Handle)
                           end,
                           job_request),
    {reply, Res, Store} .

handle_cast({update_current, ProxyIdentifier, Current}, State) ->
    %log:debug(["Updating current :",ProxyIdentifier, Current]),
    mnesia_store:update(fun(OldState) -> OldState#worker_proxy_info{ current = Current } end,
                        ProxyIdentifier,
                        worker_proxy_info),
    {noreply, State} ;


handle_cast({update_worker_id, ProxyIdentifier, Id}, State) ->
    %log:debug([ProxyIdentifier, "Updating worker ID", Id]),
    mnesia_store:update(fun(OldState) -> OldState#worker_proxy_info{ worker_id = Id } end,
                        ProxyIdentifier,
                        worker_proxy_info),
    {noreply, State} ;

handle_cast({add_worker_function, ProxyIdentifier, Function}, State) ->
    mnesia_store:update(fun(#worker_proxy_info{ functions = Fs } = OldState) -> OldState#worker_proxy_info{ functions = add_function(Function, Fs, []) } end,
                        ProxyIdentifier,
                        worker_proxy_info),
    {noreply, State} ;

handle_cast({remove_worker_function, ProxyIdentifier, Function}, State) ->
    mnesia_store:update(fun(#worker_proxy_info{ functions = Fs } = OldState) -> OldState#worker_proxy_info{ functions = remove_function(Function, Fs, []) } end,
                        ProxyIdentifier,
                        worker_proxy_info),
    {noreply, State} .

%% dummy callbacks so no warning are shown at compile time
handle_info(_Msg, State) ->
    {noreply, State}.


terminate(shutdown, _State) ->
    ok.


%% private functions

add_function(F,[], Acum) -> [F | Acum] ;
add_function(F, [F | Fs], Acum) -> [F | Fs] ++ Acum ;
add_function(F, [Fp | Fs], Acum) -> add_function(F, Fs, [Fp | Acum]) .


remove_function(_F,[], Acum) -> Acum ;
remove_function(F, [F | Fs], Acum) -> Fs ++ Acum ;
remove_function(F, [Fp | Fs], Acum) -> remove_function(F, Fs, [Fp | Acum]) .
