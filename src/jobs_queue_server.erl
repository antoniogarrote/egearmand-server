-module(jobs_queue_server) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, submit_job/4, lookup_job/1, reeschedule_job/1]).
-export([init/1, handle_call/3]).


%% Public API


start_link() ->
    gen_server:start_link({local, jobs_queue_server}, jobs_queue_server, job_request, []) .


submit_job(FunctionName, [UniqueId, OpaqueData], ClientSocket, Level) ->
    log:t(["jobs_queue_server, submiting job pre call"]),
    ClientProxyId = case ClientSocket of
                        no_socket -> no_socket ;
                        _Other     ->  {ok, {Adress,Port}} = inet:peername(ClientSocket),
                                       log:t(1),
                                       ClientProxyIdentifier = list_to_atom(lists:flatten(io_lib:format("client@~p:~p:~p",[node(),Adress,Port]))),
                                       log:t(ClientProxyIdentifier),
                                       client_proxy:start_link(ClientProxyIdentifier, ClientSocket),
                                       log:t(2),
                                       ClientProxyIdentifier
                    end,
    Identifier = lists:flatten(io_lib:format("job@~p:(~p)",[node(),make_ref()])),
    log:t(Identifier),
    JobRequest = #job_request{ identifier = Identifier,
                               function = FunctionName,
                               unique_id = UniqueId,
                               opaque_data = OpaqueData,
                               level = Level,
                               client_socket_id = ClientProxyId }, % this can be no_socket if the job is detached
    log:t(["jobs_queue_server, submiting job", Identifier]),
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .


reeschedule_job(#job_request{ level=Level } = JobRequest) ->
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .


lookup_job(FunctionName) ->
    gen_server:call(jobs_queue_server, {lookup_job, FunctionName}) .


%% Callbacks


init(State) ->
    {ok, State}.


handle_call({submit_job, JobRequest, Level}, _From, _State) ->
    log:t(["jobs_queue_server, submiting job"]),
    {reply, {ok, JobRequest#job_request.identifier}, mnesia_store:insert(JobRequest#job_request{queue_key = {JobRequest#job_request.function, Level}}, job_request)} ;

handle_call({lookup_job, FunctionName}, _From, State) ->
    Found = do_lookup_job(FunctionName, State, [high, normal, low]),
    case Found of
        {not_found, StateP} -> {reply, {ok, not_found}, StateP} ;
        {Result, StateP}    -> {reply, {ok, Result}, StateP}
    end .


%% Private functions


do_lookup_job(_FunctionName, Queues, []) ->
    {not_found, Queues} ;

do_lookup_job(FunctionName, Queues, [Level | Levels]) ->
    log:t(["Lookup job for", {FunctionName, Level}, "in", job_request]),
    Result = mnesia_store:dequeue(fun(X) -> X#job_request.queue_key == {FunctionName, Level} end, job_request),
    case Result of
        {not_found, _QueueP} ->
            do_lookup_job(FunctionName, Queues, Levels) ;
        _Else ->
            Result
    end .
