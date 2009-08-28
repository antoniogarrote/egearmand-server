-module(jobs_queue_server) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, submit_job/4, lookup_job/1, dequeue_job_with_identifier/1]).
-export([init/1, handle_call/3]).


%% Public API


start_link() ->
    gen_server:start_link({local, jobs_queue_server}, jobs_queue_server, [], []) .

submit_job(FunctionName, [UniqueId, OpaqueData], ClientSocket, Level) ->
    Identifier = case ClientSocket of % detached or actual socket?
                     no_socket -> lists:flatten(io_lib:format("job@~p:(~p)",[detached,make_ref()])) ;
                     _Other    -> {ok, {Address,Port}} = inet:peername(ClientSocket),
                                  lists:flatten(io_lib:format("job@~p:~p(~p)",[Address,Port,make_ref()]))
                 end,
    JobRequest = #job_request{ identifier = Identifier,
                               function = FunctionName,
                               unique_id = UniqueId,
                               opaque_data = OpaqueData,
                               socket = ClientSocket }, % this can be no_socket if the job is detached
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .

lookup_job(FunctionName) ->
    gen_server:call(jobs_queue_server, {lookup_job, FunctionName}) .


dequeue_job_with_identifier(Identifier) ->
    gen_server:call(jobs_queue_server, {dequeue, Identifier}) .


update_job_status(Identifier, Numerator, Denominator) ->
    gen_server:call(jobs_queue_server, {update_job_status, Identifier, Numerator, Denominator}) .


%% Callbacks


init(State) ->
    {ok, State}.

handle_call({update_job_status, Identifier, Numerator, Denominator}, _From, State) ->
    {Result,StateP} = store:alter(fun(#job_request{ identifier = I } = JobRequest) ->
                                          if
                                              I =:= Identifier -> JobRequest#job_request{ status = {Numerator, Denominator} } ;
                                              true             -> JobRequest
                                          end
                                  end, State),
    {reply, Result, StateP} ;

handle_call({dequeue, Identifier}, _From, State) ->
    case store:dequeue_if(fun(JobRequest) -> JobRequest#job_request.identifier =:= Identifier end, State) of
        {not_found, StateP}  ->  {reply, {error, not_found}, StateP} ;
        {Job, StateP}        ->  {reply, {ok, Job}, StateP}
    end ;

handle_call({submit_job, JobRequest, Level}, _From, State) ->
    {reply, {ok, JobRequest#job_request.identifier}, store:insert({JobRequest#job_request.function, Level}, JobRequest, State)} ;

handle_call({lookup_job, FunctionName}, _From, State) ->
    Found = do_lookup_job(FunctionName, State, [high, normal, low]),
    case Found of
        {Result, StateP} -> {reply, {ok, Result}, StateP} ;
        not_found        -> {reply, not_found, State}
    end .


%% Private functions


do_lookup_job(_FunctionName, Queues, []) ->
    {not_found, Queues} ;

do_lookup_job(FunctionName, Queues, [Level | Levels]) ->
    log:t(["Lookup job for", {FunctionName, Level}, "in", Queues]),
    Result = store:next({FunctionName, Level}, Queues),
    case Result of
        not_found ->
            do_lookup_job(FunctionName, Queues, Levels) ;
        _Else ->
            Result
    end .
