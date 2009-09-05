-module(jobs_queue_server) .

%% @doc
%% Module with functions handling the queues with job requests
%% for different priority levels.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, submit_job/4, lookup_job/1, reeschedule_job/1, check_option_for_job/2]).
-export([init/1, handle_call/3]).


%% Public API


start_link() ->
    gen_server:start_link({local, jobs_queue_server}, jobs_queue_server, job_request, []) .


%% @doc
%% Creates a new job request for a certain FunctionName, and Level from a
%% client connection established from ClientSocket.
submit_job(FunctionName, [UniqueId, OpaqueData], ClientSocket, Level) ->
    Identifier = lists:flatten(io_lib:format("job@~p:(~p)",[node(),make_ref()])),
    ClientProxyId = case ClientSocket of
                        no_socket -> no_socket ;
                        _Other     ->  {ok, {Adress,Port}} = inet:peername(ClientSocket),
                                       ClientProxyIdentifier = list_to_atom(lists:flatten(io_lib:format("client@~p:~p:~p",[node(),Adress,Port]))),
                                       client_proxy:start_link(ClientProxyIdentifier, [ClientSocket, Identifier]),
                                       ClientProxyIdentifier
                    end,
    JobRequest = #job_request{ identifier = Identifier,
                               function = FunctionName,
                               unique_id = UniqueId,
                               opaque_data = OpaqueData,
                               level = Level,
                               client_socket_id = ClientProxyId }, % this can be no_socket if the job is detached
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .


%% @doc
%% Inserts again an existent JobRequest in the queue of jobs
reeschedule_job(#job_request{ level=Level } = JobRequest) ->
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .


%% @doc
%% Updates the options of a scheduled job identified by JobHandle
%% inserting the provided Option.
update_job_options(JobHandle, Option) ->
    gen_server:call(jobs_queue_server, {update_options, JobHandle, Option}) .


%% @doc
%% Searches for some job associated to the function identified byFunctionName in the queues 
%% of jobs for all the priorities: high, normal and low.
%% The first job request associated to FunctionName in the queue of higher priorityis returned.
-spec(lookup_job(atom()) -> {ok, (not_found | #job_request{})}) .

lookup_job(FunctionName) ->
    gen_server:call(jobs_queue_server, {lookup_job, FunctionName}) .


%% @doc
%% Checks if an option is set for a ceratin JobRequest
-spec(check_option_for_job(binary(), #job_request{}) -> (true | false)) .

check_option_for_job(Option, JobRequest) ->
    lists:any(fun(X) -> X =:= Option end,
              JobRequest#job_request.options) .


%% Callbacks


init(State) ->
    {ok, State}.


handle_call({submit_job, JobRequest, Level}, _From, _State) ->
    log:info(["jobs_queue_server, submit job", JobRequest, Level]),
    {reply, {ok, JobRequest#job_request.identifier}, mnesia_store:insert(JobRequest#job_request{queue_key = {JobRequest#job_request.function, Level}}, job_request)} ;

handle_call({update_options, JobHandle, Option}, _From, _State) ->
    log:info(["jobs_queue_server, update_options", JobHandle]),
    Res =  mnesia_store:update(fun(#job_request{ options = Options } = Job) ->
                                       Job#job_request{ options = [Option | Options] }
                               end,
                               JobHandle,
                               job_request),
    {reply, Res, _State} ;

handle_call({lookup_job, FunctionName}, _From, State) ->
    log:info(["jobs_queue_server, lookup_job", FunctionName]),
    Found = do_lookup_job(FunctionName, State, [high, normal, low]),
    case Found of
        {not_found, StateP} -> {reply, {ok, not_found}, StateP} ;
        {Result, StateP}    -> {reply, {ok, Result}, StateP}
    end .


%% Private functions


do_lookup_job(_FunctionName, Queues, []) ->
    {not_found, Queues} ;

do_lookup_job(FunctionName, Queues, [Level | Levels]) ->
    Result = mnesia_store:dequeue(fun(X) -> X#job_request.queue_key == {FunctionName, Level} end, job_request),
    case Result of
        {not_found, _QueueP} ->
            do_lookup_job(FunctionName, Queues, Levels) ;
        _Else ->
            Result
    end .


%% tests


check_option_for_job_test() ->
    Job = #job_request{ options = [<<"tests">>] },
    ?assertEqual(true, check_option_for_job(<<"tests">>, Job)),
    ?assertEqual(false, check_option_for_job(<<"foo">>, Job)) .
