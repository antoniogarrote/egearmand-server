-module(jobs_queue_server) .

%% @doc
%% Module with functions handling the queues with job requests
%% for different priority levels.

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, submit_job/5, lookup_job/1, reeschedule_job/1, check_option_for_job/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, update_job_options/2]).
-export([submit_job_from_client_proxy/5]) .


%% Public API


start_link() ->
    gen_server:start_link({local, jobs_queue_server}, jobs_queue_server, job_request, []) .


%% @doc
%% Creates a new job request for a certain FunctionName, and Level from a
%% client connection established from ClientSocket.
submit_job(Background, FunctionName, [UniqueId, OpaqueData], ClientSocket, Level) ->
    Identifier = lists:flatten(io_lib:format("H:~p:~p",[node(),make_ref()])),
    {ok, {Adress,Port}} = inet:peername(ClientSocket),
    ClientProxyId = list_to_atom(lists:flatten(io_lib:format("client@~p:~p:~p",[node(),Adress,Port]))),
    case global:whereis_name(ClientProxyId) of
        undefined -> client_proxy:start_link(ClientProxyId, ClientSocket) ;
        _PID      -> dont_care
    end,
    JobRequest = #job_request{ identifier = Identifier,
                               function = FunctionName,
                               unique_id = UniqueId,
                               opaque_data = OpaqueData,
                               level = Level,
                               background = Background,
                               client_socket_id  = ClientProxyId },
    gen_server:call(jobs_queue_server, {submit_job, JobRequest, Level}) .


%% @doc
%% Creates a new job request for a certain FunctionName, and Level from a
%% client connection established from ClientSocket.
submit_job_from_client_proxy(ClientProxyId, Background, FunctionName, [UniqueId, OpaqueData], Level) ->
    Identifier = lists:flatten(io_lib:format("H:~p:~p",[node(),make_ref()])),
    JobRequest = #job_request{ identifier = Identifier,
                               function = FunctionName,
                               unique_id = UniqueId,
                               opaque_data = OpaqueData,
                               level = Level,
                               background = Background,
                               client_socket_id  = ClientProxyId },
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
    {reply, {ok, JobRequest#job_request.identifier}, mnesia_store:insert(JobRequest#job_request{queue_key = {JobRequest#job_request.function, Level}}, job_request)} ;

handle_call({update_options, JobHandle, Option}, _From, _State) ->
    Res =  mnesia_store:update(fun(#job_request{ options = Options } = Job) ->
                                       Job#job_request{ options = [Option | Options] }
                               end,
                               JobHandle,
                               job_request),
    {reply, Res, _State} ;

handle_call({lookup_job, FunctionName}, _From, State) ->
    Found = do_lookup_job(FunctionName, State, [high, normal, low]),
    case Found of
        {not_found, StateP} -> {reply, {ok, not_found}, StateP} ;
        {Result, StateP}    -> {reply, {ok, Result}, StateP}
    end .


%% dummy callbacks so no warning are shown at compile time
handle_cast(_Msg, State) ->
    {noreply, State} .


handle_info(_Msg, State) ->
    {noreply, State}.


terminate(shutdown, #connections_state{ socket = _ServerSock }) ->
    ok.


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
