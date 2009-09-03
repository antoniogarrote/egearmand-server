-module(rabbitmq_extension) .

-author("Antonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl").

-export([start/0, connection_hook_for/1, stop/0, entry_point/2]) .


% callbacks


% @doc
% Starts the extension
start() ->
    rabbit_backend:start(),
    rabbit_backend:start_link() .


% @doc
% We state which messages we are interested to process
-spec(connection_hook_for(atom()) -> boolean()) .

connection_hook_for(Msg) ->
    log:t(["Checking hook for", Msg]),
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", _Unique, _Options]}  -> true ;
        {submit_job, ["/egearmand/rabbitmq/publish", _Unique, _Options]}  -> true ;
        {submit_job, ["/egearmand/rabbitmq/consume", _Unique, _Options]}  -> true ;
        _Other                                                            -> false
    end .


% @doc
% We state which messages we are interested to process
-spec(entry_point(atom(),any()) -> boolean()) .

entry_point(Msg, Socket) ->
    log:t(["Entry point of the extensions", Msg, Socket]),
    log:t([1,Msg]),
    log:t([2,Socket]),
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", _Unique, Options]}   -> process_queue_creation(Options) ;
        {submit_job, ["/egearmand/rabbitmq/publish", _Unique, Options]}   -> process_queue_publish(Options) ;
        {submit_job, ["/egearmand/rabbitmq/consume", _Unique, Options]}   -> process_queue_consume(Options, Socket) ;
        _Other                                                            -> false
    end .


%% @doc
%% Stops the extension
stop() ->
    ok .


%% Implementation


process_queue_creation(Options) ->
    log:t(["options: ",Options]),
    log:t(["Lets decode",rfc4627:decode(Options)]),
    case rfc4627:decode(Options) of
        {ok, {obj, DecodedOptions}, []} -> rabbit_backend:create_queue(fix_json_object(DecodedOptions)) ;
        _Other                          -> true
    end .

process_queue_publish(Options) ->
    case rfc4627:decode(Options) of
        {ok, {obj, DecodedOptions}, []} -> rabbit_backend:publish(proplists:get_value(content, fix_json_object(DecodedOptions)),
                                                                  proplists:get_value(name, fix_json_object(DecodedOptions)),
                                                                  proplists:get_value(routing_key, fix_json_object(DecodedOptions))) ;
        _Other                          -> true
    end .

process_queue_consume(Options, Socket) ->
    %NewIdentifier = list_to_atom(lists:flatten(io_lib:format("notification@~p",[make_ref()]))),
    HandlerFunction = fun(Notification)  ->
                              Response = protocol:pack_response(work_data, {"/egearmand/rabbitmq/consume", Notification}),
                              gen_tcp:send(Socket,Response)
                      end,
    case rfc4627:decode(Options) of

        {ok, {obj, DecodedOptions}, []} ->    rabbit_backend:consume(HandlerFunction,
                                                                     proplists:get_value(name, fix_json_object(DecodedOptions))) ;
        _Other -> true
    end .


%% Transforms the JSON tuple obtained from rfc4627
%% into the options list accepted by rabbitmq
fix_json_object(JsonObject) ->
    fix_json_object(JsonObject,[]) .

fix_json_object([], NewPairs) ->
    NewPairs ;

fix_json_object([{K,V} | T], NewPairs) ->
    fix_json_object(T,[{list_to_atom(K), V} | NewPairs]) .


%% tests


fix_json_object_test() ->
    Result = fix_json_object([{"name",<<"test_queue">>}, {"routing_key",<<"test_queue">>}]),
    ?assertEqual([{routing_key, <<"test_queue">>},{name, <<"test_queue">>}], Result) .
