-module(rabbitmq_extension) .

-author("Antonio Garrote Hernandez") .

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
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", _Options]}  -> true ;
        {submit_job, ["/egearmand/rabbitmq/publish", _Options]}  -> true ;
        {submit_job, ["/egearmand/rabbitmq/consume", _Options]}                  -> true ;
        _Other                                                   -> false
    end .


% @doc
% We state which messages we are interested to process
-spec(entry_point(atom(),any()) -> boolean()) .

entry_point(Msg, Socket) ->
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", Options]}   -> process_queue_creation(Options) ;
        {submit_job, ["/egearmand/rabbitmq/publish", Options]}   -> process_queue_publish(Options) ;
        {submit_job, ["/egearmand/rabbitmq/consume", Options]}   -> process_queue_consume(Options, Socket) ;
        _Other                                                   -> false
    end .


%% @doc
%% Stops the extension
stop() ->
    ok .


%% Implementation


process_queue_creation(Options) ->
    case rfc4627:decode(Options) of
        {ok, {obj, DecodedOptions}, []} -> rabbit_backend:create_queue(DecodedOptions) ;
        _Other                          -> true
    end .

process_queue_publish(Options) ->
    case rfc4627:decode(Options) of
        {ok, {obj, DecodedOptions}, []} -> rabbit_backend:create_queue(propslists:get_value(content,DecodedOptions),
                                                                       propslists:get_value(name,DecodedOptions),
                                                                       propslists:get_value(routing_key, DecodedOptions)) ;
        _Other                          -> true
    end .

process_queue_consume(Options, Socket) ->
    %NewIdentifier = list_to_atom(lists:flatten(io_lib:format("notification@~p",[make_ref()]))),
    HandlerFunction = fun(Notification)  ->
                              Response = protocol:pack_response(work_data, {"/egearmand/rabbitmq/notification", Notification}),
                              gen_tcp:send(Socket,Response)
                      end,
    case rfc4627:decode(Options) of

        {ok, {obj, DecodedOptions}, []} ->    rabbit_backend:create_queue(HandlerFunction,
                                                                          propslists:get_value(name,DecodedOptions)) ;
        _Other -> true
    end .
