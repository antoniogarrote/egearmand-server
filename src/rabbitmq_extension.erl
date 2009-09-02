-module(rabbitmq_extension) .

-author("Antonio Garrote Hernandez") .

-export([start/0, connection_hook_for/1, stop/0, entry_point/2]) .


%% callbacks


%% @doc
%% Starts the extension
start() ->
    rabbit_backend:start(),
    rabbit_backend:start_link() .


%% @doc
%% We state which messages we are interested to process
-spec(connection_hook_for(atom()) -> boolean()) .

connection_hook_for(Msg) ->
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", _Options]}  -> true ;
        {submit_job, ["/egearmand/rabbitmq/publish", _Options]}  -> true ;
        {can_do, "/egearmand/rabbitmq/consume"}                  -> true ;
        _Other                                                   -> false
    end .


%% @doc
%% We state which messages we are interested to process
-spec(entry_point(atom(),any()) -> boolean()) .

entry_point(Msg, _Socket) ->
    case Msg of
        {submit_job, ["/egearmand/rabbitmq/declare", Options]}   -> process_queue_creation(Options) ;
        {submit_job, ["/egearmand/rabbitmq/publish", _Options]} -> true ;
        {can_do, "/egearmand/rabbitmq/consume"}                 -> true ;
        _Other                                                  -> false
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
