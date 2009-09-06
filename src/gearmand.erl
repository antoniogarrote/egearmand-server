-module(gearmand) .

%% @doc
%% Top level functions for managing the server.

-author("Antonio Garrote Hernandez") .

-export([start/2, cmd_start/1, version/0]) .


%% @doc
%% Starts the server at the given Host and Port.
start(Host, Port) ->
    log:start_link([{method, file}, {level, debug}, {path, "gearmand.log"}]),
    log:info(["Starting egearmand version ", version(), " at ", Host, Port]),
    log:info("starting mnesia backend store"),
    mnesia_store:start(),
    log:info("starting functions_registry"),
    functions_registry:start_link(),
    log:info("starting jobs_queue_server"),
    jobs_queue_server:start_link(),
    log:info("loading extensions"),
    lists:foreach(fun(E) -> erlang:apply(E,start,[]) end,
                  configuration:extensions()),
    log:info("starts receiving incoming connections"),
    connections:start_link(Host,Port) .


%% Starts the server at the given Host and Port.
cmd_start([Host, Port]) ->
    start(Host, Port) .


%% Version of the server.
version() -> "0.4" .
