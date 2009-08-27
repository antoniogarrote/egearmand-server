-module(gearmand) .

-author("Antonio Garrote Hernandez") .

-export([start/2, cmd_start/1]) .

start(Host, Port) ->
    functions_registry:start_link(),
    jobs_queue_server:start_link(),
    connections:start_link(Host,Port) .

cmd_start([Host, Port]) ->
    start(Host, Port) .
