-module(egearmand_app) .

-author("Antonio Garrote Hernandez") .

-behaviour(application) .

-export([start/2, stop/1]) .

start(_Type, Arguments) ->
    gearmand_supervisor:start_link(Arguments) .

stop(_State) ->
    ok .
