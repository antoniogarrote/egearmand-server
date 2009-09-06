-module(gearmand) .

%% @doc
%% Top level functions for managing the server.

-author("Antonio Garrote Hernandez") .

-export([start/2, cmd_start/0, version/0]) .
-export([show_version/0, show_help/0]) .

%% @doc
%% Starts the server at the given Host and Port.
start(Host, Port) ->
    start(Host, Port, [{method, file}, {level, debug}, {path, "gearmand.log"}]) .

start(Host, Port, Args) ->
    log:start_link(Args),
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
cmd_start() ->
    ParsedArgs = command_line_options([host, port, log, level]),
    start(proplists:get_value(host, ParsedArgs),
          proplists:get_value(port, ParsedArgs),
          ParsedArgs) .


%% Version of the server.
version() -> "0.5" .


gearmand_default_values() ->
    [ {host, "localhost"},
      {port, 4730},
      {level, info},
      {method, stdout} ] .

%% @doc
%% Parses command line arguments.
command_line_options(Arguments) ->
    Options = command_line_options(Arguments, []),
    default_values(gearmand_default_values(), Options) .

command_line_options([], Acum) -> Acum ;
command_line_options([Flag | T], Acum) ->
    Arg = init:get_argument(Flag),
    case Arg of
        {ok,[[Value]]} ->
            command_line_options(T,process_value(Flag, Value, Acum)) ;
        error ->
            command_line_options(T,Acum)
    end .


default_values([], Acum) ->
    Acum ;
default_values([{Default, DefaultValue} | Defaults], Acum) ->
    case proplists:get_value(Default, Acum) of
        undefined ->  default_values(Defaults, [{Default, DefaultValue} | Acum]) ;
        _Other    ->  default_values(Defaults, Acum)
    end .


process_value(Flag, Value, Acum) ->
    case Flag of
        host ->  [{host, Value} | Acum] ;
        port ->  [{port, list_to_integer(Value)} | Acum] ;
        log  ->  [{method, file}, {path, Value} | Acum] ;
        level -> [{level, list_to_atom(Value)} | Acum]
    end .

show_version() ->
    io:format("egearmand version ~p~n", [version()]) .

show_help() ->
    io:format("egearmand version ~p~n use: egearmand [-host host] [-port port] [-log path] [-level (debug | info | warning | error)]~n", [version()]) .
