-module(egearmand_app) .

-author("Antonio Garrote Hernandez") .

-behaviour(application) .

-export([start/2, stop/1, show_version/0, show_help/0, cmd_start/0, version/0]) .


start(_Type, Arguments) ->
    ParsedArguments = cmd_line_options(Arguments,[host, port, log, level, check_nodes]),
    ShouldCheckNodes = proplists:get_bool(check_nodes, ParsedArguments),
    IsSlave = proplists:get_bool(slave, ParsedArguments),
    if
        ShouldCheckNodes =:= true   -> check_environment() ;
        ShouldCheckNodes =:= false  -> if
                                           IsSlave =:= false -> gearmand_supervisor:start_link(ParsedArguments) ;
                                           IsSlave =:= true  -> gearmand_slave_supervisor:start_link(ParsedArguments)
                                       end
    end .


stop(_State) ->
    ok .

%% auxiliary functions


%% Checks if all the nodes are up and running
check_environment() ->
    lists:foreach(fun(N) ->
                          Res = net_adm:ping(N),
                          if
                              Res =:= pong -> error_logger:info_msg("Node ~p up~n",[N]) ;
                              Res =:= pang -> error_logger:error_msg("Node ~p down!!!~n",[N])
                          end
                  end,
                  configuration:gearmand_nodes()),
    init:stop() .


%% Starts the server at the given Host and Port.
cmd_start() ->
        application:start(mnesia),
        application:start(egearmand) .



%% Version of the server.
version() ->
    application:load(egearmand),
    {ok,Vsn} = application:get_key(egearmand,vsn),
    Vsn .


%% @doc
%% Parses command line arguments.
cmd_line_options(ApplicationArguments,Arguments) ->
    Options = command_line_options(Arguments, []),
    default_values(ApplicationArguments, Options) .

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
        host        ->  [{host, Value} | Acum] ;
        port        ->  [{port, list_to_integer(Value)} | Acum] ;
        log         ->  [{method, file}, {path, Value} | Acum] ;
        level       ->  [{level, list_to_atom(Value)} | Acum] ;
        check_nodes ->  [{check_nodes, list_to_boolean(Value)} | Acum]
    end .

list_to_boolean("true")  -> true ;
list_to_boolean("false") -> false .

show_version() ->
    io:format("egearmand version ~s~n", [version()]) .

show_help() ->
    io:format("egearmand version ~p~n use: egearmand [-host host] [-port port] [-log path] [-level (debug | info | warning | error)]~n", [version()]) .
