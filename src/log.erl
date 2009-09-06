-module(log).

%% @doc
%% Logging server

-author("Antonio Garrote Hernandez").

-behaviour(gen_server) .

-import(io).

-export([t/1,p/1,error/1, info/1, warning/1, debug/1, ot/1]).
-export([start_link/1, msg/2]).
-export([init/1, handle_cast/2]).


%% Public API


%% @doc
%% Creates the logger server.
%% Options are: method:(stdout | file),
%%              level:([debug, info, warning, error])
%%              path: string()
start_link(Args) ->
    gen_server:start_link({global, egearmand_logger}, log, Args, []) .


ot(Msg) ->
    io:format("~n*** trace: ~p~n",[Msg]),
    Msg .


%% @doc
%% Traces code Msg returning Msg.
-spec(t(any()) -> any()) .

t(Msg) ->
    Msg .


%% @doc
%% Prints the message Msg.
p(Msg) ->
    io:format("~n*** print: ~p~n",[Msg]) .


%% @doc
%% Logs message Msg with error log level.
error(Msg) ->
    msg(error, Msg) .

%% @doc
%% Logs message Msg with debug log level.
debug(Msg) ->
    msg(debug, Msg) .


%% @doc
%% Logs message Msg with error info level.
info(Msg) ->
    msg(info, Msg) .


%% @doc
%% Logs message Msg with error warning level.
warning(Msg) ->
    msg(warning, Msg) .


%% @doc
%% Sends a log message to the logger server with the given log level
msg(Level, Msg) ->
    MsgP = io_lib:format("~n*** ~p: ~n~p~n***~n",[Level,Msg]),
    gen_server:cast({global, egearmand_logger},{Level, MsgP}) .


%% Callbacks


init(Options) ->
    LoggerMethod = proplists:get_value(method, Options),
    State = if LoggerMethod =:= file ->
                    case file:open(proplists:get_value(path,Options), [write, raw]) of
                        {ok, F}         -> [{file, F}, {method, file}] ;
                        {error, _Reason} -> [{method, none}]
                    end ;
               LoggerMethod =:= stdout ->
                    [{method, stdout}]
            end,
    {ok, [{level, proplists:get_value(level, Options)} | State]} .


handle_cast({Level, Msg}, State) ->
    ShouldLog = should_log_p(Level,State),
    if  ShouldLog -> process(Msg, State) end,
    {noreply, State} .


%% private functions


%% @doc
%% Tests if Level is greater or equal than the default logging
%% level stored in Options.
should_log_p(Level, Options) ->
    Levels = [debug, info, warning, error],
    DefaultLevel = proplists:get_value(level,Options),
    IndexLevel = lists_extensions:index(Level, Levels),
    IndexDefault = lists_extensions:index(DefaultLevel, Levels),
    if (IndexLevel =:= not_found) or (IndexDefault =:= not_found) ->
            false ;
       true ->
            if Level <  DefaultLevel -> false ;
               Level >= DefaultLevel -> true
            end
    end .


%% @doc
%% Writes a message using the method stored in State.
process(Msg, Options) ->
    Method = proplists:get_value(method, Options),
    case Method of
        file      -> File = proplists:get_value(file, Options),
                     file:write(File, Msg) ;
        stdout    -> io:format(Msg)
    end .
