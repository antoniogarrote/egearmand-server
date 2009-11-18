-module(gearmand_supervisor).

-author("Antonio Garrote Hernandez") .

-behavior(supervisor).


-export([start_link/1]).
-export([init/1]).


start_link(Options) ->
    supervisor:start_link({global, gearmand_supervisor}, gearmand_supervisor, Options).


init(Options) ->
    %% Let's start the extensions
    lists:foreach(fun(Elem) ->
                                     erlang:apply(Elem, start, [])
                             end,
                             configuration:extensions()),

    %% Let's start Mnesia in all the nodes
    lists:foreach(fun(N) ->
                          if
                              N =/= node() -> rpc:call(N,application,start,[mnesia]) ;
                              true         -> application:start(mnesia)
                          end
                  end,
                  configuration:gearmand_nodes()),

    %% Starts the database
    mnesia_store:otp_start(),

    %% Starting remote processes
    lists:foreach(fun(N) ->
                          if
                              N =/= node() ->     error_logger:info_msg("Starting supervisor_slave at ~p with options ~p",[N, Options]),
                                                  % Res = rpc:call(N,gearmand_slave_supervisor,start_link,[Options]),
                                                  % Res = rpc:call(N,gearmand_slave_supervisor, start_supervisor, [Options]),
                                                  % Res = spawn(N, gearmand_slave_supervisor, start_link, [Options]),
                                                  % Res = spawn(N, gearmand_slave_supervisor, start_supervisor, [Options]),
                                                  % Res = spawn(N, administration, start_link, []),
                                                  Res = rpc:call(N, application, start, [egearmand]),
                                                  error_logger:info_msg("Result has been ~p",[Res]) ;
                              true         -> none
                          end
                  end,
                  configuration:gearmand_nodes()),

    %% children specifications
    LoggerServer = {log,
                    {log, start_link, [Options]},
                    permanent, 5000, worker, [log]},
    WorkersRegistry = {workers_registry,
                       {workers_registry, start_link, []},
                       permanent, 5000, worker, [workers_registry]},
    FunctionsRegistry = {functions_registry,
                         {functions_registry, start_link, []},
                         permanent, 5000, worker, [functions_registry]},
     JobsQueueServer = {jobs_queue_server,
                        {jobs_queue_server, start_link, []},
                        permanent, 5000, worker, [jobs_queue_server]},
     ConnectionsServer = {connections,
                          {connections, start_link, [proplists:get_value(host, Options),
                                                     proplists:get_value(port, Options)]},
                          permanent, 5000, worker, [connections]},
    AdministrationServer = {administration,
                          {administration, start_link, []},
                          permanent, 5000, worker, [administration]},
    %% supervisor specification
    {ok,{{one_for_all,1,1},
         [LoggerServer, WorkersRegistry, FunctionsRegistry, JobsQueueServer, ConnectionsServer, AdministrationServer]}}.
