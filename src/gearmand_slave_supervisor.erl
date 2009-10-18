-module(gearmand_slave_supervisor).

-author("Antonio Garrote Hernandez") .

-behavior(supervisor).


-export([start_link/1, start_supervisor/1]).
-export([init/1]).


start_supervisor(Options) ->
    gearmand_slave_supervisor:start_link(Options) .

start_link(Options) ->
    error_logger:info_msg("Starting supervisor_slave with options ~p",[Options]),
    supervisor:start_link({local, gearmand_slave_supervisor}, gearmand_slave_supervisor, Options).


init(Options) ->
    %% children specifications
    error_logger:info_msg("init slave supervisor with options ~p",[Options]),
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
         [FunctionsRegistry, JobsQueueServer, ConnectionsServer, AdministrationServer]}}.
