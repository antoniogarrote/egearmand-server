-module(gearmand_supervisor).

-author("Antonio Garrote Hernandez") .

-behavior(supervisor).


-export([start_link/1]).
-export([init/1]).


start_link(Options) ->
    supervisor:start_link({global, gearmand_supervisor}, gearmand_supervisor, Options).


init(Options) ->
    %% Start the database
    mnesia_store:otp_start(),
    %% children specifications
    LoggerServer = {log,
                    {log, start_link, [Options]},
                    permanent, 5000, worker, [log]},
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
         [LoggerServer, FunctionsRegistry, JobsQueueServer, ConnectionsServer, AdministrationServer]}}.
