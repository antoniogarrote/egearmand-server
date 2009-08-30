-module(configuration) .

-author("Antonio Garrote Hernandez") .

-export([rabbit_user/0, rabbit_password/0, on_worker_failure/0, gearmand_nodes/0, persistent_queues/0, backup_gearmand_nodes/0]) .

rabbit_user() ->
    <<"gearman">> .


rabbit_password() ->
    <<"gearman">> .


%% @doc
%% What to do if a worker fails while executing a task.
%% If the value is set to reeschedule the task will be
%% queued again.
%% If the value is set to none, it will be discarded.
-spec(on_worker_failure() -> reeschedule | none) .
on_worker_failure() ->
    reeschedule .


%% @doc
%% Defines the cluster of erlang nodes where
%% the gearman server is going to run.
-spec(gearmand_nodes() -> [node()]) .
gearmand_nodes() ->
    [node()] .


%% @doc
%% If using persitent job queues, nodes where to
%% to store disc copies.
-spec(backup_gearmand_nodes() -> [node()]) .
backup_gearmand_nodes() ->
    [node()] .


%% @doc
%% Specifies if the Gearman queues should be 
%% persistent between servers restarts.
-spec(persistent_queues() -> true | false) .
persistent_queues() -> false .
