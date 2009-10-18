-module(configuration) .

%% @doc
%% Configuration of the egearmand server

-author("Antonio Garrote Hernandez") .

-export([on_worker_failure/0, gearmand_nodes/0, persistent_queues/0, backup_gearmand_nodes/0]) .
-export([rabbit_user/0, rabbit_password/0, rabbit_exchange_configuration/0, rabbit_channel_configuration/0]) .
-export([extensions/0]) .


%% @doc
%% The RabbitMQ user
rabbit_user() ->
    <<"guest">> .


%% @doc
%% The RabbitMQ password
rabbit_password() ->
    <<"guest">> .


%% @doc
%% RabbitMQ channel configuration
rabbit_channel_configuration() ->
    [ {realm, <<"gearman">>},
      {exclusive, false},
      {passive, true},
      {active, true},
      {write, true},
      {read, true} ] .


%% @doc
%% RabbitMQ exchange configuration
rabbit_exchange_configuration() ->
    [ {type, <<"topic">>},
      {passive, false},
      {durable, false},
      {auto_delete, false},
      {internal, false},
      {nowait, false},
      {arguments, []} ] .


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
    {ok, Nodes} = application:get_env(egearmand,nodes),
    [node() | Nodes] .


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


%% @doc
%% The list of installed extensions
-spec(extensions() -> [atom()]) .
extensions() ->
    [] . % [ rabbitmq_extension ] .
