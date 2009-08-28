-module(configuration) .

-author("Antonio Garrote Hernandez") .

-export([rabbit_user/0, rabbit_password/0, on_worker_failure/0]) .

rabbit_user() ->
    <<"gearman">> .


rabbit_password() ->
    <<"gearman">> .


%% @doc
%% what to do if a worker fails while executing a task.
%% If the value is set to reeschedule the task will be
%% queued again.
%% If the value is set to none, it will be discarded.
-spec(on_worker_failure() -> reeschedule | none) .
on_worker_failure() ->
    reeschedule .
