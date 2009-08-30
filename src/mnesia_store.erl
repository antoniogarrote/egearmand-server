-module(mnesia_store) .

-author("Antonio Garrote Hernandez") .

-include_lib("states.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl") .

-export([start/0, create_tables/0, load_tables/0, insert/2, dequeue/2, all/2]) .


start() ->
    Persistent = configuration:persistent_queues(),
    if Persistent =:= false ->
            mnesia:create_schema(configuration:gearmand_nodes())
    end,

    application:start(mnesia),

    if Persistent =:= false ->
            create_tables()
    end,

    load_tables() .


create_tables() ->
    PersistenceCopies = case configuration:persistent_queues() of
                            true -> [{disc_copies, configuration:backup_gearmand_nodes()}];
                            false -> []
                        end,
    mnesia:create_table(job_request, [{index, [#job_request.queue_key]},
                                      {ram_copies, configuration:gearmand_nodes()},
                                      {type, ordered_set},
                                      {attributes, record_info(fields,job_request)}] ++ PersistenceCopies),
    mnesia:create_table(function_register, [{index, [#function_register.function_name]},
                                            {ram_copies, configuration:gearmand_nodes()},
                                            {type, ordered_set},
                                            {attributes, record_info(fields,function_register)}] ++ PersistenceCopies) .

load_tables() ->
    mnesia:wait_for_tables([job_request, function_register], infinity).


%% @doc
%% Inserts a new value in one of the queues identified by Key.
insert(Value, TableName) ->
    log:t(["Mnesia inserting",Value, TableName]),
    mnesia:transaction(fun() -> mnesia:write( Value ) end),
    TableName .


%% @doc
%% retrieves the first element from queue with value Key deleting it
%% from the queue
dequeue(PKey, TableName) ->
    {atomic, Value } = mnesia:transaction(fun() ->
                                                  C = qlc:cursor(qlc:sort(qlc:q([X || X <- mnesia:table(TableName), PKey(X)]))),
                                                  case  qlc:next_answers(C,1) of
                                                      [Item] ->
                                                          mnesia:delete({job_request, Item#job_request.identifier}),
                                                          {Item, TableName} ;
                                                      [] ->
                                                          {not_found, TableName}
                                                  end
                                          end),
    Value .


%% @doc
%% Deletes the object with the given key from the table.
delete(Key, TableName) ->
    mnesia:transaction(fun() -> mnesia:delete({ TableName, Key}) end) .


%% @doc
%% retrieves all the elements for a key
all(P,TableName) ->
    {atomic, Res} = mnesia:transaction(fun() -> qlc:eval( qlc:q([X || X <- mnesia:table(TableName), P(X)])) end),
    Res .


% tests


with_empty_database(T) ->
    mnesia:clear_table(job_request),
    T() .


dequeue_test() ->
    with_empty_database(fun() ->
                                insert(#job_request{identifier=1,
                                                    queue_key = {test, high},
                                                    function=test,
                                                    level= high},
                                       job_request),
                                insert(#job_request{identifier=2,
                                                    queue_key = {test, high},
                                                    function=test,
                                                    level= high},
                                       job_request),
                                {Ret,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end, job_request),
                                ?assertEqual(Ret#job_request.identifier,1),
                                {RetB,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end, job_request),
                                ?assertEqual(RetB#job_request.identifier,2),
                                {RetC,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end,  job_request),
                                ?assertEqual(RetC,not_found)
                        end) .

insertion_test() ->
    with_empty_database(fun() ->
                                Res = insert(#job_request{identifier=1,
                                                          queue_key = {test, high},
                                                          function=test,
                                                          level= high},
                                             job_request),
                                ?assertEqual(Res,job_request),
                                {Ret,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end,  job_request),
                                ?assertEqual(Ret#job_request.identifier,1)
                        end) .


deletion_test() ->
    with_empty_database(fun() ->
                                Res = insert(#job_request{identifier=1,
                                                          queue_key = {test, high},
                                                          function=test,
                                                          level= high},
                                             job_request),
                                ?assertEqual(Res,job_request),
                                {Ret,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end,  job_request),
                                ?assertEqual(Ret#job_request.identifier,1),
                                delete({test, high}, job_request),
                                {Ret2,job_request} = dequeue(fun(V) -> V#job_request.queue_key == {test, high} end,  job_request),
                                ?assertEqual(Ret2, not_found)
                        end) .

all_test() ->
    with_empty_database(fun() ->
                                insert(#job_request{identifier=1,
                                                    queue_key = {test, high},
                                                    function=test,
                                                    level= high},
                                       job_request),
                                insert(#job_request{identifier=2,
                                                    queue_key = {test, high},
                                                    function=test,
                                                    level= high},
                                       job_request),
                                Res = all(fun(V) -> V#job_request.queue_key == {test, high} end, job_request),
                                ?assertEqual(length(Res),2)
                        end) .
