-module(mnesia_store) .

%% @doc
%% Creation and manipulation of a Mnesia
%% persistence layer for the gearmand implementation.

-author("Antonio Garrote Hernandez") .

-include_lib("states.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl") .

-export([otp_start/0, start/0, create_tables/0, load_tables/0, insert/2, dequeue/2, all/1, all/2, delete/2, dirty_find/2, update/3]) .
-export([delete_multi/2]) .


%% @doc
%% Starts the mnesia backend, creating the schema and tables if required.
%% The persistence settings are read from the configuration module.
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

otp_start() ->
    Persistent = configuration:persistent_queues(),
    if Persistent =:= false ->
            mnesia:create_schema(configuration:gearmand_nodes())
    end,
    if Persistent =:= false ->
            create_tables()
    end,

    load_tables() .


%% @doc
%% Create the backend tables for the mnesia data base.
create_tables() ->
    PersistenceCopies = case configuration:persistent_queues() of
                            true -> [{disc_copies, configuration:backup_gearmand_nodes()}];
                            false -> []
                        end,
    mnesia:create_table(worker_proxy_info, [{ram_copies, configuration:gearmand_nodes()},
                                            {type, ordered_set},
                                            {attributes, record_info(fields,worker_proxy_info)}] ++ PersistenceCopies),

    mnesia:create_table(job_request, [{index, [#job_request.queue_key]},
                                      {ram_copies, configuration:gearmand_nodes()},
                                      {type, ordered_set},
                                      {attributes, record_info(fields,job_request)}] ++ PersistenceCopies),

    mnesia:create_table(function_register, [{index, [#function_register.function_name]},
                                            {ram_copies, configuration:gearmand_nodes()},
                                            {type, ordered_set},
                                            {attributes, record_info(fields,function_register)}] ++ PersistenceCopies) .


%% @doc
%% Waits until the tables are loaded and ready to use. This function blocks
%% undefinitely.
load_tables() ->
    mnesia:wait_for_tables([worker_proxy_info, job_request, function_register], infinity).


%% @doc
%% looks by key in the table without transaction
dirty_find(Key, TableName) ->
    case mnesia:dirty_read({TableName, Key}) of
        [Elem]  -> Elem ;
        []      -> not_found ;
        Other   -> {error,Other}
    end .


%% @doc
%% updates the one element in the store selecte by Key, using the provided
%% predicate P
update(P, Key, TableName) ->
    F = fun() ->
                case mnesia:read({TableName, Key}) of
                    []      -> not_found;
                    [Found] -> mnesia:write(P(Found))
                end
        end,
    {atomic, Result} = mnesia:transaction(F),
    Result.


%% @doc
%% Inserts a new value in one of the queues identified by Key.
insert(Value, TableName) ->
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
%% Deletes the object with the given key from the table.
delete_multi(Keys, TableName) ->
    mnesia:transaction(fun() ->
                               lists:foreach(fun(K) ->
                                                     mnesia:delete({ TableName, K})
                                             end,
                                             Keys)
                       end).


%% @doc
%% retrieves all the elements for a key
all(TableName) ->
    {atomic, Res} = mnesia:transaction(fun() -> qlc:eval( qlc:q([X || X <- mnesia:table(TableName)]) ) end),
    Res .


%% @doc
%% retrieves all the elements for a key
all(P,TableName) ->
    {atomic, Res} = mnesia:transaction(fun() -> qlc:eval( qlc:q([X || X <- mnesia:table(TableName), P(X)])) end),
    Res .


%% tests


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

dirty_find_test() ->
    with_empty_database(fun() ->
                                Res = insert(#job_request{identifier=1,
                                                          queue_key = {test, high},
                                                          function=test,
                                                          level= high},
                                             job_request),
                                ?assertEqual(Res,job_request),
                                RequestFound = dirty_find(1,  job_request),
                                ?assertEqual(1, RequestFound#job_request.identifier),
                                RequestFound_2 = dirty_find(3,  job_request),
                                ?assertEqual(not_found, RequestFound_2)
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

update_test() ->
    with_empty_database(fun() ->
                                insert(#job_request{identifier=1,
                                                    queue_key = {test, high},
                                                    function=test,
                                                    level= high},
                                       job_request),
                                update(fun(X) -> X#job_request{ level = low } end, 1, job_request),
                                RequestFound = dirty_find(1,  job_request),
                                ?assertEqual(RequestFound#job_request.level,low)
                        end) .
