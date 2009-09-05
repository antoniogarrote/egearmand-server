-module(tests) .

-author("Atonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [ fun() -> jobs_queue_server:test() end,
      fun() -> lists_extensions:test() end,
      fun() -> proplists_extensions:test() end,
      fun() -> rabbitmq_extension:test() end,
      fun() -> worker_proxy:test() end,
      fun() -> mnesia_store:start(),
               mnesia_store:test()
      end ] .
