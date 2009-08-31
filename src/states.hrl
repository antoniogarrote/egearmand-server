
-record(connections_state, { socket :: gen_tcp:socket(),
                             configuration = []:: list(),
                             worker_proxies = []:: list() }) .

-record(job_request, { identifier :: string(),
                       function :: binary(),
                       queue_key :: { binary(), atom() },
                       unique_id :: binary(),
                       opaque_data :: binary(),
                       level :: low | normal | high,
                       status = {0,0} :: {integer(), integer()},
                       client_socket_id :: atom() }) .

-record(worker_proxy_state,{ identifier :: binary(),
                             functions = [] :: [binary()],
                             socket :: gen_tcp:socket(),
                             current = none :: #job_request{} | none }) .

-record(function_register, { table_key :: {atom(), atom()},
                             reference :: atom(),
                             function_name :: binary() }) .

-record(rabbit_queue_state, { connection :: pid(),
                              channel,
                              ticket,
                              queues = [] }) .

-record(rabbit_queue, { queue, exchange, key}) .
