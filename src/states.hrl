
-record(connections_state, { socket :: gen_tcp:socket(),
                             configuration = []:: list(),
                             worker_proxies = []:: list() }) .

-record(job_request, { identifier :: string(),
                       background :: true | false,
                       function :: binary(),
                       queue_key :: { binary(), atom() },
                       unique_id :: binary(),
                       opaque_data :: binary(),
                       level :: low | normal | high,
                       status = {0,0} :: {integer(), integer()},
                       options = [] :: {[binary()]},
                       client_socket_id :: atom() }) .

-record(worker_proxy_info, { identifier :: binary(),
                             ip :: string(),
                             worker_id = "none" :: binary(),
                             functions = [] :: [binary()],
                             current :: #job_request{} | none }) .

-record(worker_proxy_state,{ identifier :: binary(),
                             functions = [] :: [binary()],
                             socket :: gen_tcp:socket(),
                             worker_id :: binary(),
                             current = none :: #job_request{} | none }) .

-record(function_register, { table_key :: {atom(), atom()},
                             reference :: atom(),
                             function_name :: binary() }) .
