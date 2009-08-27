
-record(connections_state, { socket :: gen_tcp:socket(),
                             configuration = []:: list(),
                             worker_proxies = []:: list() }) .

-record(worker_proxy_state,{ identifier :: binary(),
                             functions = [] :: [binary()],
                             socket :: gen_tcp:socket() }) .

-record(job_request, { identifier :: string(),
                       function :: binary(),
                       unique_id :: binary(),
                       opaque_data :: binary(),
                       socket :: gen_tcp:socket() }) .
