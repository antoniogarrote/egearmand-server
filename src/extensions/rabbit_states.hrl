-record(rabbit_queue_state, { connection :: pid(),
                              channel,
                              ticket,
                              queues = [] }) .

-record(rabbit_queue, { queue, exchange, key}) .
