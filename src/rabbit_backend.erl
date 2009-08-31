-module(rabbit_backend) .

-author("Antonio Garrote Hernandez") .

-behaviour(gen_server) .

-include_lib("states.hrl").
-include_lib("rabbitmq_erlang_client/include/amqp_client.hrl").

-export([start/0, start_link/0]) .
-export([init/1, handle_call/3]).


%% public API


%% @doc
%% Starts the rabbitmq application
start() ->
    application:start(sasl) ,
    application:start(mnesia) ,
    application:start(os_mon) ,
    application:start(rabbit) .

start_link() ->
    gen_server:start_link({local, rabbit_backend}, rabbit_backend, [], []) .

create_queue(Options) ->
    gen_server:call(rabbit_backend, {create, Options}) .

publish(Content, Queue, RoutingKey) ->
    gen_server:call(rabbit_backend, {publish, Content, Queue, RoutingKey}) .

consume(F,Queue) ->
    gen_server:call(rabbit_backend, {consume, F, Queue}) .


%% callbacks


init(_Arguments) ->
    Params = #amqp_params{ username = configuration:rabbit_user(),
                           password = configuration:rabbit_password() },
    ConnectionPid = amqp_connection:start_direct(Params),
    {Channel, Ticket} = channel_setup(ConnectionPid),
    { ok, #rabbit_queue_state{ connection = ConnectionPid, channel = Channel, ticket = Ticket } } .


handle_call({create, Options}, _From, State) ->
    log:t(1),
    AlreadyDeclared = proplists:is_defined(proplists:get_value(name,Options),State#rabbit_queue_state.queues),
    log:t(2),
    if AlreadyDeclared =:= false ->
            log:t(3),
            try declare_queue(Options, State) of
                Queue     -> { reply, ok, State#rabbit_queue_state{ queues = [ Queue | State#rabbit_queue_state.queues ] } }
            catch
                _Exception -> { reply, error, State }
            end ;
       true -> {reply, ok, State}
    end ;

handle_call({publish, Content, Queue, BindingKey}, _From, State) ->
    AlreadyDeclared = proplists:is_defined(Queue,State#rabbit_queue_state.queues),
    if AlreadyDeclared =:= false ->
            try declare_queue([{queue, Queue, {bindkey, Queue}}], State) of
                Queue     -> publish_content(Content, Queue, BindingKey, State),
                             { reply, ok, State#rabbit_queue_state{ queues = [ Queue | State#rabbit_queue_state.queues ] } }
            catch
                _Exception -> { reply, error, State }
            end ;
       true -> publish_content(Content, Queue, BindingKey, State),
               {reply, ok, State}
    end ;

handle_call({consume, Function, Queue}, _From, State) ->
    AlreadyDeclared = proplists:is_defined(Queue,State#rabbit_queue_state.queues),
    if AlreadyDeclared =:= false ->
            try declare_queue([{queue, Queue}, {bindkey, Queue}], State) of
                Queue     -> register_consumer(Function, Queue, State),
                             { reply, ok, State#rabbit_queue_state{ queues = [ Queue | State#rabbit_queue_state.queues ] } }
            catch
                _Exception -> { reply, error, State }
            end ;
       true -> register_consumer(Function, Queue, State),
               {reply, ok, State}
    end .


%% private API


%% @doc
%% Default values for channel configuration
%% options of RabbitMQ
channel_default_values() ->
    [ {realm, <<"gearman">>},
      {exclusive, false},
      {passive, true},
      {active, true},
      {write, true},
      {read, true} ] .

%% @doc
%% Default values for exchange configuration
%% options of RabbitMQ
exchange_default_values() ->
    [ {type, <<"topic">>},
      {passive, false},
      {durable, false},
      {auto_delete, false},
      {internal, false},
      {nowait, false},
      {arguments, []} ] .


%% @doc
%% sets up a new channel over an already stablished connection
%% using the configuration values and the default ones
channel_setup(Connection) ->
    Configuration = configuration:rabbit_channel_configuration(),
    Defaults = channel_default_values(),
    Access = #'access.request'{ realm = proplists_extensions:get_value(realm,  Configuration, Defaults),
                                exclusive = proplists_extensions:get_value(exclusive,  Configuration, Defaults),
                                passive = proplists_extensions:get_value(passive,  Configuration, Defaults),
                                active = proplists_extensions:get_value(active,  Configuration, Defaults),
                                write = proplists_extensions:get_value(write,  Configuration, Defaults),
                                read = proplists_extensions:get_value(read,  Configuration, Defaults) },
    Channel = amqp_connection:open_channel(Connection),
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Channel, Access),
    {Channel, Ticket} .



%% @doc
%% Creates a new queue
declare_queue(Options, State) ->
    log:t(4),
    Q = proplists:get_value(name, Options),
    log:t(5),
    X = <<"x">>,
    log:t(6),
    BindKey = proplists:get_value(bind_key, Options),

    QueueDeclare = #'queue.declare'{ticket = State#rabbit_queue_state.ticket,
                                    queue = Q,
                                    passive = false,
                                    durable = false,
                                    exclusive = false,
                                    auto_delete = false,
                                    nowait = false,
                                    arguments = []},
    #'queue.declare_ok'{queue = Q,
                        message_count = _MessageCount,
                        consumer_count = _ConsumerCount}  = amqp_channel:call(State#rabbit_queue_state.channel, QueueDeclare),

    ExchangeDeclare = #'exchange.declare'{ticket = State#rabbit_queue_state.ticket,
                                          exchange = X,
                                          type= <<"topic">>,
                                          passive = false,
                                          durable = false,
                                          auto_delete=false,
                                          internal = false,
                                          nowait = false,
                                          arguments = []},

    #'exchange.declare_ok'{} = amqp_channel:call(State#rabbit_queue_state.channel, ExchangeDeclare),

    QueueBind = #'queue.bind'{ticket = State#rabbit_queue_state.ticket,
                              queue = Q,
                              exchange = X,
                              routing_key = BindKey,
                              nowait = false,
                              arguments = []},

    #'queue.bind_ok'{} = amqp_channel:call(State#rabbit_queue_state.channel, QueueBind),
    {Q, #rabbit_queue{ queue = Q, exchange = X, key = BindKey }} .


publish_content(Content, Queue, BindingKey, State) ->
    QueueState = proplists:get_value(Queue, State#rabbit_queue_state.queues),
    BasicPublish = #'basic.publish'{ticket = State#rabbit_queue_state.ticket,
                                    exchange = QueueState#rabbit_queue.exchange,
                                    routing_key = BindingKey,
                                    mandatory = false,
                                    immediate = false},
    ContentP = #content{class_id = 60,
                        properties = amqp_util:basic_properties(),
                        properties_bin = none,
                        payload_fragments_rev = [Content]
                       },
    amqp_channel:cast(State#rabbit_queue_state.channel, BasicPublish, ContentP).


register_consumer(Function, Queue, State) ->
    BasicConsume = #'basic.consume'{ticket = State#rabbit_queue_state.ticket,
                                    queue = Queue,
                                    consumer_tag = <<"">>,
                                    no_local = false,
                                    no_ack = true,
                                    exclusive = false,
                                    nowait = false},

    ConsumerPid = spawn_consumer(Function, State#rabbit_queue_state.channel),
    #'basic.consume_ok'{} = amqp_channel:call(State#rabbit_queue_state.channel, BasicConsume, ConsumerPid),
    ConsumerPid .


spawn_consumer(_Function, Channel) ->
    spawn(fun() ->
                  %% If the registration was sucessful, the consumer will
                  %% be notified

                  receive
                      #'basic.consume_ok'{consumer_tag = ConsumerTag} ->  ok
                  end,

                  ConsumeLoop = fun(F) ->
                                        %% When a message is routed to the queue, it will be
                                        %% delivered to this consumer

                                        receive
                                            {#'basic.deliver'{delivery_tag = _DeliveryTag}, Content} ->

                                                #content{payload_fragments_rev = [Payload]} = Content,
                                                %% TODO pass the value read to the Function passed as a parameter
                                                io:format("Message received: ~p~n", [Payload]),
                                                F(F) ;
                                            exit ->

                                                %% After the consumer is finished interacting with the
                                                %% queue, it can deregister itself

                                                BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag,
                                                                              nowait = false},
                                                #'basic.cancel_ok'{consumer_tag = ConsumerTag}
                                                    = amqp_channel:call(Channel,BasicCancel)
                                        end
                                end,
                  ConsumeLoop(ConsumeLoop)
          end) .
