-module(rabbit_backend) .

-author("Antonio Garrote Hernandez") .

-include_lib("rabbitmq_erlang_client/include/amqp_client.hrl").

-export([start/0]) .


start() ->
    application:start(sasl) ,
    application:start(mnesia) ,
    application:start(os_mon) ,
    application:start(rabbit) ,
    Params = #amqp_params{ username = configuration:rabbit_user(),
                           password = configuration:rabbit_password() },
    amqp_connection:start_direct(Params) .
