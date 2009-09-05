-module(proplists_extensions) .

%% @doc
%% Additional functions for manipulation fo proplists.

-author("Antonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl") .

-export([get_value/3]) .


%% @doc
%% Gets a value from a proplists using the values from Defaults
%% proplist as possible default values
-spec(get_value(any(), [{any(), any()}], [{any(), any()}]) -> {any(), any()}) .

get_value(Key, List, Defaults) ->
    Value = proplists:get_value(Key, List),
    case Value of
        undefined ->  proplists:get_value(Key, Defaults) ;
        _Other    ->  Value
    end .


%% tests


get_value_test() ->
    ?assertEqual(get_value(a, [], [{a,default}]), default) .
