-module(proplists_extensions) .

-author("Antonio Garrote Hernandez") .

-include_lib("eunit/include/eunit.hrl") .


get_value(Key, List, Defaults) ->
    Value = proplists:get_value(Key, List),
    case Value of
        undefined ->  proplists:get_value(Key, Defaults) ;
        _Other    ->  Value
    end .


%% tests


get_value_test() ->
    ?assertEqual(get_value(a, [], [{a,default}]), default) .
