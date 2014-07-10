%% Various pure tests
-module(ensemble_tests_pure).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TEST(X), {timeout, 60, {test, ?MODULE, X}}).

run_test_() ->
    [?TEST(test_monotonic_time)].

test_monotonic_time() ->
    {ok, N1} = riak_ensemble_clock:monotonic_time(),
    {ok, M1} = riak_ensemble_clock:monotonic_time_ms(),
    timer:sleep(1000),
    {ok, N2} = riak_ensemble_clock:monotonic_time(),
    {ok, M2} = riak_ensemble_clock:monotonic_time_ms(),
    ?assert((N2 - N1) >= 1000000000), 
    ?assert((M2 - M1) >= 1000), 
    ok.
