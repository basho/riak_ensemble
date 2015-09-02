-module(subscription_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%% This test targets the subscribe/unsubscribe functionality for
%% receiving notifications of ensemble state changes.

-define(TAB, subscription_test_callback_count).

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    init_tables(),
    ?assertEqual(ok, riak_ensemble_manager:subscribe(fun callback/1)),
    ?assertEqual(ok, riak_ensemble_manager:subscribe(fun evil_callback/1)),
    CurrentCount = callback_count(),
    riak_ensemble_manager:enable(),
    wait_until_callback_fires(CurrentCount),
    State = latest_state(),
    ?assert(riak_ensemble_state:enabled(State)),
    ok.

init_tables() ->
    ets:new(?TAB, [named_table, public, set]),
    ets:insert(?TAB, {callback_count, 0}).

latest_state() ->
    [{latest_state, State}] = ets:lookup(?TAB, latest_state),
    State.

callback_count() ->
    [{callback_count, Count}] = ets:lookup(?TAB, callback_count),
    Count.

callback(CS) ->
    ets:insert(?TAB, {latest_state, CS}),
    ets:update_counter(?TAB, callback_count, 1).

evil_callback(_CS) ->
    throw(revenge_of_the_evil_callback).

wait_until_callback_fires(PrevCount) ->
    wait_until_callback_fires(PrevCount, 3000).

wait_until_callback_fires(PrevCount, Retry) ->
    CurrentCount = callback_count(),
    if
        CurrentCount > PrevCount ->
            ok;
        Retry =< 0 ->
            throw(callback_wait_timeout);
        Retry > 0 ->
            timer:sleep(10),
            wait_until_callback_fires(PrevCount, Retry - 1)
    end.
