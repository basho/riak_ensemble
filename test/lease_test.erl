-module(lease_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    ens_test:start(3),
    {ok, _} = ens_test:kput(<<"test">>, <<"test">>),

    %% Test with lease trusted
    {ok, _} = ens_test:kget(<<"test">>), 

    %% Test with lease not trusted
    ok = application:set_env(riak_ensemble, trust_lease, false),
    {ok, _} = ens_test:kget(<<"test">>),

    %% Test with lease not trusted and followers intercepted to
    %% nack epoch check.
    rt_intercept:add(node(), {riak_ensemble_peer, [{{check_epoch,3}, check_epoch_false}]}),
    {error, timeout} = ens_test:kget(<<"test">>),

    %% Test with lease trusted again
    ok = application:set_env(riak_ensemble, trust_lease, true),
    %% Because of error above, leader may have changed. Wait until stable.
    ens_test:wait_stable(root),
    %% Read twice because leader change forces first read to rewrite, which
    %% ignores the lease entirely.
    {ok, _} = ens_test:kget(<<"test">>),
    {ok, _} = ens_test:kget(<<"test">>),

    %% Test with simulated expired lease
    ok = application:set_env(riak_ensemble, follower_timeout, 1000),
    ok = application:set_env(riak_ensemble, lease_duration, 0),
    timer:sleep(1000),
    {error, _} = ens_test:kget(<<"test">>),

    %% Remove intercept and test that all is well
    rt_intercept:add(node(), {riak_ensemble_peer, [{{check_epoch,3}, check_epoch}]}),
    ens_test:wait_stable(root), 
    {ok, _} = ens_test:kget(<<"test">>),
    {ok, _} = ens_test:kget(<<"test">>),

    ok.
