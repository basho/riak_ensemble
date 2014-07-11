-module(synctree_path_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    ens_test:run(fun scenario/0, 40).

scenario() ->
    rt_intercept:add(node(), {riak_ensemble_basic_backend,
                              [{{synctree_path,2}, synctree_path_shared}]}),
    ens_test:start(3),
    ens_test:wait_stable(root),
    {ok, _} = ens_test:kput(<<"test">>, <<"test">>),
    {ok, _} = ens_test:kget(<<"test">>),
    ens_test:kget(cluster_state),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),
    {ok, _} = ens_test:kget(<<"test">>),
    erlang:resume_process(Pid),
    {ok, _} = ens_test:kget(<<"test">>),
    ok.
