-module(corrupt_exchange_test).
-compile(export_all).

run_test_() ->
    ens_test:run(fun scenario/0, 120).

scenario() ->
    ens_test:start(3),
    rt_intercept:add(node(), {synctree, [{{m_store,2}, corrupt_segment_all}]}),
    io:format(user, "Leader = ~p~n", [riak_ensemble_manager:get_leader(root)]),
    {ok, _} = ens_test:kput(<<"corrupt">>, <<"test">>),
    io:format(user, "~p~n", [ens_test:kget(<<"corrupt">>)]),
    timer:sleep(10000),
    rt_intercept:add(node(), {synctree, [{{m_store,2}, m_store_normal}]}),
    timer:sleep(60000),
    {ok, _} = ens_test:kget(<<"corrupt">>),
    ok.

%% detect corruption
%% wait for all to be trusted again / corruption heals
%% wait for successful read
%% fail if we ever get notfound / etc

