-module(corrupt_upper_test).
-compile(export_all).

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    ens_test:start(5),
    rt_intercept:add(node(), {synctree, [{{m_store,2}, corrupt_upper}]}),
    {ok, _} = ens_test:kput(<<"corrupt">>, <<"test">>),
    %% rt_intercept:add(node(), {synctree, [{{m_store,2}, m_store_normal}]}),
    io:format(user, "~p~n", [ens_test:kget(<<"corrupt">>)]),
    timer:sleep(20000),
    ok.

%% detect corruption / not-trusted
%% wait until all peers (esp. root) are trusted
%% check some keys?

