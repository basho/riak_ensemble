-module(replace_members_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 60). %% seconds
run_test_() ->
    ens_test:run(fun scenario/0, ?TIMEOUT).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),
    {ok, V} = ens_test:kput(<<"test">>, <<"test">>),
    {ok, V} = ens_test:kget(<<"test">>),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    Adds = [{I, node()} || I <- [4,5,6]],
    Dels = [{I, node()} || I <- [root, 2, 3]],
    Changes = [{add, Member} || Member <- Adds] ++
              [{del, Member} || Member <- Dels],
    ?debugMsg("Replacing Root Members: root/2/3 -> 4/5/6"),
    ok = riak_ensemble_peer:update_members(Pid, Changes, 10000),

    ?debugMsg("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Adds),
    ens_test:wait_stable(root),
    ?debugMsg("Done waiting for root ensemble to stabilize"),

    %% This fails because we sync metadata trees, but not the data itself.
    %% Riak KV hands off data before the transition so this shouldn't happen in
    %% practice.
    ?debugMsg("Performing failing get"),
    {error, failed} = ens_test:kget(<<"test">>),

    %% The failure above causes the leader to step down.  We need to wait for a
    %% new election or the call to update_members/3 below will fail.
    ?debugMsg("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Adds),
    ens_test:wait_stable(root),
    ?debugMsg("Done waiting for root ensemble to stabilize"),

    ?debugMsg("Replacing root members with original members"),
    Changes2 = [{add, M} || M <- Dels] ++ [{del, M} || M <- Adds],
    Leader = riak_ensemble_manager:get_leader_pid(root),
    ok = riak_ensemble_peer:update_members(Leader, Changes2, 10000),

    ?debugMsg("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Dels),
    ens_test:wait_stable(root),
    ?debugMsg("Done waiting for root ensemble to stabilize"),

    %% The following get succeeds because the data is still stored on root/2/3
    ?debugMsg("Performing successful get"),
    {ok, _} = ens_test:kget(<<"test">>),
    ok.
