-module(read_tombstone_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%% This test specifically targets an optimization in riak_ensemble that is
%% intended to avoid writing tombstones when possible on reads that return
%% notfound. Normally reads that return notfound require a tombstone to be
%% written in case there's a partial write somewhere in the ensemble that
%% hasn't been read yet. If we wait an extra, say, 1ms though, then it's
%% very likely we will see replies from every peer in the ensemble, in
%% which case we can definitively see whether any partial writes exist or
%% not, and potentially avoid the need to write a tombstone.

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),
    application:set_env(riak_ensemble, notfound_read_delay, 3000),

    Peers = riak_ensemble_manager:get_members(root),
    Leader = riak_ensemble_manager:get_leader(root),
    [Follow1, Follow2] = Peers -- [Leader],

    ?debugMsg("Running kget on a nonexistent key"),
    {ok, _} = ens_test:kget(<<"test">>),

    ?debugMsg("Testing that no peers have tombstones"),
    ?assert(is_notfound(Leader, <<"test">>)),
    ?assert(is_notfound(Follow1, <<"test">>)),
    ?assert(is_notfound(Follow2, <<"test">>)),

    ?debugMsg("Running kget with one member suspended"),
    application:set_env(riak_ensemble, notfound_read_delay, 0),
    Peer2Pid = riak_ensemble_manager:get_peer_pid(root, Follow2),
    erlang:suspend_process(Peer2Pid),
    {ok, _} = ens_test:kget(<<"test2">>),
    erlang:resume_process(Peer2Pid),

    ?debugMsg("Testing that active peers have tombstones"),
    ?assertNot(is_notfound(Leader, <<"test2">>)),
    ?assertNot(is_notfound(Follow1, <<"test2">>)),

    ok.

%% This can be used to check for tombstones, because if a key
%% has a tombstone, we'll see an object returned that wraps
%% the 'notfound' value rather than just the atom 'notfound' alone.
is_notfound(Member, Key) ->
    Pid = riak_ensemble_manager:get_peer_pid(root, Member),
    Res = riak_ensemble_peer:debug_local_get(Pid, Key),
    Res =:= notfound.
