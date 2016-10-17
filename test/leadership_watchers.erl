-module(leadership_watchers).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    ens_test:run(fun scenario/0, 40).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),

    Pid = riak_ensemble_manager:get_leader_pid(root),

    ?assertEqual(0, length(riak_ensemble_peer:get_watchers(Pid))),
    ?debugMsg("Watching leader"),
    riak_ensemble_peer:watch_leader_status(Pid),
    ?assertEqual(1, length(riak_ensemble_peer:get_watchers(Pid))),
    ?debugMsg("Waiting for is_leading notification"),
    wait_status(is_leading, Pid),

    ?debugMsg("Stopping watching leader"),
    riak_ensemble_peer:stop_watching(Pid),
    ?assertEqual(0, length(riak_ensemble_peer:get_watchers(Pid))),

    ?debugMsg("Starting watching leader again"),
    riak_ensemble_peer:watch_leader_status(Pid),
    ?assertEqual(1, length(riak_ensemble_peer:get_watchers(Pid))),
    wait_status(is_leading, Pid),

    ?debugMsg("Suspending leader, and waiting for new leader to be elected"),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),

    ?debugMsg("Resuming former leader, and waiting for is_not_leading notification"),
    erlang:resume_process(Pid),
    wait_status(is_not_leading, Pid),

    ?debugMsg("Watching leader in external process"),
    Watcher = spawn_link(fun() -> watcher(Pid) end),
    wait_until_n_watchers(2, Pid),

    ?debugMsg("Killing external watcher process and checking peer state"),
    Watcher ! die,
    wait_until_n_watchers(1, Pid).

wait_status(Status, Pid) ->
    receive
        {Status, Pid, _, _, _} ->
            ok
    after
        5000 ->
            throw(timeout_waiting_for_leader_status)
    end.

%% Just a fun to spawn a process that will exit when we tell it to, so we
%% can test that the watcher gets removed from the ensemble peer state when
%% a watcher process dies.
watcher(PeerPid) ->
    riak_ensemble_peer:watch_leader_status(PeerPid),
    receive
        die ->
            ok
    end.

wait_until_n_watchers(N, Pid) ->
    WatcherCountCheck = fun() -> N =:= length(riak_ensemble_peer:get_watchers(Pid)) end,
    ?assertEqual(ok, ens_test:wait_until(WatcherCountCheck)).
