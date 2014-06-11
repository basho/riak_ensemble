-module(pending_dataloss_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ensemble_types.hrl").

-define(REQ_TIMEOUT, 1000).

pending_test_() ->
    {setup, spawn, fun setup/0, fun cleanup/1,
        [{timeout, 30, ?_assert(no_dataloss())}]}.

setup() ->
    Node = node(),
    make_data_dir(),
    application:set_env(riak_ensemble, data_root, data_dir()),
    application:ensure_all_started(riak_ensemble),
    riak_ensemble_manager:enable(),
    [{root, Node}] = riak_ensemble_manager:get_members(root),
    wait_quorum(root).

cleanup(_) ->
    application:stop(riak_ensemble),
    rm_data_dir().

no_dataloss() ->
    create_ensemble(ensemble1, [a, b, c]),
    riak_ensemble_client:kput_once(ensemble1, key1, val1, ?REQ_TIMEOUT),
    {ok, {obj, _, _, _, Val}} = riak_ensemble_client:kget(ensemble1, key1,
        ?REQ_TIMEOUT),
    ?assertEqual(val1, Val),
    replace_peers(ensemble1, [a, b, c], [d, e, f]),
    {ok, {obj, _, _, _, Val2}} = riak_ensemble_client:kget(ensemble1, key1,
        ?REQ_TIMEOUT),
    ?assertEqual(val1, Val2),
    true.

replace_peers(Ensemble, Del0, Add0) ->
    Node = node(),
    Leader = riak_ensemble_manager:get_leader_pid(Ensemble),
    Del = [{del, {P, Node}} || P <- Del0],
    Add = [{add, {P, Node}} || P <- Add0],
    riak_ensemble_peer:update_members(Leader, Del ++ Add, 10000),
    wait_for_transition(Ensemble, Add0).

create_ensemble(Ensemble, PeerNames) ->
    Node = node(),
    Peers = [{Name, Node} || Name <- PeerNames],
    ok = riak_ensemble_manager:create_ensemble(Ensemble, hd(Peers), Peers,
        riak_ensemble_basic_backend, []),
    wait_quorum(Ensemble).

rm_data_dir() ->
    os:cmd("rm -rf " ++ data_dir()).

make_data_dir() ->
    os:cmd("mkdir -p " ++ data_dir()).

data_dir() ->
    "/tmp/ensemble_dataloss_test_data".

wait_for_transition(Ensemble, New) ->
    Node = node(),
    Peers = riak_ensemble_manager:get_members(Ensemble),
    Done = lists:all(fun(P) ->
                         lists:member({P, Node}, Peers)
                     end, New)
           andalso length(Peers) =:= length(New),
    case Done of
        true ->
            ok;
        false ->
            wait_for_transition(Ensemble, New)
    end.

wait_quorum(Ensemble) ->
    case riak_ensemble_manager:check_quorum(Ensemble, ?REQ_TIMEOUT) of
        true ->
            ok;
        false ->
            wait_quorum(Ensemble)
    end.
