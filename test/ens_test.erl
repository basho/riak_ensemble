-module(ens_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

run(Test) ->
    %% run(Test, 5*60).
    run(Test, 45).

run(Test, Timeout) ->
    {setup,
     fun() ->
             application:load(crypto),
             application:load(riak_ensemble),
             os:cmd("rm -rf test-tmp"),
             application:set_env(riak_ensemble, data_root, "test-tmp"),
             {ok, _} = application:ensure_all_started(riak_ensemble),
             ok
     end,
     fun(_) ->
             application:stop(riak_ensemble)
     end,
     {timeout, Timeout, Test}}.

start() ->
    Node = node(),
    riak_ensemble_manager:enable(),
    [{root, Node}] = riak_ensemble_manager:get_members(root),
    ens_test:wait_stable(root), 
    ok.

start(N) ->
    start(),
    expand(N).

expand(N) ->
    NewMembers = [{X, node()} || X <- lists:seq(2,N)],
    Changes = [{add, Member} || Member <- NewMembers],
    Pid = riak_ensemble_manager:get_leader_pid(root),
    riak_ensemble_peer:update_members(Pid, Changes, 5000),
    ens_test:wait_stable(root),

    Members = [{root, node()} | NewMembers],
    ens_test:wait_members(root, Members),
    ens_test:wait_stable(root),
    ok.

wait_stable(Ensemble) ->
    case check_stable(Ensemble) of
        true ->
            ok;
        false ->
            wait_stable(Ensemble)
    end.

check_stable(Ensemble) ->
    case riak_ensemble_manager:check_quorum(Ensemble, 1000) of
        true ->
            case riak_ensemble_peer:stable_views(Ensemble, 1000) of
                {ok, true} ->
                    true;
                _Other ->
                    false
            end;
        false ->
            false
    end.

wait_members(Ensemble, Expected) ->
    Members = riak_ensemble_manager:get_members(Ensemble),
    case (Expected -- Members) of
        [] ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_members(Ensemble, Expected)
    end.

kput(Key, Val) ->
    riak_ensemble_client:kover(root, Key, Val, 5000).

kget(Key) ->
    riak_ensemble_client:kget(root, Key, 5000).

kget(Key, Opts) ->
    riak_ensemble_client:kget(node(), root, Key, 5000, Opts).

read_until(Key) ->
    case ens_test:kget(Key) of
        {ok, Obj} ->
            Value = riak_ensemble_basic_backend:obj_value(Obj),
            ?assert(Value =/= notfound),
            ok;
        {error, _} ->
            timer:sleep(100),
            read_until(Key)
    end.
