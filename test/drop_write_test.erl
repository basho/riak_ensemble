-module(drop_write_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    rt_intercept:add(node(), {riak_ensemble_basic_backend, [{{put,4}, drop_put}]}),
    ens_test:start(5),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    {ok, _} = ens_test:kput(<<"drop">>, <<"test">>),
    {ok, _} = ens_test:kget(<<"drop">>),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),
    erlang:resume_process(Pid),
    read_until(<<"drop">>),
    ok.

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
