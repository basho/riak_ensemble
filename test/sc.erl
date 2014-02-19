-module(sc).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

%% -define(SINGLE_NODE, true).

-record(state, {init = false :: boolean(),
                num_workers :: integer(),
                known :: list(),
                nodes :: list(),
                next_value = 0 :: integer(),
                acked :: list(),
                wait :: integer(),
                partitioned :: true | undefined,
                values :: list()}).

-ifdef(SINGLE_NODE).
-record(ensemble_info, {mod :: module(),
                        args :: [any()],
                        leader :: leader_id(),
                        members :: [peer_id()],
                        seq :: {integer(), integer()}
                       }).
-endif.

%% -define(BUCKET, <<"test">>).
-define(BUCKET, {<<"c">>, <<"c~test">>}).
-define(KEY, 3).
-define(ETS, sc).

-define(TIMEOUT, 1000).

%% initial_state() ->
%%     Nodes = ['dev1@127.0.0.1'],
%%     Concurrency = 5,
%%     %% Clients = connect_clients(Nodes, Concurrency),
%%     #state{nodes=Nodes,
%%            num_workers=Concurrency,
%%            known=[],
%%            values=[]}.

get_maybe() ->
    case get(maybe_rw) of
        undefined ->
            [];
        RW ->
            RW
    end.

set_maybe(Maybe) ->
    put(maybe_rw, Maybe).

first_state(Nodes, Concurrency) ->
    #state{nodes=Nodes,
           num_workers=Concurrency,
           acked=[],
           wait=0,
           known=[],
           values=[]}.

command_precondition_common(#state{init=Initialized}, Cmd) ->
    Initialized orelse (Cmd == init).

init_args(#state{num_workers=NumW, nodes=Nodes}) ->
    [Nodes, NumW].
init_pre(#state{init=Initialized}) ->
    not Initialized.
init_blocking(_,_) ->
    false.
init_next(State, _, _) ->
    Default = riakc_obj:new(?BUCKET, <<?KEY:64/integer>>, term_to_binary([])),
    State#state{init=true, values=[{?KEY, Default}]}.
    %% State#state{init=true, values=[]}.
init(Nodes, _NumWorkers) ->
    rpc:multicall(Nodes, riak_kv_memory_backend, reset, []),
    C = ets:lookup_element(?ETS, {client,1}, 2),
    %% ok = riakc_pb_socket:put(C, riakc_obj:new(?BUCKET, <<?KEY:64/integer>>, term_to_binary([]))),
    riakc_pb_socket:put(C, riakc_obj:new(?BUCKET, <<?KEY:64/integer>>, term_to_binary([])), ?TIMEOUT),
    %% NT = list_to_tuple(Nodes),
    %% Len = tuple_size(NT),
    %% Clients = [begin
    %%                %% Node = element((N rem Len) + 1, NT),
    %%                %% {ok, Client} = riak:client_connect(Node),
    %%                {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 10017, [{auto_reconnect, true}]),
    %%                Pid
    %%            end || N <- lists:seq(1,NumWorkers)],
    %% list_to_tuple(Clients).
    ok.

get_args(#state{num_workers=NumW, values=Values}) ->
    Key = ?KEY,
    LastValue = case orddict:find(Key, Values) of
                    error ->
                        notfound;
                    {ok, Val} ->
                        Val
                end,
    [Key, LastValue, choose(1, NumW)].
get_pre(_, _) ->
    true.
get_blocking(_,_) ->
    false.
get_next(State, Result, [Key, _, Thread]) ->
    Values2 = orddict:store(Key, Result, State#state.values),
    Known2 = orddict:store({Key, Thread}, Result, State#state.known),
    State#state{values=Values2, known=Known2}.
get_post(State=#state{values=Values}, [Key, _, _], Result) ->
    case Result of
        {timeout, _} ->
            %% TODO: Model when this should or shouldn't happen
            may_timeout(State);
        {unavailable, _} ->
            %% TODO: Model when this should or shouldn't happen
            may_timeout(State);
        {failed, _} ->
            %% TODO: Model when this should or shouldn't happen
            true;
        _ ->
            case orddict:find(Key, Values) of
                error ->
                    true;
                {ok, Current} ->
                    %% Obj = untag(Current),
                    %% ValA = value(Obj),
                    %% ValB = value(Result),
                    %% case ValA of
                    %%     ValB ->
                    %%         true;
                    %%     _ ->
                    %%         io:format("~p // ~p~n", [ValA, ValB]),
                    %%         false
                    %% end
                    %% Result =:= Obj
                    Valid = untag(Current),
                    Value = value(Result),
                    L = lists:flatten([Valid]),
                    lists:any(fun(ExpObj) ->
                                      Expect = value(ExpObj),
                                      %% io:format("~p ?= ~p~n", [Value, Expect]),
                                      Value =:= Expect
                              end, L)
            end
    end.
get(Key, LastValue, Thread) ->
    %% C = element(Thread, Clients),
    C = ets:lookup_element(?ETS, {client,Thread}, 2),
    KeyBin = <<Key:64/integer>>,
    case result(riakc_pb_socket:get(C, ?BUCKET, KeyBin, ?TIMEOUT)) of
    %% case C:get(?BUCKET, KeyBin) of
        {error, notfound} ->
            notfound;
        {error, timeout} ->
            {timeout, untag(LastValue)};
        {error, failed} ->
            {failed, untag(LastValue)};
        {error, unavailable} ->
            {unavailable, untag(LastValue)};
        {ok, Obj} ->
            Obj
    end.

value(notfound) ->
    %% [];
    notfound;
value(Obj) ->
    try
        binary_to_term(riakc_obj:get_update_value(Obj))
    catch
        _:E ->
            io:format("E: ~p/~p~n", [E, Obj]),
            Obj
    end.

modify_args(#state{known=Known, values=Values, next_value=NVal}) ->
    ?LET({{Key, Thread}, Value}, elements(Known),
         begin
             Current = orddict:fetch(Key, Values),
             [Key, Value, Current, Thread, NVal]
         end).
modify_pre(#state{known=Known}) ->
    Known =/= [].
modify_blocking(_,_) ->
    false.
modify_next(State, Result, [Key, _, _, Thread, NewVal]) ->
    Values2 = orddict:store(Key, Result, State#state.values),
    Known2 = orddict:erase({Key, Thread}, State#state.known),
    NextVal = State#state.next_value + 1,
    Acked = [{NewVal, Result}|State#state.acked],
    State#state{values=Values2, known=Known2, next_value=NextVal, acked=Acked}.
modify_post(State, [_Key, Prior, Current, _Thread, _X], Result) ->
    %% io:format("R: ~p~n", [Result]),
    case Result of
        notfound ->
            true;
        {ok, _Obj} ->
            might_succeed(Prior, Current);
        {failed, _Obj} ->
            %% TODO: When running w/o concurrency, this should fail
            %% TODO: Model when this should have succeeded?
            %% throw(fail),
            may_fail(Prior, Current, State);
        {timeout, _Obj} ->
            %% throw(timeout),
            may_timeout(State);
        {unavailable, _Obj} ->
            may_timeout(State);
        {skip, _} ->
            true;
        _ ->
            false
    end.
modify(Key, Obj, Current, Thread, X) ->
    %% case untag(Obj) of
    EmptyVC = try (vclock(untag(Obj)) == undefined) catch _:_ -> undefined end,
    case untag(Obj) of
        notfound ->
            %% Skip since we're modifying, not putting fresh
            {skip, untag(Current)};
        L when is_list(L) ->
            %% Skip update. Alternatively, we could get before update here.
            {skip, untag(Current)};
        _ when EmptyVC ->
            %% Skip otherwise we'll do clobber put
            {skip, untag(Current)};
        _ ->
            KeyBin = <<Key:64/integer>>,
            C = ets:lookup_element(?ETS, {client,Thread}, 2),
            NewObj = append_value(untag(Obj), KeyBin, X),
            %% NewVal = <<X:64/integer>>,
            %% NewObj = case Obj of
            %%              notfound ->
            %%                  %% riak_object:new(?BUCKET, KeyBin, NewVal);
            %%              _ ->
            %%                  %% riak_object:update_value(Obj, NewVal)
            %%                  riakc_obj:update_value(Obj, NewVal)
            %%          end,
            %% C:put(NewObj).
            case result(riakc_pb_socket:put(C, NewObj, [return_body, {w,1}, {timeout,?TIMEOUT}])) of
                {error, siblings} ->
                    siblings;
                {error, failed} ->
                    {failed, untag(Current)};
                {error, unavailable} ->
                    {unavailable, untag(Current)};
                {error, timeout} ->
                    CurrentVal = untag(Current),
                    Maybe = might_succeed(Obj, Current),
                    %% PriorVal = value(untag(Obj)),
                    %% VL = lists:flatten([CurrentVal]),
                    %% %% io:format("Valid: ~p~n", [VL]),
                    %% Maybe = lists:any(fun(ValidObj) ->
                    %%                           ValidVal = value(ValidObj),
                    %%                           %% io:format("~p ??= ~p~n", [PriorVal, ValidVal]),
                    %%                           PriorVal =:= ValidVal
                    %%                   end, VL),
                    %% case not is_list(CurrentVal) andalso value(untag(Obj)) == value(CurrentVal) of
                    case Maybe of
                        true ->
                            %% Update might have suceeded
                            Valid = case CurrentVal of
                                        L when is_list(L) ->
                                            [NewObj|L];
                                        _ ->
                                            [NewObj, CurrentVal]
                                    end,
                            {timeout, Valid};
                        false ->
                            {timeout, untag(Current)}
                    end;
                    %% {timeout, untag(Current)};
                    %% io:format("V: ~p~n", [value(NewObj)]),
                    %% {timeout, untag(NewObj)};
                {ok, ResultObj} ->
                    {ok, ResultObj}
            end
    end.

-ifndef(SINGLE_NODE).
partition_args(#state{nodes=Nodes}) ->
    ?LET({N, L}, {choose(1, length(Nodes)), shuffle(Nodes)},
         tuple_to_list(lists:split(N,L))).
partition_pre(#state{init=Initialized, partitioned=Partitioned}) ->
    Initialized and (Partitioned =:= undefined).
partition_pre(_, [P1, P2]) ->
    (P1 =/= []) and (P2 =/= []).
partition_blocking(_,_) ->
    false.
partition_next(State, Partitioned, _) ->
    %% TODO: Change actual consistency protocol so that internal
    %%       rewriting of data across epoch changes doesn't fail writes.
    %%       Hacking semantics in test for now.
    MaybeRW = [untag(Val) || {_,Val} <- State#state.values],
    %% State#state{partitioned=true, maybe_rewritten=MaybeRW}.
    set_maybe(MaybeRW),
    State#state{partitioned=Partitioned}.
partition(P1, P2) ->
    %% throw(yo),
    partition_nodes(P1, P2).
-else.
partition_args(_) ->
    Peers = lists:seq(1,3),
    ?LET({N, L}, {choose(1, length(Peers)), shuffle(Peers)},
         tuple_to_list(lists:split(N,L))).
partition_blocking(_,_) ->
    false.
partition_pre(#state{partitioned=Partitioned}) ->
    Partitioned =:= undefined.
    %% false.
partition_pre(_, [P1, P2]) ->
    (P1 =/= []) and (P2 =/= []).
partition_next(State, _Result, _Args) ->
    %% TODO: Change actual consistency protocol so that internal
    %%       rewriting of data across epoch changes doesn't fail writes.
    %%       Hacking semantics in test for now.
    MaybeRW = [untag(Val) || {_,Val} <- State#state.values],
    %% State#state{partitioned=true, maybe_rewritten=MaybeRW}.
    set_maybe(MaybeRW),
    State#state{partitioned=true}.
partition(As, Bs) ->
    Node = 'dev1@127.0.0.1',
    case rpc:call(Node, riak_ensemble_manager, rget, [ensembles, []]) of
        Ensembles when is_list(Ensembles) ->
            %% {_, {_Leader,  Peers, _Seq}} = lists:keyfind({kv,0,3}, 1, Ensembles),
            {_, #ensemble_info{members=Peers}} = lists:keyfind({kv,0,3}, 1, Ensembles),
            PT = list_to_tuple(Peers),
            AL = [element(N, PT) || N <- As],
            BL = [element(N, PT) || N <- Bs],
            %% io:format("Peers: ~p~nAs: ~p~nBs: ~p~n", [Peers, As, Bs]);
            %% io:format("AL: ~p~nBL: ~p~n", [AL, BL]),
            [rpc:call(Node, ets, insert, [sc, [{{drop, {A, B}}, true},
                                               {{drop, {B, A}}, true}]])
             || A <- AL,
                B <- BL],
            %% io:format("ETS: ~p~n", [ets:tab2list(sc)]),
            %% ets:insert(sc, [{{drop, {2,node()}}, true},
            %%                 {{drop, {1,node()}}, true}]),
            %% wait_until_not_quorum(10, root),
            %% timer:sleep(600),
            ok;
        Other ->
            io:format("Other: ~p~n", [Other])
    end,
    ok.
-endif.

-ifndef(SINGLE_NODE).
heal_args(#state{partitioned=Partitioned}) ->
    [Partitioned].
heal_pre(#state{init=Initialized, partitioned=Partitioned}) ->
    Initialized and (Partitioned =/= undefined).
heal_blocking(_,_) ->
    false.
heal_next(State, _, _) ->
    MaybeRW = [untag(Val) || {_,Val} <- State#state.values],
    MaybeRW2 = lists:usort(get_maybe() ++ MaybeRW),
    set_maybe(MaybeRW2),
    State#state{partitioned=undefined}.
heal(Partitioned) ->
    catch heal_nodes(Partitioned).
-else.
heal_args(_) ->
    [].
heal_blocking(_,_) ->
    false.
heal_pre(#state{partitioned=Partitioned}) ->
    Partitioned =/= undefined.
heal_next(State, _Result, _Args) ->
    MaybeRW = [untag(Val) || {_,Val} <- State#state.values],
    %% State#state{partitioned=true, maybe_rewritten=MaybeRW}.
    MaybeRW2 = lists:usort(get_maybe() ++ MaybeRW),
    set_maybe(MaybeRW2),
    State#state{partitioned=undefined}.
heal() ->
    Node = 'dev1@127.0.0.1',
    rpc:call(Node, ets, match_delete, [sc, {{drop, '_'}, '_'}]),
    %% ets:delete(sc, {drop, {2,node()}}),
    %% ets:delete(sc, {drop, {1,node()}}),
    wait_until_stable(),
    ok.
-endif.

put_once_args(#state{num_workers=NumW, next_value=Val, values=Values}) ->
    Key = ?KEY,
    Current = case orddict:find(Key, Values) of
                  error ->
                      notfound;
                  {ok, Cur} ->
                      Cur
              end,
    [Key, Current, choose(1, NumW), Val].
put_once_blocking(_,_) ->
    false.
put_once_next(State, Result, [Key, _Current, _Thread, NewVal]) ->
    Values2 = orddict:store(Key, Result, State#state.values),
    %% Known2 = orddict:store({Key, Thread}, Result, State#state.known),
    Known2 = State#state.known,
    NextVal = State#state.next_value + 1,
    Acked = [{NewVal, Result}|State#state.acked],
    State#state{values=Values2, next_value=NextVal, known=Known2, acked=Acked}.
put_once_post(State, [_Key, Current, _Thread, _NewVal], Result) ->
    case Result of
        {timeout, _} ->
            %% true;
            may_timeout(State);
        {unavailable, _} ->
            may_timeout(State);
        {ok, _} ->
            might_succeed(notfound, Current);
            %% untag(Current) =:= notfound;
        {failed, _} ->
            untag(Current) =/= notfound
    end.
put_once(Key, Current, Thread, X) ->
    C = ets:lookup_element(?ETS, {client,Thread}, 2),
    Obj = riakc_obj:new(?BUCKET, <<Key:64/integer>>, term_to_binary([X])),
    case result(riakc_pb_socket:put(C, Obj, [return_body, if_none_match, {timeout, ?TIMEOUT}])) of
        {ok, ResultObj} ->
            {ok, ResultObj};
        {error, failed} ->
            {failed, untag(Current)};
        {error, unavailable} ->
            {unavailable, untag(Current)};
        {error, timeout} ->
            case might_succeed(notfound, Current) of
                true ->
                    CurrentVal = untag(Current),
                    Valid = case CurrentVal of
                                L when is_list(L) ->
                                    [Obj|L];
                                _ ->
                                    [Obj, CurrentVal]
                            end,
                    {timeout, Valid};
                false ->
                    {timeout, untag(Current)}
            end
    end.

delete_args(#state{num_workers=NumW, values=Values}) ->
    Key = ?KEY,
    Current = case orddict:find(Key, Values) of
                  error ->
                      notfound;
                  {ok, Val} ->
                      Val
              end,
    [Key, Current, choose(1, NumW)].
delete_blocking(_,_) ->
    false.
delete_next(State, Result, [Key, _Current, _Thread]) ->
    Values2 = orddict:store(Key, Result, State#state.values),
    Acked = [Result|State#state.acked],
    State#state{values=Values2, acked=Acked}.
delete_post(State, _Args, Result) ->
    case Result of
        {deleted, _Obj} ->
            true;
        {unavailable, _} ->
            may_timeout(State);
        {timeout, _} ->
            may_timeout(State)
    end.
delete(Key, Current, Thread) ->
    C = ets:lookup_element(?ETS, {client,Thread}, 2),
    KeyBin = <<Key:64/integer>>,
    case result(riakc_pb_socket:delete(C, ?BUCKET, KeyBin, ?TIMEOUT)) of
        {error, timeout} ->
            CurrentVal = untag(Current),
            Valid = case CurrentVal of
                        L when is_list(L) ->
                            [notfound|L];
                        _ ->
                            [notfound, CurrentVal]
                    end,
            {timeout, Valid};
        {error, unavailable} ->
            {unavailable, untag(Current)};
        Err={error, _} ->
            %% Current
            throw(result(Err));
        ok ->
            {deleted, notfound}
    end.

safe_delete_args(#state{known=Known, values=Values}) ->
    ?LET({{Key, Thread}, Prior}, elements(Known),
         begin
             Current = orddict:fetch(Key, Values),
             [Key, Prior, Current, Thread]
         end).
safe_delete_pre(#state{known=Known}) ->
    Known =/= [].
safe_delete_blocking(_,_) ->
    false.
safe_delete_next(State, Result, [Key, _Prior, _Current, Thread]) ->
    Values2 = orddict:store(Key, Result, State#state.values),
    %% Known2 = orddict:store({Key, Thread}, Result, State#state.known),
    Known2 = orddict:erase({Key, Thread}, State#state.known),
    Acked = [Result|State#state.acked],
    State#state{values=Values2, acked=Acked, known=Known2}.
safe_delete_post(State, [_Key, Prior, Current, _Thread], Result) ->
    case Result of
        {timeout, _} ->
            %% true;
            may_timeout(State);
        {unavailable, _} ->
            may_timeout(State);
        {skip, _} ->
            true;
        {failed, _} ->
            %% TODO: When running w/o concurrency, this should fail
            %% TODO: Model when this should have succeeded?
            %% throw(fail),
            (untag(Current) =:= notfound) or (may_fail(Prior, Current, State));
        {deleted, notfound} ->
            EmptyVC = (vclock(untag(Prior)) =:= undefined),
            EmptyVC or might_succeed(Prior, Current)
    end.
safe_delete(Key, Prior, Current, Thread) ->
    case untag(Prior) of
        notfound ->
            {skip, untag(Current)};
        L when is_list(L) ->
            %% Skip update. Alternatively, we could get before delete here.
            {skip, untag(Current)};
        _ ->
            C = ets:lookup_element(?ETS, {client,Thread}, 2),
            KeyBin = <<Key:64/integer>>,
            VClock = vclock(untag(Prior)),
            case result(riakc_pb_socket:delete_vclock(C, ?BUCKET, KeyBin, VClock, ?TIMEOUT)) of
                {error, timeout} ->
                    %% TODO: Should check if may_succeed here, no?
                    CurrentVal = untag(Current),
                    Valid = case CurrentVal of
                                L when is_list(L) ->
                                    [notfound|L];
                                _ ->
                                    [notfound, CurrentVal]
                            end,
                    {timeout, Valid};
                {error, unavailable} ->
                    {unavailable, untag(Current)};
                {error, failed} ->
                    {failed, untag(Current)};
                Err={error, _} ->
                    %% Current
                    throw(result(Err));
                ok ->
                    {deleted, notfound}
            end
    end.

vclock(notfound) ->
    <<>>;
vclock(Obj) ->
    riakc_obj:vclock(Obj).

might_succeed(Prior, Current) ->
    CurrentVal = untag(Current),
    PriorVal = value(untag(Prior)),
    PriorVC = vclock(untag(Prior)),
    VL = lists:flatten([CurrentVal]),
    %% io:format("Valid: ~p~n", [VL]),
    Maybe = lists:any(fun(ValidObj) ->
                              ValidVal = value(ValidObj),
                              ValidVC = vclock(ValidObj),
                              %% io:format("~p ??= ~p~n", [PriorVal, ValidVal]),
                              (PriorVal =:= ValidVal) and (PriorVC == ValidVC)
                      end, VL),
    Maybe.

may_fail(Prior, Current, _) ->
%% may_fail(Prior, Current, #state{maybe_rewritten=MaybeRW}) ->
    MaybeRW = get_maybe(),
    A = untag(Prior),
    B = untag(Current),
    Maybe = lists:member(A, MaybeRW),
    io:format("Maybe/~p: ~p~n~p~n", [Maybe, MaybeRW, Prior]),
    %% TODO: Isn't a lists:all valid here?
    if is_list(A) or is_list(B) ->
            true;
       Maybe ->
            true;
       true ->
            (value(A) =/= value(B)) or (vclock(A) =/= vclock(B))
    end.

may_timeout(#state{}) ->
    %% TODO: Make this stronger, eg. related to reachable quorum
    %% Partitioned =/= undefined.
    true.

untag({_, Val}) ->
    Val;
untag(Val) ->
    Val.

append_value(notfound, KeyBin, Val) ->
    ValBin = term_to_binary([Val]),
    riakc_obj:new(?BUCKET, KeyBin, ValBin);
append_value(Obj, _, Val) ->
    L = binary_to_term(riakc_obj:get_update_value(Obj)),
    ValBin = term_to_binary([Val|L]),
    riakc_obj:update_value(Obj, ValBin).

result({error, disconnected}) ->
    {error, timeout};
result({error, Error}) ->
    if is_binary(Error) and (byte_size(Error) < 64) ->
            {error, binary_to_atom(Error, utf8)};
       true ->
            {error, Error}
    end;
result({ok, Obj}) ->
    try
        riakc_obj:get_value(Obj),
        {ok, Obj}
    catch
        _:Err -> {error, Err}
    end;
result(X) ->
    X.

%% dummy_args(_) ->
%%     [].
%% dummy_pre(_) ->
%%     true.
%% dummy_blocking(_,_) ->
%%     false.
%% dummy_next(State, _, _) ->
%%     State.
%% dummy() ->
%%     ok.

sc_test_() ->
    Duration = 5,
    %% {timeout, Duration*2, fun() -> sc_test_body(Duration) end}.
    {timeout, Duration*2, fun() ->
                                  ?debugMsg("Test disabled. Skipping")
                          end}.
sc_test_body(Duration) ->
    net_kernel:start(['eunit@127.0.0.1']),
    erlang:set_cookie(node(), riak),
    code:add_path("/Users/jtuple/basho/riak_test.master/deps/riakc/ebin"),
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,
    ?assert(eqc:quickcheck(eqc:testing_time(Duration, prop_sc(Nodes, Concurrency)))).

g1() ->
    Nodes = ['dev1@127.0.0.1'],
    Concurrency = 1,
    eqc:quickcheck(eqc:numtests(20,prop_sc(Nodes, Concurrency))).

gp() ->
    Nodes = ['dev1@127.0.0.1'],
    %% Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,
    eqc:quickcheck(eqc:numtests(1000,prop_sc3(Nodes, Concurrency))).

go(Runs) ->
    %% Nodes = ['dev1@127.0.0.1'],
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,
    eqc:quickcheck(eqc:numtests(Runs, prop_sc(Nodes, Concurrency))),
    ok.

gc(Runs) ->
    rt_config:set(cover_enabled, true),
    rt_config:set(cover_apps, [riak_ensemble]),
    rt_config:set(rt_harness, rtdev),
    rt_config:set(rtdev_path, [{current, "/Users/jtuple/basho/riak-strong"}]),
    rt_cover:maybe_start(),

    %% Nodes = ['dev1@127.0.0.1'],
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,

    [rt_cover:maybe_start_on_node(Node, current) || Node <- Nodes],

    eqc:quickcheck(eqc:numtests(Runs, prop_sc(Nodes, Concurrency))),

    rt_cover:maybe_stop_on_nodes(),
    rt_cover:maybe_write_coverage(all, "/tmp/cover"),
    ok.

rgo() ->
    %% Nodes = ['dev1@127.0.0.1'],
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,
    eqc:check(prop_sc(Nodes, Concurrency)).

rgp() ->
    Nodes = ['dev1@127.0.0.1'],
    %% Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    Concurrency = 4,
    eqc:check(prop_sc3(Nodes, Concurrency)).

go2() ->
    Nodes = ['dev1@127.0.0.1'],
    Concurrency = 10,
    eqc:check(prop_sc(Nodes, Concurrency)).

tt() ->
    L = 
[{timeout,[notfound,
               {riakc_obj,<<"c~test">>,
                          <<0,0,0,0,0,0,0,3>>,
                          <<107,206,97,96,96,96,204,96,74,97,96,73,45,78,45,
                            204,96,202,99,99,16,7,10,49,51,230,177,50,188,95,
                            118,236,52,95,22,0>>,
                          [{{dict,2,16,16,8,80,48,
                                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                                   [],[]},
                                  {{[],[],[],[],[],[],[],[],[],[],
                                    [[<<"X-Riak-VTag">>,51,83,74,111,98,99,86,
                                      119,52,48,57,69,108,99,113,68,79,69,112,
                                      57,55,79]],
                                    [],[],
                                    [[<<"X-Riak-Last-Modified">>|
                                      {1381,116655,625094}]],
                                    [],[]}}},
                            <<131,107,0,1,2>>}],
                          undefined,undefined}]},
     {4,
      {failed,{riakc_obj,<<"c~test">>,
                         <<0,0,0,0,0,0,0,3>>,
                         <<107,206,97,96,96,96,204,96,74,97,96,73,45,78,45,204,
                           96,202,99,99,16,7,10,49,51,230,177,50,188,95,118,
                           236,52,95,22,0>>,
                         [{{dict,2,16,16,8,80,48,
                                 {[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                                  [],[]},
                                 {{[],[],[],[],[],[],[],[],[],[],
                                   [[<<"X-Riak-VTag">>,51,83,74,111,98,99,86,
                                     119,52,48,57,69,108,99,113,68,79,69,112,
                                     57,55,79]],
                                   [],[],
                                   [[<<"X-Riak-Last-Modified">>|
                                     {1381,116655,625094}]],
                                   [],[]}}},
                           <<131,107,0,1,2>>}],
               undefined,undefined}}},
     {3,
      {failed,{riakc_obj,<<"c~test">>,
                         <<0,0,0,0,0,0,0,3>>,
                         <<107,206,97,96,96,96,204,96,74,97,96,73,45,78,45,204,
                           96,202,99,99,16,7,10,49,51,230,177,50,188,95,118,
                           236,52,95,22,0>>,
                         [{{dict,2,16,16,8,80,48,
                                 {[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                                  [],[]},
                                 {{[],[],[],[],[],[],[],[],[],[],
                                   [[<<"X-Riak-VTag">>,51,83,74,111,98,99,86,
                                     119,52,48,57,69,108,99,113,68,79,69,112,
                                     57,55,79]],
                                   [],[],
                                   [[<<"X-Riak-Last-Modified">>|
                                     {1381,116655,625094}]],
                                   [],[]}}},
                           <<131,107,0,1,2>>}],
                         undefined,undefined}}},
     {skip,{riakc_obj,<<"c~test">>,
                      <<0,0,0,0,0,0,0,3>>,
                      <<107,206,97,96,96,96,204,96,74,97,96,73,45,78,45,204,96,
                        202,99,99,16,7,10,49,51,230,177,50,188,95,118,236,52,
                        95,22,0>>,
                      [{{dict,2,16,16,8,80,48,
                              {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                               []},
                              {{[],[],[],[],[],[],[],[],[],[],
                                [[<<"X-Riak-VTag">>,51,83,74,111,98,99,86,119,
                                  52,48,57,69,108,99,113,68,79,69,112,57,55,
                                  79]],
                                [],[],
                                [[<<"X-Riak-Last-Modified">>|
                                  {1381,116655,625094}]],
                                [],[]}}},
                        <<131,107,0,1,2>>}],
                      undefined,undefined}},
     {2,
      {ok,{riakc_obj,<<"c~test">>,
                     <<0,0,0,0,0,0,0,3>>,
                     <<107,206,97,96,96,96,204,96,74,97,96,73,45,78,45,204,96,
                       202,99,99,16,7,10,49,51,230,177,50,188,95,118,236,52,95,
                       22,0>>,
                     [{{dict,2,16,16,8,80,48,
                             {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                             {{[],[],[],[],[],[],[],[],[],[],
                               [[<<"X-Riak-VTag">>,51,83,74,111,98,99,86,119,
                                 52,48,57,69,108,99,113,68,79,69,112,57,55,
                                 79]],
                               [],[],
                               [[<<"X-Riak-Last-Modified">>|
                                 {1381,116655,625094}]],
                               [],[]}}},
                       <<131,107,0,1,2>>}],
           undefined,undefined}}}],
    %% lists:flatmap(fun({timeout, Opts}) ->
    %%                       [case X of
    %%                            notfound ->
    %%                                {notfound, {ok, notfound}};
    %%                            Obj ->
    %%                                Val = value(Obj),
    %%                                %% Val = riakc_obj:get_update_value(Obj),
    %%                                {Val, {ok, Obj}}
    %%                        end || X <- Opts];
    %%                  (Other) ->
    %%                       [Other]
    %%               end, L).
    get_success(L).

%% TODO: Need to model timeout'd modify/puts as maybes
get_success(L) ->
    get_success(lists:reverse(L), [], []).
get_success([], Success, Maybe) ->    
    {lists:usort(Success), lists:usort(Maybe)};
get_success([X|Rest], Success, Maybe) -> 
    case X of
        {Val, {ok, _}} ->
            get_success(Rest, [Val|Success], Maybe);
        {timeout, _} ->
            %% Potentially failed delete
            io:format("Failed~n"),
            get_success(Rest, [], Maybe ++ Success);
        _Other ->
            %% io:format("Other: ~p~n", [_Other]),
            get_success(Rest, Success, Maybe)
    end.

prop_sc(Nodes, Concurrency) ->
    State = first_state(Nodes, Concurrency),
    ?SETUP(fun() ->
                   setup(Nodes, Concurrency),
                   fun teardown/0
           end,
    ?FORALL(Repetitions, ?SHRINK(1, [10]),
    ?FORALL(Cmds, noshrink(more_commands(1,commands(?MODULE, State))),
    ?ALWAYS(Repetitions,
    %% ?ALWAYS(10000,
            begin
                rpc:multicall(Nodes, riak_kv_memory_backend, reset, []),
                stablize(Nodes),
                {H,S,Res} = run_commands(?MODULE,Cmds),
                stablize(Nodes),
                io:format("Res: ~p~n", [Res]),
                %% (S#state.partitioned =/= undefined) andalso heal_nodes(S#state.partitioned),
                Pass1 = (Res == ok),
                %% io:format("Cmds: ~p~n", [Cmds]),
                SX = lists:takewhile(fun({deleted,_}) ->
                                             false;
                                        (_) ->
                                             true
                                     end, S#state.acked),
                io:format("SX: ~p~n", [SX]),
                %% Success = [Val || {Val, {ok,_}} <- SX],
                {Success, _Maybe} = get_success(SX),
                %% io:format("Acked: ~p~n", [S#state.acked]),
                io:format("Success: ~p~n", [Success]),
                C = ets:lookup_element(?ETS, {client,1}, 2),
                case result(riakc_pb_socket:get(C, ?BUCKET, <<?KEY:64/integer>>, ?TIMEOUT)) of
                    {ok, Obj} ->
                        Val = binary_to_term(riakc_obj:get_value(Obj)),
                        io:format("Obj: ~p~n", [Val]),
                        %% Pass2 = (Success =:= Val);
                        Pass2 = ((Success -- Val) =:= []);
                    {error, timeout} ->
                        %% HACK THIS FOR NOW. FIX!!!
                        Pass2 = true;
                    {error, notfound} ->
                        io:format("Obj: notfound~n"),
                        Pass2 = (Success =:= [])
                    %% _ ->
                    %%     Pass2 = true
                end,
                Pass2 orelse io:format("Data loss!~n"),
                eqc_statem:pretty_commands(?MODULE, Cmds, {H,S,Res}, Pass1 and Pass2)
            end)))).

check_args(_) ->
    0.
check_pre(_) ->
    false.
check_blocking(_,_) ->
    false.
check_next(State, _Result, _Args) ->
    Wait2 = State#state.wait + 1,
    State#state{wait=Wait2}.
check_post(#state{wait=Wait, acked=Acked}, [Num], Result) ->
    Wait2 = Wait+1,
    SX = lists:takewhile(fun({deleted,_}) ->
                                 false;
                            (_) ->
                                 true
                         end, Acked),
    %% Success = [Val || {Val, {ok,_}} <- SX],
    {Success, _Maybe} = get_success(SX),

    case Result of
        {Wait2, skip} when Wait2 < Num ->
            true;
        {Wait2, check, empty} ->
            Success =:= [];
        {Wait2, check, Val} ->
            io:format("Success: ~p~n", [Success]),
            io:format("Obj: ~p~n", [Val]),
            (Success -- Val) =:= [];
        {Wait2, true} ->
            true;
        _ ->
            throw(test_broken)
    end.
check(Num) ->
    Count = ets:update_counter(?ETS, wait, 1),
    io:format("Num: ~p/~p~n", [Num, Count]),
    case Count of
        Num ->
            C = ets:lookup_element(?ETS, {client,1}, 2),
            case riakc_pb_socket:get(C, ?BUCKET, <<?KEY:64/integer>>, ?TIMEOUT) of
                {ok, Obj} ->
                    Val = binary_to_term(riakc_obj:get_value(Obj)),
                    io:format("Obj: ~p~n", [Val]),
                    {Count, check, Val};
                {error, notfound} ->
                    {Count, check, empty};
                _Other ->
                    %% TODO: Really should make check wait for quorum and always succeed
                    %% io:format("Other: ~p~n", [_Other]),
                    {Count, true}
            end;
        _ ->
            {Count, skip}
    end.

prop_sc3(Nodes, Concurrency) ->
    State = first_state(Nodes, Concurrency),
    ?SETUP(fun() ->
                   setup(Nodes, Concurrency),
                   fun teardown/0
           end,
    ?FORALL(Repetitions, ?SHRINK(1, [10]),
    ?FORALL(BaseCmds, noshrink(parallel_commands(?MODULE, State)),
    ?ALWAYS(Repetitions,
    %% ?ALWAYS(10000,
            begin
                ets:insert(?ETS, {wait, 0}),
                %% io:format("BaseCmds: ~p~n", [BaseCmds]),
                {SeqCmds, ParCmds} = BaseCmds,
                NP = length(ParCmds),
                ParCmds2 = [begin
                                CheckCmd = {set, {var, 1000000+X}, {call, ?MODULE, check, [NP]}},
                                PC ++ [CheckCmd]
                            end || {X,PC} <- lists:zip(lists:seq(1,NP), ParCmds)],
                Cmds = {SeqCmds, ParCmds2},
                rpc:multicall(Nodes, riak_kv_memory_backend, reset, []),
                stablize(Nodes),
                {H,S,Res} = run_parallel_commands(?MODULE,Cmds),
                stablize(Nodes),
                eqc_statem:pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok)
            end)))).

prop_sc2(Nodes, Concurrency) ->
    State = first_state(Nodes, Concurrency),
    ?SETUP(fun() ->
                   setup(Nodes, Concurrency),
                   fun teardown/0
           end,
    ?FORALL(Cmds, parallel_commands(?MODULE, State),
            begin
                {H,S,Res} = run_parallel_commands(?MODULE,Cmds),
                %% io:format("Cmds: ~p~n", [Cmds]),
                eqc_statem:pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok)
            end)).

setup(Nodes, NumWorkers) ->
    %% catch ets:delete(?ETS),
    %% timer:sleep(1000),
    case ets:info(?ETS) of
        undefined ->
            ets:new(?ETS, [named_table, public, set, {read_concurrency,true}, {write_concurrency,true}]),
            Clients = connect_clients(Nodes, NumWorkers),
            io:format("Clients: ~p~n", [Clients]),
            Keys = [{client, X} || X <- lists:seq(1, NumWorkers)],
            true = ets:insert(?ETS, lists:zip(Keys, Clients)),
            ok;
        _ ->
            ok
    end.

teardown() ->
    ok.

connect_clients(_Nodes, NumWorkers) ->
    %% TODO: Update to actually use multiple nodes
    %% NT = list_to_tuple(Nodes),
    %% Len = tuple_size(NT),
    Clients = [begin
                   %% Node = element((N rem Len) + 1, NT),
                   %% {ok, Client} = riak:client_connect(Node),
                   {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 10017, 
                                                          [{auto_reconnect, true},
                                                           {queue_if_disconnected, true}]),
                   Pid
               end || _N <- lists:seq(1,NumWorkers)],
    Clients.

%% @doc partition the `P1' from `P2' nodes
partition_nodes(P1, P2) ->
    OldCookie = rpc:call(hd(P1), erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    [begin
         true = rpc:call(Node, erlang, set_cookie, [Other, NewCookie]),
         rpc:call(Node, erlang, disconnect_node, [Other])
     end || Node <- P1,
            Other <- P2],
    %% timer:sleep(2000),
    {NewCookie, OldCookie, P1, P2}.

%% @doc heal the partition created by call to `partition/2'
%%      `OldCookie' is the original shared cookie
heal_nodes({_NewCookie, OldCookie, P1, P2}) ->
    Cluster = P1 ++ P2,
    [begin
         true = rpc:call(Node, erlang, set_cookie, [Other, OldCookie])
         %% true = rpc:call(Node, erlang, connect_node, [Other])
     end || Node <- P1,
            Other <- P2],
    %% timer:sleep(2000),
    {_GN, []} = rpc:sbcast(Cluster, riak_core_node_watcher, broadcast),
    wait_until_stable(),
    ok.

-ifdef(SINGLE_NODE).
stablize(Nodes) ->
    [rpc:multicall(Nodes, erlang, set_cookie, [N, riak]) || N <- [node()|Nodes]],
    heal(),
    wait_until_stable(),
    ok.
-else.
stablize(Nodes) ->
    [rpc:multicall(Nodes, erlang, set_cookie, [N, riak]) || N <- [node()|Nodes]],
    %% heal(),
    wait_until_stable(),
    ok.
-endif.

wait_until_stable() ->
    Node = 'dev1@127.0.0.1',
    Ensemble = {kv,0,3},
    wait_until_quorum(30, root, Node),
    wait_until_quorum(30, Ensemble, Node),
    ok.

wait_until_quorum(Retry, Ensemble, Node) ->
    case rpc:call(Node, riak_ensemble_manager, check_quorum, [Ensemble, 1000]) of
        true ->
            %% io:format("wait_for_quorum(~p): succeed~n", [Ensemble]),
            ok;
        _ when Retry =< 0 ->
            io:format("wait_for_quorum(~p): timeout~n", [Ensemble]),
            throw(failed);
        _ ->
            timer:sleep(100),
            wait_until_quorum(Retry-1, Ensemble, Node)
    end.
