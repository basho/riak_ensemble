-module(ensemble_only_test).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_ensemble_types.hrl").

-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).

-behaviour(eqc_statem).

%% eqc_statem exports
-export([command/1, initial_state/0, next_state/3, postcondition/3,
         precondition/2]).

-compile(export_all).

-define(NUM_NODES, 5).
-define(REQ_TIMEOUT, 1000).
-define(QC_TIMEOUT, 300).
-define(EUNIT_TIMEOUT, 1800).

-record(state, {ensembles=sets:new() :: set(),
                data=dict:new() :: dict(),
                up_nodes=other_node_names() :: list(atom()),
                down_nodes=[] :: list(atom())}).

ensemble_test_() ->
    {setup, spawn, fun setup/0, fun cleanup/1, [
        {timeout, ?EUNIT_TIMEOUT,
            ?_assertEqual(true, quickcheck(?QC_OUT(eqc:testing_time(?QC_TIMEOUT,
                            prop_ensemble()))))}]}.

setup() ->
    net_kernel:start(['ensemble_tester@127.0.0.1']),
    erlang:set_cookie(node(), riak_ensemble_test),
    lager:start(),
    cleanup_riak_ensemble_on_all_nodes(),
    Nodes = node_names(),
    [erl_port:start_link(Node) || Node <- Nodes],
    wait_for_nodeups(Nodes),
    make_data_dirs(),
    _Output = start_riak_ensemble_on_all_nodes(),
    wait_for_manager(Nodes),
    initial_join(Nodes),
    ok = create_root_ensemble(Nodes).

cleanup(_) ->
    ok.

wait_for_manager([]) ->
    ok;
wait_for_manager([H | T]=Nodes) ->
    case rpc:call(H, erlang, whereis, [riak_ensemble_manager], 5000) of
        undefined ->
            timer:sleep(100),
            wait_for_manager(Nodes);
        _ ->
            wait_for_manager(T)
    end.

wait_for_nodeups([]) ->
    ok;
wait_for_nodeups([H | T]=Nodes) ->
    case net_adm:ping(H) of
        pong ->
            wait_for_nodeups(T);
        pang ->
            timer:sleep(10),
            wait_for_nodeups(Nodes)
    end.

wait_for_shutdown(Node) ->
    case net_adm:ping(Node) of
        pong ->
            timer:sleep(100),
            wait_for_shutdown(Node);
        pang ->
            ok
    end.

prop_ensemble() ->
    ?FORALL(Repetitions, ?SHRINK(1,[10]),
        ?FORALL(Cmds, more_commands(100, parallel_commands(?MODULE)),
            ?ALWAYS(Repetitions, begin
                ParallelCmds = element(2, Cmds),
                lager:info("number of parallel sequences= ~p",
                    [length(ParallelCmds)]),
                lager:info("Cmds in each parallel sequence= ~w",
                    [[length(C) || C <- ParallelCmds]]),
                lager:info("Parallel Cmds = ~p~n", [ParallelCmds]),
                {_, _, Res} = Result = run_parallel_commands(?MODULE, Cmds),
                aggregate(command_names(Cmds),
                    eqc_statem:pretty_commands(?MODULE, Cmds, Result,
                        begin
                            Res =:= ok
                        end))
            end))).

%% ==============================
%% EQC Callbacks
%% ==============================
initial_state() ->
    #state{}.

precondition(_State, _Call) ->
    true.

command(State) ->
    frequency([{10, {call, ?MODULE, create_ensemble, [ensemble()]}},
               {1, {call, ?MODULE, stop_node, [State]}},
%%               {10, {call, ?MODULE, start_node, [State]}},
               {100, {call, riak_ensemble_client, kput_once,
                       [ensemble(), key(), value(), ?REQ_TIMEOUT]}},
               {100, {call, riak_ensemble_client, kget,
                       [ensemble(), key(), ?REQ_TIMEOUT]}},
               {100, {call, riak_ensemble_client, kover,
                       [ensemble(), key(), value(), ?REQ_TIMEOUT]}},
               {40, {call, riak_ensemble_client, kdelete,
                       [ensemble(), key(), ?REQ_TIMEOUT]}},
               {30, {call, ?MODULE, ksafe_delete,
                       [ensemble(), key(), ?REQ_TIMEOUT]}},
               {100, {call, ?MODULE, kupdate,
                       [ensemble(), key(), value()]}}]).

postcondition(State, {call, _, kget, [Ensemble, Key, _]},
    {ok, {obj, _, _, _, notfound}}) ->
        is_missing(Ensemble, Key, State);
postcondition(#state{data=Data}, {call, _, kget, [Ensemble, Key, _]},
    {ok, {obj, _, _, _, Val}}) ->
        compare_val({Ensemble, Key}, Val, Data);
postcondition(_, {call, _, kget, _}, _)->
    true;
postcondition(_State, {call, ?MODULE, create_ensemble, [_Ensemble]}, _Res) ->
    true;
postcondition(_State, {call, _, kput_once, _}, _Res) ->
    true;
postcondition(_State, {call, _, kupdate, _}, {error, _}) ->
    true;
postcondition(_State, {call, _, kupdate, [_Ensemble, _Key, NewVal]},
    {ok, {obj, _, _, _, Val}}) ->
        NewVal =:= Val;
postcondition(_State, {call, _, ksafe_delete, [_Ensemble, _Key, _]},
    {ok, {obj, _, _, _, notfound}}) ->
        true;
postcondition(State, {call, _, ksafe_delete, [Ensemble, Key, _]},
    {error, notfound}) ->
        is_missing(Ensemble, Key, State);
postcondition(_State, {call, _, ksafe_delete, [_Ensemble, _Key, _]},
    {error, _}) ->
        true;
postcondition(_State, {call, _, kdelete, [_Ensemble, _Key, _]},
    {ok, {obj, _, _, _, notfound}}) ->
        true;
postcondition(_State, {call, _, kdelete, [_Ensemble, _Key, _]},
    {error, _}) ->
        true;
postcondition(_State, {call, ?MODULE, stop_node, [_]}, ok) ->
    true;
postcondition(_State, {call, _, kover, _}, _) ->
    true.

next_state(State=#state{ensembles=Ensembles}, Result,
    {call, ?MODULE, create_ensemble, [Ensemble]}) ->
        State#state{ensembles = {call, ?MODULE, maybe_add_ensemble,
                                 [Ensemble, Ensembles, Result]}};

next_state(State=#state{data=Data}, Result,
    {call, riak_ensemble_client, kget, [Ensemble, Key, _]}) ->
        %% In most protocols state doesn't change based on a read. But here, it
        %% can wipe out a chain of partial failures by giving a reliable answer.
        State#state{data = {call, ?MODULE, maybe_put_data,
                [Ensemble, Key, Data, Result]}};

next_state(State, Result, {call, _, ksafe_delete, [Ensemble, Key, _]}) ->
    State#state{data={call, ?MODULE, maybe_delete,
            [Ensemble, Key, Result, State]}};

next_state(State, Result, {call, _, kdelete, [Ensemble, Key, _]}) ->
    State#state{data={call, ?MODULE, maybe_delete,
        [Ensemble, Key, Result, State]}};

next_state(State=#state{data=Data},
    Result, {call, _, kput_once, [Ensemble, Key, Val, _]}) ->
        State#state{data = {call, ?MODULE, maybe_put_data,
                             [Ensemble, Key, Val, Data, Result]}};

next_state(State=#state{data=Data},
    Result, {call, _, kover, [Ensemble, Key, Val, _]}) ->
        State#state{data = {call, ?MODULE, maybe_put_data,
                [Ensemble, Key, Val, Data, Result]}};

next_state(State=#state{up_nodes=[]}, _Result, {call, _, stop_node, _}) ->
    State;
next_state(State=#state{up_nodes=[H | T], down_nodes=Down}, _Result,
    {call, _, stop_node, [_]}) ->
        State#state{up_nodes=T, down_nodes=[H | Down]};

next_state(State=#state{data=Data}, Result,
    {call, _, kupdate, [Ensemble, Key, Val]}) ->
        State#state{data = {call, ?MODULE, maybe_put_data,
                [Ensemble, Key, Val, Data, Result]}}.

%% ==============================

maybe_add_ensemble(Ensemble, Ensembles, ok) ->
    sets:add_element(Ensemble, Ensembles);
maybe_add_ensemble(_, Ensembles, _) ->
    Ensembles.

maybe_delete(_Ensemble, _Key, {error, _}, #state{data=Data}) ->
    Data;
maybe_delete(Ensemble, Key, {ok, _}, #state{data=Data}) ->
    dict:erase({Ensemble, Key}, Data).

%% maybe_put_data/4 is only done on reads, so we don't change our
%% state on partial failure since we aren't adding an actually new value
maybe_put_data(_Ensemble, _Key, Data, {error, _}) ->
    Data;
maybe_put_data(_, _, Data, {ok, {obj, _, _, _, notfound}}) ->
    Data;
maybe_put_data(Ensemble, Key, Data, {ok, {obj, _, _, _, Val}}=Res) ->
    maybe_put_data(Ensemble, Key, Val, Data, Res).

maybe_put_data(Ensemble, Key, Val, Data, {ok, _}) ->
    dict:store({Ensemble, Key}, Val, Data);
maybe_put_data(Ensemble, Key, Val, Data, {error, timeout}) ->
    partial_failure_write(Ensemble, Key, Val, Data);
maybe_put_data(_, _, _, Data, _) ->
    Data.

ksafe_delete(Ensemble, Key, Timeout) ->
    Node1 = node_name(1),
    case riak_ensemble_client:kget(Node1, Ensemble, Key, Timeout) of
        {ok, {obj, _, _, _, notfound}} ->
            {error, notfound};
        {ok, Obj} ->
            riak_ensemble_client:ksafe_delete(Node1, Ensemble, Key, Obj, Timeout);
        E ->
            E
    end.

kupdate(Ensemble, Key, Val) ->
    Node1 = node_name(1),
    case riak_ensemble_client:kget(Node1, Ensemble, Key, ?REQ_TIMEOUT) of
        {ok, {obj, _, _, _, notfound}} ->
            {error, notfound};
        {ok, CurrentObj} ->
            riak_ensemble_client:kupdate(Node1, Ensemble, Key, CurrentObj, Val,
                ?REQ_TIMEOUT);
        E ->
           E
    end.

create_ensemble(Ensemble) ->
    Res = rpc:call(node_name(1), riak_ensemble_manager, create_ensemble,
        [Ensemble, {Ensemble++"_peer", node()}, members(Ensemble),
            riak_ensemble_basic_backend, []]),
    wait_quorum(Ensemble),
    Res.

is_possible(Ensemble, Key, #state{data=Data}) ->
    case dict:find({Ensemble, Key}, Data) of
        {ok, {possible, _}} ->
            true;
        _ ->
            false
    end.

is_missing(Ensemble, Key, #state{data=Data}) ->
    case dict:find({Ensemble, Key}, Data) of
        error ->
            true;
        {ok, {possible, Vals}} ->
            lists:member(notfound, Vals);
        _V ->
            false
    end.

compare_val(EnsembleKey, ActualVal, Data) ->
    case dict:find(EnsembleKey, Data) of
        error ->
            false;
        {ok, {possible, Vals}} ->
            lists:member(ActualVal, Vals);
        {ok, Val} ->
            Val =:= ActualVal
    end.

partial_failure_write(Ensemble, Key, Val, Data) ->
    K = {Ensemble, Key},
    case dict:find(K, Data) of
        error ->
            dict:store(K, {possible, [notfound, Val]}, Data);
        {ok, {possible, Vals}} ->
            dict:store(K, {possible, [Val | Vals]}, Data);
        {ok, Val2} ->
            dict:store(K, {possible, [Val2, Val]}, Data)
    end.

%% ==============================
%% Generators
%% ==============================

ensemble() ->
    name("ensemble", 3).

key() ->
    name("key", 10).

name(Prefix, Max) ->
    oneof([Prefix ++ integer_to_list(I) ||
            I <- lists:seq(1, Max)]).

value() ->
    int().

node_name() ->
    oneof(node_names()).

%% ==============================
%% Internal Functions
%% ==============================
members(Ensemble) ->
    [{Ensemble ++ "_peer", node()} |
        [{Ensemble ++ "_peer", Node} || Node <- node_names()]].

initial_join([Node1 | Rest]) ->
    ok = rpc:call(Node1, riak_ensemble_manager, enable, []),
    [{root, Node1}] =
        rpc:call(Node1, riak_ensemble_manager, get_members, [root]),
    wait_quorum(root),
    Output = [rpc:call(Node1, riak_ensemble_manager, join, [Node1, Node])
        || Node <- Rest],
    ?assertEqual([ok || _ <- lists:seq(2, ?NUM_NODES)], Output),
    wait_cluster().

create_root_ensemble(Nodes) ->
        Leader = rpc:call(hd(Nodes), riak_ensemble_manager, rleader_pid, []),
        Changes = [{add, {root, Node}} || Node <- Nodes],
        riak_ensemble_peer:update_members(Leader, Changes, 10000),
        wait_quorum(root).

wait_quorum(Ensemble) ->
    case rpc:call(node_name(1), riak_ensemble_manager, check_quorum,
            [Ensemble, ?REQ_TIMEOUT]) of
        true ->
            ok;
        false ->
            timer:sleep(10),
            wait_quorum(Ensemble)
    end.

wait_cluster() ->
    try
        Cluster = rpc:call(node_name(1), riak_ensemble_manager, cluster, []),
        case length(Cluster) of
            ?NUM_NODES ->
                ok;
            _ ->
                timer:sleep(10),
                wait_cluster()
        end
    catch _:_ ->
        timer:sleep(10),
        wait_cluster()
    end.

cleanup_riak_ensemble_on_all_nodes() ->
   [cleanup_riak_ensemble(Path) || Path <- data_dirs()].

cleanup_riak_ensemble(Path) ->
    os:cmd("rm -rf "++Path).

start_riak_ensemble_on_all_nodes() ->
    [start_riak_ensemble(Node, Path)||
        {Node, Path} <- lists:zip(node_names(), data_dirs())].

start_riak_ensemble(Node, Path) ->
    rpc:call(Node, application, set_env, [riak_ensemble, data_root, Path], 5000),
    rpc:call(Node, application, ensure_all_started, [riak_ensemble], 5000).

data_dir(Num) ->
    "/tmp/dev" ++ integer_to_list(Num) ++ "_data".

data_dirs() ->
    [data_dir(Num) ||
        Num <- lists:seq(1, ?NUM_NODES)].

make_data_dirs() ->
    [os:cmd("mkdir -p " ++ Path) || Path <- data_dirs()].

stop_node(#state{up_nodes=[]}) ->
    ok;
stop_node(#state{up_nodes=[H | _]}) ->
    stop_node(H);
stop_node(Node) ->
    rpc:call(Node, init, stop, [], 10000),
    wait_for_shutdown(Node).

node_names() ->
    [node_name(Num) || Num <- lists:seq(1, ?NUM_NODES)].

other_node_names() ->
    [node_name(Num) || Num <- lists:seq(2, ?NUM_NODES)].

node_name(Num) ->
    list_to_atom("dev" ++ integer_to_list(Num) ++ "@127.0.0.1").

