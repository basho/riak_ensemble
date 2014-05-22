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
-define(REQ_TIMEOUT, 5000).
-define(QUORUM_TIMEOUT, 10000).
-define(QC_TIMEOUT, 300).
-define(EUNIT_TIMEOUT, 2*?QC_TIMEOUT).

-record(state, {ensembles=sets:new() :: set(),
                data=dict:new() :: dict(),
                up_nodes=node_names() :: list(atom())}).

ensemble_test_() ->
    {setup, spawn, fun setup/0, fun cleanup/1, [
        {timeout, ?EUNIT_TIMEOUT,
            ?_assertEqual(true, quickcheck(?QC_OUT(eqc:testing_time(?QC_TIMEOUT,
                            prop_ensemble()))))}]}.

setup() ->
    net_kernel:start(['ensemble_tester@127.0.0.1']),
    erlang:set_cookie(node(), riak_ensemble_test),
    lager:start(),
    cleanup_riak_ensemble_on_all_nodes().

setup_prop() ->
    cleanup_riak_ensemble_on_all_nodes(),
    Nodes = node_names(),
    make_data_dirs(),
    _Output = start_riak_ensemble_on_all_nodes(),
    io:format(user, "setup_prop _OUTPUT = ~p~n", [_Output]),
    wait_for_manager(Nodes),
    initial_join(Nodes),
    ok = create_root_ensemble(Nodes),
    create_ensembles().

cleanup(_) ->
    ok.

create_ensembles() ->
    [ok = create_ensemble(Ensemble) || Ensemble <- ["ensemble1", "ensemble2"]].

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
wait_for_nodeups([Node | T]) ->
    case net_adm:ping(Node) of
        pong ->
            lager:info("Wait_for_nodeups PONG ~p~n", [Node]),
            wait_for_nodeups(T);
        pang ->
            lager:info("Wait_for_nodeups PANG ~p~n", [Node]),
            timer:sleep(10),
            wait_for_nodeups(T ++ [Node])
    end.

wait_for_shutdown(Node) ->
    case net_adm:ping(Node) of
        pong ->
            io:format(user, "Wait for shutdown PONG ~p~n", [Node]),
            timer:sleep(100),
            wait_for_shutdown(Node);
        pang ->
            io:format(user, "Wait for shutdown PANG ~p~n", [Node]),
            timer:sleep(5000),
            ok
    end.

prop_ensemble() ->
    ?FORALL(Repetitions, ?SHRINK(1,[3]),
        ?FORALL(Cmds, more_commands(100, parallel_commands(?MODULE)),
            ?ALWAYS(Repetitions, begin

                ParallelCmds = element(2, Cmds),
                lager:info("number of parallel sequences= ~p",
                    [length(ParallelCmds)]),
                lager:info("Cmds in each parallel sequence= ~w",
                    [[length(C) || C <- ParallelCmds]]),

                setup_prop(),

                {_SeqH, _ParH, Res} = Result = run_parallel_commands(?MODULE, Cmds),
                %%lager:info("Sequential Commands = ~p~n", [_SeqH]),
                %%lager:info("Parallel Commands = ~p~n", [_ParH]),

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

command(_State) ->
    frequency([{10, {call, ?MODULE, stop_node, [node_name()]}},
%%               {10, {call, ?MODULE, start_node, [State]}},
               {100, {call, riak_ensemble_client, kput_once,
                       [node_name(), ensemble(), key(), value(),
                           ?REQ_TIMEOUT]}},
               {100, {call, riak_ensemble_client, kget,
                       [node_name(), ensemble(), key(), ?REQ_TIMEOUT]}},
               {100, {call, riak_ensemble_client, kover,
                       [node_name(), ensemble(), key(), value(),
                           ?REQ_TIMEOUT]}},
               {40, {call, riak_ensemble_client, kdelete,
                       [node_name(), ensemble(), key(), ?REQ_TIMEOUT]}},
               {30, {call, ?MODULE, ksafe_delete,
                       [ensemble(), key(), ?REQ_TIMEOUT]}},
               {100, {call, ?MODULE, kupdate,
                       [ensemble(), key(), value()]}}]).

postcondition(State, {call, _, kget, [_, Ensemble, Key, _]},
    {ok, {obj, _, _, _, notfound}}) ->
        is_missing(Ensemble, Key, State);
postcondition(#state{data=Data}, {call, _, kget, [_, Ensemble, Key, _]},
    {ok, {obj, _, _, _, Val}}) ->
        compare_val({Ensemble, Key}, Val, Data);
postcondition(State, {call, _, kget, [Node, _, _, _]}, {error, timeout})->
    is_valid_timeout(State, Node);
postcondition(_, {call, _, kget, _}, _)->
    true;
postcondition(_State, {call, _, kput_once, _}, _Res) ->
    true;
postcondition(State, {call, _, kupdate, [Ensemble, Key, _Val]},
    {error, notfound}) ->
        is_missing(Ensemble, Key, State);
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
postcondition(_State, {call, _, kdelete, [_, _Ensemble, _Key, _]},
    {ok, {obj, _, _, _, notfound}}) ->
        true;
postcondition(_State, {call, _, kdelete, [_, _Ensemble, _Key, _]},
    {error, _}) ->
        true;
postcondition(_State, {call, ?MODULE, stop_node, [_]}, _) ->
    true;
postcondition(_State, {call, _, kover, _}, _) ->
    true.


next_state(State=#state{data=Data}, Result,
    {call, riak_ensemble_client, kget, [_, Ensemble, Key, _]}) ->
        %% In most protocols state doesn't change based on a read. But here, it
        %% can wipe out a chain of partial failures by giving a reliable answer.
        State#state{data = {call, ?MODULE, maybe_put_data,
                [Ensemble, Key, Data, Result]}};

next_state(State, Result, {call, _, ksafe_delete, [Ensemble, Key, _]}) ->
    State#state{data={call, ?MODULE, maybe_delete,
            [Ensemble, Key, Result, State]}};

next_state(State, Result, {call, _, kdelete, [_, Ensemble, Key, _]}) ->
    State#state{data={call, ?MODULE, maybe_delete,
        [Ensemble, Key, Result, State]}};

next_state(State=#state{data=Data},
    Result, {call, _, kput_once, [_, Ensemble, Key, Val, _]}) ->
        State#state{data = {call, ?MODULE, maybe_put_data,
                             [Ensemble, Key, Val, Data, Result]}};

next_state(State=#state{data=Data},
    Result, {call, _, kover, [_, Ensemble, Key, Val, _]}) ->
        State#state{data = {call, ?MODULE, maybe_put_data,
                [Ensemble, Key, Val, Data, Result]}};

next_state(State=#state{up_nodes=Up}, _Result,
    {call, _, stop_node, [Node]}) ->
        State#state{up_nodes=lists:delete(Node, Up)};

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
    Node = random_node(),
    case riak_ensemble_client:kget(Node, Ensemble, Key, Timeout) of
        {ok, {obj, _, _, _, notfound}} ->
            {error, notfound};
        {ok, Obj} ->
            riak_ensemble_client:ksafe_delete(Node, Ensemble, Key, Obj, Timeout);
        E ->
            E
    end.

kupdate(Ensemble, Key, Val) ->
    Node = random_node(),
    case riak_ensemble_client:kget(Node, Ensemble, Key, ?REQ_TIMEOUT) of
        {ok, {obj, _, _, _, notfound}} ->
            {error, notfound};
        {ok, CurrentObj} ->
            riak_ensemble_client:kupdate(Node, Ensemble, Key, CurrentObj, Val,
                ?REQ_TIMEOUT);
        E ->
           E
    end.

create_ensemble(Ensemble) ->
    Res = rpc:call(node_name(1), riak_ensemble_manager, create_ensemble,
        [Ensemble, undefined, members(Ensemble),
            riak_ensemble_basic_backend, []]),
    io:format(user, "Create Ensemble ~p, Res = ~p~n", [Ensemble, Res]),
    wait_quorum(Ensemble),
    Res.

is_valid_timeout(#state{up_nodes=Up}, Node) ->
    length(Up) < ?NUM_NODES div 2 + 1 orelse not lists:member(Node, Up).

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
    name("ensemble", 2).

key() ->
    name("key", 2).

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
        [{Ensemble, Node} || Node <- node_names()].

random_node() ->
    lists:nth(random:uniform(?NUM_NODES), node_names()).

initial_join([Node1 | Rest]) ->
    ok = rpc:call(Node1, riak_ensemble_manager, enable, []),
    [{root, Node1}] =
        rpc:call(Node1, riak_ensemble_manager, get_members, [root]),
    wait_quorum_initial(),
    Output = [rpc:call(Node1, riak_ensemble_manager, join, [Node1, Node])
        || Node <- Rest],
    ?assertEqual([ok || _ <- lists:seq(2, ?NUM_NODES)], Output),
    wait_cluster().

create_root_ensemble([H | T]) ->
        Leader = rpc:call(H, riak_ensemble_manager, rleader_pid, []),
        Changes = [{add, {root, Node}} || Node <- T],
        riak_ensemble_peer:update_members(Leader, Changes, 10000),
        wait_quorum(root).

wait_quorum_initial() ->
    case rpc:call(node_name(1), riak_ensemble_manager, check_quorum,
            [root, ?QUORUM_TIMEOUT]) of
        true ->
            ok;
        false ->
            timer:sleep(50),
            wait_quorum_initial()
    end.

wait_quorum(Ensemble) ->
    case rpc:call(random_node(), riak_ensemble_manager, check_quorum,
            [Ensemble, ?QUORUM_TIMEOUT]) of
        true ->
            case rpc:call(random_node(), riak_ensemble_manager, count_quorum,
                    [Ensemble, ?QUORUM_TIMEOUT]) of
                N when N > 1 ->
                    lager:info("Reached Quorum for Ensemble ~p with ~p Responses",
                        [Ensemble, N]),
                    ok;
                _ ->
                    wait_quorum(Ensemble)
            end;
        false ->
            timer:sleep(10),
            wait_quorum(Ensemble);
        {badrpc, nodedown} ->
            io:format(user, "Nodedown, Wait Quorum Ensemble = ~p~n", [Ensemble]),
            timer:sleep(10),
            wait_quorum(Ensemble)
    end.

wait_cluster() ->
    try
        Cluster = rpc:call(node_name(1), riak_ensemble_manager, cluster, []),
        case length(Cluster) of
            ?NUM_NODES ->
                ok;
            _R ->
                timer:sleep(10),
                wait_cluster()
        end
    catch _:_=Z ->
        io:format(user, "Wait Cluster: exception ~p~n", [Z]),
        timer:sleep(10),
        wait_cluster()
    end.

cleanup_riak_ensemble_on_all_nodes() ->
    Nodes = node_names(),
    [cleanup_riak_ensemble(Node, Path) ||
        {Node, Path} <- lists:zip(Nodes, data_dirs())],
    wait_for_nodeups(Nodes).


cleanup_riak_ensemble(Node, Path) ->
    case rpc:call(Node, application, stop, [riak_ensemble], 2000) of
        {badrpc, _} ->
            Status = erl_port:start(Node),
            case Status of
                {error,{already_started, Pid}} ->
                    exit(Pid, normal),
                    _Output = erl_port:start(Node),
                    io:format(user, "cleanup_riak_ensemble output = ~p~n", [_Output]);
                {ok, _} ->
                    ok
            end;
        _ ->
            ok
    end,
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

stop_node(Node) ->
    Out = rpc:call(Node, init, stop, [], 10000),
    lager:info("Stopped Node ~p with Output ~p~n", [Node, Out]),
    wait_for_shutdown(Node),
    %% erl_port gen_servers are registered with Node names for simplicity
    %% In case the erl process exits, but this pid doesn't, shut it down
    Pid = whereis(Node),
    case is_pid(Pid) of
        true ->
            exit(Pid, kill);
        _ ->
            ok
    end.

node_names() ->
    [node_name(Num) || Num <- lists:seq(1, ?NUM_NODES)].

other_node_names() ->
    [node_name(Num) || Num <- lists:seq(2, ?NUM_NODES)].

node_name(Num) ->
    list_to_atom("dev" ++ integer_to_list(Num) ++ "@127.0.0.1").

