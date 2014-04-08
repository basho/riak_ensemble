-module(pr10_test).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_ensemble_types.hrl").

-behaviour(eqc_statem).

%% eqc_statem exports
-export([command/1, initial_state/0, next_state/3, postcondition/3,
         precondition/2]).

-compile(export_all).

-define(NUM_NODES, 5).

-record(state, {ensembles=sets:new() :: set(),
                data=dict:new() :: dict()}).

setup() ->
    lager:start(),
    cleanup(),
    make_data_dirs(),
    setup_this_node(),
    {ok, Nodes} = launch_nodes(?NUM_NODES),
    %% allow nodes to start with a dirty, dirty sleep
    timer:sleep(1000),
    io:format("NODES = ~p~n", [Nodes]),
    [pong = net_adm:ping(Node) || Node <- node_names(?NUM_NODES)],
    Output = start_riak_ensemble_on_all_nodes(),
    io:format("~p~n", [Output]),
    initial_join(Nodes),
    RootEnsembleOutput = create_root_ensemble(Nodes),
    ?assertEqual(ok, RootEnsembleOutput).

stop_nodes() ->
    teardown_nodes(?NUM_NODES).

cleanup() ->
    {ok, _Nodes} = teardown_nodes(?NUM_NODES),
    [os:cmd("rm -rf "++Dir) || Dir <- data_dirs()],
    ok.

prop_normal() ->
    ?FORALL(Cmds, commands(?MODULE), 
        begin
            {_H, _S, Res} = Result = run_commands(?MODULE, Cmds),
            aggregate(command_names(Cmds),
                eqc_statem:pretty_commands(?MODULE, Cmds, Result, 
                    begin
                        Res =:= ok
                    end))
         end).

%% ==============================
%% EQC Callbacks
%% ==============================
initial_state() ->
    #state{}.

precondition(_State, _Call) ->
    true.

command(_State=#state{ensembles=_Ensembles}) ->
    frequency([{1, {call, ?MODULE, create_ensemble, [ensemble()]}},
               {10, {call, riak_ensemble_client, kput_once, 
                       [node(), ensemble(), key(), value(), 1000]}},
               {10, {call, riak_ensemble_client, kget, 
                       [node(), ensemble(), key(), 1000]}}]).

postcondition(_State, {call, _, kget, _}, {ok, {obj, _, _, _, notfound}}) ->
    true;
postcondition(#state{data=Data}, {call, _, kget, [_, Ensemble, Key, _]}, 
    {ok, {obj, _, _, _, Val}}) ->
        Val =:= dict:fetch({Ensemble, Key}, Data);
postcondition(_, {call, _, kget, _}, _)->
    true;
postcondition(_State, {call, ?MODULE, create_ensemble, [_Ensemble]}, _Res) ->
    true;
postcondition(_State, {call, _, kput_once, _}, _Res) ->
    true.

next_state(State, _Result, {call, riak_ensemble_client, kget, _}) ->
    State;
next_state(State=#state{ensembles=Ensembles}, Result, 
    {call, ?MODULE, create_ensemble, [Ensemble]}) ->
        State#state{ensembles = {call, ?MODULE, maybe_add_ensemble, 
                                 [Ensemble, Ensembles, Result]}};
next_state(State=#state{data=Data}, 
    Result, {call, _, kput_once, [_, Ensemble, Key, Val, _]}) ->
        State#state{data = {call, 
                            ?MODULE, 
                            maybe_put_data(Ensemble, Key, Val, Data, Result)}}.

%% ==============================

maybe_add_ensemble(Ensemble, Ensembles, ok) ->
    sets:add_element(Ensemble, Ensembles);
maybe_add_ensemble(_, Ensembles, _) ->
    Ensembles.

maybe_put_data(Ensemble, Key, Val, Data, {ok, _}) ->
    dict:store({Ensemble, Key}, Val, Data);
maybe_put_data(_, _, _, Data, _) ->
    Data.

create_ensemble(Ensemble) ->
    riak_ensemble_manager:create_ensemble(Ensemble, {Ensemble++"_peer", node()}, 
        members(Ensemble), riak_ensemble_basic_backend, []).

query_no_such_ensemble() ->
    Val = riak_ensemble_client:kget(no_such_ensemble, somekey, 1000),
    %% TODO: Eventually this should return a no_such_ensemble error
    ?assertEqual({error, timeout}, Val).

%% ==============================
%% Generators
%% ==============================

-define(MAX_ENSEMBLES, 10).

ensemble() -> 
    name("ensemble").

key() ->
    name("key").

name(Prefix) ->
    oneof([Prefix ++ integer_to_list(I) || 
            I <- lists:seq(1, ?MAX_ENSEMBLES)]).

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

initial_join(Nodes) ->
    Node1 = node(),
    io:format("Enable ~p~n", [Node1]),
    ok = riak_ensemble_manager:enable(),
    [{root, Node1}] = riak_ensemble_manager:get_members(root),
    wait_quorum(root),
    io:format("ensemble_join_nodes = ~p to ~p~n", [Nodes, Node1]),
    Output = [riak_ensemble_manager:join(Node1, Node) || Node <- Nodes],
    ?assertEqual([ok || _ <- lists:seq(2, ?NUM_NODES)], Output),
    wait_cluster().

create_root_ensemble(Nodes) ->
        wait_quorum(root),
        Leader = riak_ensemble_manager:rleader_pid(),
        Changes = [{add, {root, Node}} || Node <- Nodes],
        riak_ensemble_peer:update_members(Leader, Changes, 10000).

setup_this_node() ->
    os:cmd("rm -rf "++data_dir(1)),
    {ok, _} = net_kernel:start(['dev1@127.0.0.1']),
    erlang:set_cookie(node(), riak_ensemble_test),
    application:set_env(riak_ensemble, data_root, data_dir(1)),
    application:ensure_all_started(riak_ensemble).

wait_quorum(Ensemble) ->
    %% ?FMT("leader: ~p~n", [riak_ensemble_manager:get_leader(test)]),
    case riak_ensemble_manager:check_quorum(Ensemble, 1000) of
        true ->
            ok;
        false ->
            %% erlang:yield(),
            %% timer:sleep(1000),
            wait_quorum(Ensemble)
    end.

wait_cluster() ->
    case length(riak_ensemble_manager:cluster()) of 
        ?NUM_NODES ->
            ok;
        _ ->
            wait_cluster()
    end.

start_riak_ensemble_on_all_nodes() ->
    [start_riak_ensemble(Node, Path)||
        {Node, Path} <- lists:zip(node_names(?NUM_NODES), data_dirs())].

start_riak_ensemble(Node, Path) ->
    rpc:call(Node, application, set_env, [riak_ensemble, data_root, Path]),
    rpc:call(Node, application, ensure_all_started, [riak_ensemble]).

data_dir(Num) ->
    "/tmp/dev" ++ integer_to_list(Num) ++ "_data".
data_dirs() ->
    [data_dir(Num) ||
        Num <- lists:seq(2, ?NUM_NODES)].

make_data_dirs() ->
    [os:cmd("mkdir -p " ++ Path) || Path <- data_dirs()].

launch_node(NodeName, Num) ->
    FmtStr =
        "run_erl -daemon ~p/ ~p \"erl -pa ebin -setcookie riak_ensemble_test -name ~p\"",
    Dir = data_dir(Num),
    Str = io_lib:format(FmtStr, [Dir, Dir, NodeName]),
    Output = os:cmd(Str),
    io:format("Output = ~p~n", [Output]),
    ok.

-spec launch_nodes(non_neg_integer()) -> {ok, [node()]} | {error, term()}.
launch_nodes(NumNodes) when is_integer(NumNodes) ->
    launch_nodes(node_names(NumNodes));
launch_nodes(NodeNames) ->
    FmtStr =
        "run_erl -daemon ~p/ ~p \"erl -pa ebin deps/*/ebin -setcookie riak_ensemble_test -name ~p\"",
    Output = lists:map(fun({NodeName, Dir}) ->
                           Str = io_lib:format(FmtStr, [Dir, Dir, NodeName]),
                           os:cmd(Str)
                       end, lists:zip(NodeNames, data_dirs())),
    io:format("Output = ~p~n", [Output]),
    {ok, NodeNames}.

teardown_nodes(NumNodes) when is_integer(NumNodes) ->
    teardown_nodes(node_names(NumNodes));
teardown_nodes(NodeNames) ->
    %% TODO: parse and handle output
    Output = [kill_node(Node) || Node <- NodeNames],
    io:format("Output = ~p~n", [Output]),
  {ok, NodeNames}.

kill_node(NodeName) ->
    Str0 = "kill -9 `ps -ef | grep ~p | cut -d \" \" -f 5`",
    [H | T] = atom_to_list(NodeName),
    Pattern = "[" ++ [H] ++ "]" ++ T,
    Str = io_lib:format(Str0, [Pattern]),
    os:cmd(Str).

node_names() ->
    node_names(?NUM_NODES).

node_names(NumNodes) ->
    [list_to_atom("dev" ++ integer_to_list(Num) ++ "@127.0.0.1") ||
        Num <- lists:seq(2, NumNodes)].
