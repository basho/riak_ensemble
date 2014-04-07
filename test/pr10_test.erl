-module(pr10_test).

-include_lib("riak_ensemble_types.hrl").

-define(NUM_NODES, 5).

-compile([export_all]).

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
    timer:sleep(1000),
    ensemble_join_nodes(Nodes),
    UpdateRootEnsembleOutput = update_root_ensemble(Nodes),
    io:format("update_root_ensemble_output = ~p~n", [UpdateRootEnsembleOutput]),
    ok.

stop_nodes() ->
    teardown_nodes(?NUM_NODES).

cleanup() ->
    {ok, _Nodes} = teardown_nodes(?NUM_NODES),
    [os:cmd("rm -rf "++Dir) || Dir <- data_dirs()],
    ok.

%% ==============================
%% Internal Functions
%% ==============================

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

ensemble_join_nodes(Nodes) ->
    Node1 = node(),
    io:format("Enable ~p~n", [Node1]),
    ok = riak_ensemble_manager:enable(),
    [{root, Node1}] = riak_ensemble_manager:get_members(root),
    wait_quorum(root),
    io:format("ensemble_join_nodes = ~p to ~p~n", [Nodes, Node1]),
    Output = [riak_ensemble_manager:join(Node1, Node) || Node <- Nodes],
    io:format("ensemble_join_nodes output = ~p~n", [Output]),
    wait_cluster().

update_root_ensemble(Nodes) ->
        wait_quorum(root),
        io:format("Members = ~p~n", [riak_ensemble_manager:get_members(root)]),
        io:format("Cluster = ~p~n", [riak_ensemble_manager:cluster()]),
        Leader = riak_ensemble_manager:rleader_pid(),
        Changes = [{add, {root, Node}} || Node <- Nodes],
        riak_ensemble_peer:update_members(Leader, Changes, 10000).

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
    Str0 = "kill -9 `ps -ef | grep ~p | cut -d \" \" -f 4`",
    [H | T] = atom_to_list(NodeName),
    Pattern = "[" ++ [H] ++ "]" ++ T,
    Str = io_lib:format(Str0, [Pattern]),
    os:cmd(Str).

node_names() ->
    node_names(?NUM_NODES).

node_names(NumNodes) ->
    [list_to_atom("dev" ++ integer_to_list(Num) ++ "@127.0.0.1") ||
        Num <- lists:seq(2, NumNodes)].
