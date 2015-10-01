%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% TODO: Before PR. Module + other edocs, general cleanup/refactor.

-module(riak_ensemble_manager).

%% API
-export([start/0,
         start_link/0,
         cluster/0,
         get_cluster_state/0,
         get_peer_pid/2,
         get_leader_pid/1,
         get_members/1,
         get_views/1,
         get_pending/1,
         get_leader/1,
         get_peer_info/2,
         rleader_pid/0,
         update_ensemble/4,
         gossip/1,
         gossip_pending/3,
         join/2,
         remove/2,
         enabled/0,
         enable/0,
         subscribe/1,
         create_ensemble/4,
         create_ensemble/5,
         known_ensembles/0,
         check_quorum/2,
         count_quorum/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("riak_ensemble_types.hrl").
-define(ETS, ?MODULE).

-type vsn_views()     :: {vsn(), views()}.
-type ensemble_data() :: {ensemble_id(), leader_id(), vsn_views(), vsn_views()}.
-type ensembles()     :: orddict(ensemble_id(), ensemble_data()).
-type cluster_state() :: riak_ensemble_state:state().

-record(state, {version       :: integer(),
                ensemble_data :: ensembles(),
                remote_peers  :: orddict(peer_id(), pid()),
                cluster_state :: cluster_state(),
                subscribers :: [fun()]
               }).

-type state() :: #state{}.

-type call_msg() :: {join, node()} |
                    {remove, node()}.

-type cast_msg() :: {update_ensembles, [{ensemble_id(), ensemble_info()}]} |
                    {update_root_ensemble, ensemble_info()}                |
                    {peer_pid, peer_id(), pid()}                           |
                    {request_peer_pid,pid(), {ensemble_id(), peer_id()}}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start() -> 'ignore' | {'error',_} | {'ok',pid()}.
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

join(Same, Same) ->
    {error, same_node};
join(Node, OtherNode) ->
    typed_call(OtherNode, {join, Node}, infinity).

remove(Same, Same) ->
    {error, same_node};
remove(Node, NodeToRemove) ->
    typed_call(Node, {remove, NodeToRemove}, infinity).

-spec enabled() -> boolean().
enabled() ->
    try
        ets:lookup_element(?ETS, enabled, 2)
    catch _:_ ->
            false
    end.

-spec enable() -> ok | error.
enable() ->
    gen_server:call(?MODULE, enable, infinity).

-spec get_leader_pid(ensemble_id()) -> undefined | pid().
get_leader_pid(Ensemble) ->
    case get_leader(Ensemble) of
        undefined ->
            undefined;
        Leader ->
            get_peer_pid(Ensemble, Leader)
    end.

-spec get_peer_info(ensemble_id(), peer_id()) -> nodedown | undefined | peer_info().
get_peer_info(Ensemble, Peer={_, Node}) ->
    try
        gen_server:call({?MODULE, Node}, {peer_info, Ensemble, Peer}, 60000)
    catch
        exit:{{nodedown, _},_} ->
            nodedown
    end.

subscribe(Callback)->
    gen_server:call(?MODULE, {subscribe, Callback}, infinity).


%%%===================================================================
%%% Gossip/State API
%%%===================================================================

gossip(CS) ->
    gen_server:call(?MODULE, {gossip, CS}, infinity).

gossip_pending(Ensemble, Vsn, Views) ->
    gen_server:cast(?MODULE, {gossip_pending, Ensemble, Vsn, Views}).

%%%===================================================================
%%% Root-based API
%%%===================================================================

-spec rleader() -> 'undefined' | peer_id().
rleader() ->
    get_leader(root).

-spec rleader_pid() -> pid() | undefined.
rleader_pid() ->
    get_peer_pid(root, rleader()).

-spec update_ensemble(ensemble_id(), peer_id(), views(), vsn()) -> ok.
update_ensemble(Ensemble, Leader, Views, Vsn) ->
    gen_server:call(?MODULE, {update_ensemble, Ensemble, Leader, Views, Vsn}, infinity).

-spec create_ensemble(ensemble_id(), peer_id(), module(), [any()])
                     -> ok | {error, term()}.
create_ensemble(EnsembleId, PeerId, Mod, Args) ->
    create_ensemble(EnsembleId, PeerId, [PeerId], Mod, Args).

-spec create_ensemble(ensemble_id(), leader_id(), [peer_id()], module(), [any()])
                     -> ok | {error, term()}.
create_ensemble(EnsembleId, EnsLeader, Members, Mod, Args) ->
    Info = #ensemble_info{leader=EnsLeader, views=[Members], seq={0,0}, vsn={0,0}, mod=Mod, args=Args},
    riak_ensemble_root:set_ensemble(EnsembleId, Info).

-spec known_ensembles() -> undefined |
                           {ok, orddict(ensemble_id(), ensemble_info())}.
known_ensembles() ->
    case riak_ensemble_peer:kget(node(), root, cluster_state, 5000) of
        {ok, Obj} ->
            case riak_ensemble_peer:obj_value(Obj, riak_ensemble_basic_backend) of
                notfound ->
                    undefined;
                State ->
                    Ensembles = riak_ensemble_state:ensembles(State),
                    {ok, Ensembles}
            end;
        Error ->
            Error
    end.

%%%===================================================================
%%% ETS-based API
%%%===================================================================

-spec cluster() -> [node()].
cluster() ->
    try
        ets:lookup_element(?ETS, cluster, 2)
    catch _:_ ->
            []
    end.

-spec get_cluster_state() -> cluster_state().
get_cluster_state() ->
    ets:lookup_element(?ETS, cluster_state, 2).

-spec get_peer_pid(ensemble_id(), peer_id()) -> pid() | undefined.
get_peer_pid(Ensemble, PeerId={_, Node}) ->
    case node() of
        Node ->
            %% Local peer
            riak_ensemble_peer_sup:get_peer_pid(Ensemble, PeerId);
        _ ->
            %% Remote peer
            get_remote_peer_pid(Ensemble, PeerId)
    end.

get_remote_peer_pid(Ensemble, PeerId) ->
    try
        ets:lookup_element(?ETS, {remote_pid, {Ensemble, PeerId}}, 2)
    catch
        _:_ ->
            undefined
    end.

-spec get_pending(ensemble_id()) -> {vsn(), [[peer_id()]]} | undefined.
get_pending(EnsembleId) ->
    try
        ets:lookup_element(?ETS, EnsembleId, 4)
    catch _:_ ->
            undefined
    end.

-spec get_members(ensemble_id()) -> [peer_id()].
get_members(EnsembleId) ->
    Views = try
                {_, Vs} = ets:lookup_element(?ETS, EnsembleId, 3),
                Vs
            catch _:_ ->
                    []
            end,
    compute_members(Views).

-spec get_views(ensemble_id()) -> {vsn(), views()}.
get_views(EnsembleId) ->
    try
        ets:lookup_element(?ETS, EnsembleId, 3)
    catch _:_ ->
            {{0,0}, []}
    end.

-spec get_leader(ensemble_id()) -> leader_id().
get_leader(EnsembleId) ->
    try
        ets:lookup_element(?ETS, EnsembleId, 2)
    catch _:_ ->
            undefined
    end.

-spec check_quorum(ensemble_id(), timeout()) -> boolean().
check_quorum(Ensemble, Timeout) ->
    case riak_ensemble_peer:check_quorum(Ensemble, Timeout) of
        ok ->
            true;
        _ ->
            false
    end.

-spec count_quorum(ensemble_id(), timeout()) -> integer().
count_quorum(Ensemble, Timeout) ->
    case riak_ensemble_peer:count_quorum(Ensemble, Timeout) of
        timeout ->
            0;
        Count when is_integer(Count) ->
            Count
    end.

%%%===================================================================

-spec typed_call(node(), call_msg(), infinity) -> any().
typed_call(Node, Msg, Timeout) ->
    gen_server:call({?MODULE, Node}, Msg, Timeout).

-spec typed_cast(node() | pid(), cast_msg()) -> ok.
typed_cast(Pid, Msg) when is_pid(Pid) ->
    gen_server:cast(Pid, Msg);
typed_cast(Node, Msg) when is_atom(Node) ->
    gen_server:cast({?MODULE, Node}, Msg).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {ok, state()}.
init([]) ->
    _ = ets:new(?ETS, [named_table, public, {read_concurrency, true}, {write_concurrency, true}]),
    State = load_subscribers(reload_state()),
    schedule_tick(),
    true = ets:insert(?ETS, {cluster_state, State#state.cluster_state}),
    gen_server:cast(self(), init),
    {ok, State}.

handle_call(enable, _From, State) ->
    case activate(State) of
        {ok, State2, RootInfo} ->
            case maybe_save_state(State2) of
                ok ->
                    ok = riak_ensemble_storage:sync(),
                    ets:insert(?ETS, RootInfo),
                    State3 = state_changed(State2),
                    {reply, ok, State3};
                {error,_} ->
                    {reply, error, State}
            end;
        error ->
            {reply, error, State}
    end;
handle_call({join, Node}, _From, State=#state{cluster_state=LocalCS}) ->
    %% Note: The call here can deadlock if we ever join A -> B and B -> A
    %% at the same time. There is no reason that should ever occur, but
    %% to be safe we use a non-infinity timeout.
    case gen_server:call({?MODULE, Node}, get_cluster_state, 60000) of
        {ok, RemoteCS} ->
            case join_allowed(LocalCS, RemoteCS) of
                true ->
                    State2 = State#state{cluster_state=RemoteCS},
                    case maybe_save_state(State2) of
                        ok ->
                            Reply = riak_ensemble_root:join(Node),
                            State3 = state_changed(State2),
                            {reply, Reply, State3};
                        Error={error,_} ->
                            %% Failed to save, keep original state
                            {reply, Error, State}
                    end;
                Error ->
                    {reply, Error, State}
            end;
        _ ->
            {reply, error, State}
    end;
handle_call({remove, Node}, _From, State) ->
    Reply = riak_ensemble_root:remove(Node),
    State2 = state_changed(State),
    {reply, Reply, State2};

handle_call(get_cluster_state, _From, State=#state{cluster_state=CS}) ->
    {reply, {ok, CS}, State};

handle_call({update_ensemble, Ensemble, Leader, Views, Vsn}, _From, State=#state{cluster_state=CS}) ->
    case riak_ensemble_state:update_ensemble(Vsn, Ensemble, Leader, Views, CS) of
        error ->
            {reply, error, State};
        {ok, NewCS} ->
            save_state_reply(NewCS, State)
    end;

handle_call({gossip, OtherCS}, _From, State) ->
    NewCS = merge_gossip(OtherCS, State),
    save_state_reply(NewCS, State);

handle_call({peer_info, Ensemble, Peer}, From, State) ->
    case get_peer_pid(Ensemble, Peer) of
        undefined ->
            {reply, undefined, State};
        Pid ->
            spawn_link(fun() ->
                               try
                                   Info = riak_ensemble_peer:get_info(Pid),
                                   gen_server:reply(From, Info)
                               catch
                                   _:_ ->
                                       gen_server:reply(From, undefined)
                               end
                       end),
            {noreply, State}
    end;

handle_call({subscribe, Callback}, _, State=#state{subscribers=Subscribers}) ->
    %% TODO: Change subscriber API to prevent multiple subscriptions with the same callback.
    Subscribers2 = [Callback|Subscribers],
    application:set_env(riak_ensemble, subscribers, Subscribers2),
    State2 = State#state{subscribers=Subscribers2},
    {reply, ok, State2};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({gossip, OtherCS}, State) ->
    NewCS = merge_gossip(OtherCS, State),
    save_state_noreply(NewCS, State);

handle_cast({gossip_pending, Ensemble, Vsn, Views}, State=#state{cluster_state=CS}) ->
    case riak_ensemble_state:set_pending(Vsn, Ensemble, Views, CS) of
        error ->
            {noreply, State};
        {ok, NewCS} ->
            save_state_noreply(NewCS, State)
    end;

handle_cast({peer_pid, Peer, Pid}, State=#state{remote_peers=Remote}) ->
    Remote2 = orddict:store(Peer, Pid, Remote),
    ets:insert(?ETS, {{remote_pid, Peer}, Pid}),
    erlang:monitor(process, Pid),
    %% io:format("Tracking remote peer: ~p :: ~p~n", [Peer, Pid]),
    {noreply, State#state{remote_peers=Remote2}};

handle_cast({request_peer_pid, From, PeerId}, State) ->
    %% TODO: Confusing that we use {Ensemble, PeerId} as PeerId
    {Ensemble, Id} = PeerId,
    case get_peer_pid(Ensemble, Id) of
        undefined ->
            ok;
        Pid ->
            typed_cast(From, {peer_pid, PeerId, Pid})
    end,
    {noreply, State};

handle_cast(init, State) ->
    State2 = state_changed(State),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

save_state_noreply(NewCS, State) ->
    State2 = State#state{cluster_state=NewCS},
    case maybe_save_state(State2) of
        ok ->
            State3 = state_changed(State2),
            {noreply, State3};
        {error,_} ->
            %% Failed to save, keep original state
            {noreply, State}
    end.

save_state_reply(NewCS, State) ->
    State2 = State#state{cluster_state=NewCS},
    case maybe_save_state(State2) of
        ok ->
            State3 = state_changed(State2),
            {reply, ok, State3};
        {error,_} ->
            %% Failed to save, keep original state
            {reply, ok, State}
    end.

pending(EnsembleId, Pending) ->
    case orddict:find(EnsembleId, Pending) of
        {ok, Val} ->
            Val;
        error ->
            undefined
    end.

handle_info(tick, State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};

handle_info({'DOWN', _, _, Pid, _}, State=#state{remote_peers=Remote}) ->
    ThisNode = node(),
    case node(Pid) of
        ThisNode ->
            %% TODO: Remove peer reference here, no?
            {noreply, State};
        _ ->
            case lists:keytake(Pid, 2, Remote) of
                {value, {Peer, _Pid}, Remote2} ->
                    ets:delete(?ETS, {remote_pid, Peer}),
                    %% io:format("Untracking remote peer: ~p :: ~p~n", [Peer, Pid]),
                    {noreply, State#state{remote_peers=Remote2}};
                false ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec reload_state() -> state().
reload_state() ->
    case load_saved_state() of
        {ok, State} ->
            %% io:format("reloaded~n"),
            State;
        not_found ->
            initial_state()
    end.

-spec initial_state() -> state().
initial_state() ->
    ets:insert(?ETS, {enabled, false}),
    ClusterName = {node(), erlang:timestamp()},
    CS = riak_ensemble_state:new(ClusterName),
    State=#state{version=0,
                 ensemble_data=[],
                 remote_peers=[],
                 cluster_state=CS},
    State.

-spec activate(state()) -> {ok, state(), tuple()} | error.
activate(State=#state{cluster_state=CS}) ->
    case riak_ensemble_state:enabled(CS) of
        true ->
            error;
        false ->
            RootLeader = {root, node()},
            Members = [RootLeader],
            Root = #ensemble_info{leader=RootLeader, views=[Members],
                                  seq={0,0}, vsn={0,0}},
            RootInfo = {root, RootLeader, {Root#ensemble_info.vsn,
                                           Root#ensemble_info.views},
                        undefined},
            {ok, CS1} = riak_ensemble_state:enable(CS),
            {ok, CS2} = riak_ensemble_state:add_member({0,0}, node(), CS1),
            {ok, CS3} = riak_ensemble_state:set_ensemble(root, Root, CS2),
            State2 = State#state{cluster_state=CS3},
            {ok, State2, RootInfo}
    end.

-spec join_allowed(cluster_state(), cluster_state()) -> true               |
                                                        remote_not_enabled |
                                                        already_enabled.
join_allowed(LocalCS, RemoteCS) ->
    LocalEnabled = riak_ensemble_state:enabled(LocalCS),
    RemoteEnabled = riak_ensemble_state:enabled(RemoteCS),
    LocalId = riak_ensemble_state:id(LocalCS),
    RemoteId = riak_ensemble_state:id(RemoteCS),
    if not RemoteEnabled ->
            remote_not_enabled;
       LocalEnabled and (LocalId =/= RemoteId) ->
            already_enabled;
       true ->
            true
    end.

load_subscribers(State) ->
    case application:get_env(riak_ensemble, subscribers) of
        undefined->
            State#state{subscribers=[]};
        {ok, Subscribers} ->
            State#state{subscribers=Subscribers}
    end.

-spec load_saved_state() -> not_found | {ok, state()}.
load_saved_state() ->
    try
        {ok, CS} = riak_ensemble_storage:get(manager),
        true = riak_ensemble_state:is_state(CS),
        State = #state{ensemble_data=[],
                       remote_peers=[],
                       cluster_state=CS},
        {ok, State}
    catch
        _:_ ->
            not_found
    end.

maybe_save_state(State=#state{cluster_state=NewCS}) ->
    OldState = reload_state(),
    OldCS = OldState#state.cluster_state,
    if OldCS =:= NewCS ->
            ok;
       true ->
            save_state(State)
    end.

-spec save_state(state()) -> ok | {error, term()}.
save_state(#state{cluster_state=CS}) ->
    try
        %% Note: not syncing here is an intentional performance decision
        true = riak_ensemble_storage:put(manager, CS),
        ok
    catch
        _:Err ->
            error_logger:error_msg("Failed saving riak_ensemble_manager state~n"),
            {error, Err}
    end.

-spec schedule_tick() -> 'ok'.
schedule_tick() ->
    Time = 2000,
    _ = erlang:send_after(Time, self(), tick),
    ok.

-spec tick(state()) -> state().
tick(State) ->
    request_remote_peers(State),
    send_gossip(State),
    State.

-spec send_gossip(state()) -> ok.
send_gossip(#state{cluster_state=CS}) ->
    Members = riak_ensemble_state:members(CS) -- [node()],
    Shuffle = riak_ensemble_util:shuffle(Members),
    Nodes = lists:sublist(Shuffle, 10),
    _ = [gen_server:cast({?MODULE, Node}, {gossip, CS}) || Node <- Nodes],
    ok.

-spec merge_gossip(cluster_state(), state()) -> cluster_state().
merge_gossip(OtherCS, #state{cluster_state=CS}) ->
    case CS of
        undefined ->
            OtherCS;
        _ ->
            riak_ensemble_state:merge(CS, OtherCS)
    end.

-spec compute_all_members(Ensemble, Pending, Views) -> [peer_id()] when
      Ensemble :: ensemble_id(),
      Pending :: orddict(ensemble_id(), {vsn(), views()}),
      Views :: views().
compute_all_members(Ensemble, Pending, Views) ->
    case orddict:find(Ensemble, Pending) of
        {ok, {_, PendingViews}} ->
            compute_members(PendingViews ++ Views);
        error ->
            compute_members(Views)
    end.

-spec state_changed(state()) -> state().
state_changed(State=#state{ensemble_data=EnsData, cluster_state=CS, subscribers=Subscribers}) ->
    true = ets:insert(?ETS, {cluster_state, CS}),
    true = ets:insert(?ETS, {cluster, riak_ensemble_state:members(CS)}),
    true = ets:insert(?ETS, {enabled, riak_ensemble_state:enabled(CS)}),
    {NewEnsData, EnsChanges} = check_ensembles(EnsData, CS),
    _ = [case Change of
             {add, Obj} ->
                 true = ets:insert(?ETS, Obj);
             {del, Obj} ->
                 true = ets:delete(?ETS, Obj);
             {ins, Obj} ->
                 true = ets:insert(?ETS, Obj)
         end || Change <- EnsChanges],
    PeerChanges = check_peers(CS),
    _ = [case Change of
             {add, {Ensemble, Id}, Info} ->
                 #ensemble_info{mod=Mod, args=Args} = Info,
                 %% Maybe start new peer
                 case Mod:ready_to_start() of
                     true ->
                         riak_ensemble_peer_sup:start_peer(Mod, Ensemble,
                                                           Id, Args);
                     _ ->
                         ok
                 end;
             {del, {Ensemble, Id}} ->
                 %% Stop running peer
                 %% io:format("Should stop: ~p~n", [{Ensemble, Id}]),
                 ok = riak_ensemble_peer_sup:stop_peer(Ensemble, Id)
         end || Change <- PeerChanges],
    _ = [run_subscriber_callback(Callback, CS) || Callback <- Subscribers],
    State#state{ensemble_data=NewEnsData}.

run_subscriber_callback(Callback, CS) ->
    try Callback(CS)
    catch
        Exception ->
            lager:warning("Callback ~p crashed! Exception: ~p ClusterState: ~p",
                          [Callback, Exception, CS])
    end.

-spec request_remote_peers(state()) -> ok.
request_remote_peers(State=#state{remote_peers=Remote}) ->
    Root = case rleader() of
               undefined ->
                   [];
               Leader ->
                   [{root, Leader}]
           end,
    WantedRemote = Root ++ wanted_remote_peers(State),
    Need = ordsets:subtract(ordsets:from_list(WantedRemote),
                            orddict:fetch_keys(Remote)),
    _ = [request_peer_pid(Ensemble, Peer) || {Ensemble, Peer} <- Need],
    ok.

-spec wanted_remote_peers(state()) -> [{ensemble_id(), peer_id()}].
wanted_remote_peers(#state{cluster_state=CS}) ->
    Ensembles = riak_ensemble_state:ensembles(CS),
    Pending = riak_ensemble_state:pending(CS),
    ThisNode = node(),
    [{Ensemble, Peer} || {Ensemble, #ensemble_info{views=Views}} <- Ensembles,
                         AllPeers <- [compute_all_members(Ensemble, Pending, Views)],
                         lists:keymember(ThisNode, 2, AllPeers),
                         Peer={_, Node} <- AllPeers,
                         Node =/= ThisNode].

-spec request_peer_pid(ensemble_id(), peer_id()) -> ok.
request_peer_pid(Ensemble, PeerId={_, Node}) ->
    %% io:format("Requesting ~p/~p~n", [Ensemble, PeerId]),
    %% riak_ensemble_util:cast_unreliable({?MODULE, Node},
    %%                                    {request_peer_pid, self(), {Ensemble, PeerId}}).
    typed_cast(Node, {request_peer_pid, self(), {Ensemble, PeerId}}).

-spec compute_members(views()) -> [peer_id()].
compute_members([Members]) ->
    Members;
compute_members(Views) ->
    lists:usort(lists:append(Views)).

-spec check_ensembles(ensembles(), cluster_state()) -> {ensembles(), Changes} when
      Changes :: [{add | del | ins, ensemble_data()}].
check_ensembles(EnsData, CS) ->
    NewEnsData = ensemble_data(CS),
    Delta = riak_ensemble_util:orddict_delta(EnsData, NewEnsData),
    Changes =
        [case Change of
             {'$none', Obj} ->
                 {add, Obj};
             {Obj, '$none'} ->
                 {del, Obj};
             {_, Obj} ->
                 {ins, Obj}
         end || {_Ensemble, Change} <- Delta],
    {NewEnsData, Changes}.

-spec check_peers(cluster_state()) -> [Change] when
      Change :: {add, {ensemble_id(), peer_id()}, ensemble_info()}
              | {del, {ensemble_id(), peer_id()}}.
check_peers(CS) ->
    Peers = orddict_from_list(riak_ensemble_peer_sup:peers()),
    NewPeers = wanted_peers(CS),
    Delta = orddict_from_list(riak_ensemble_util:orddict_delta(Peers, NewPeers)),
    Changes =
        [case Change of
             {'$none', Info} ->
                 {add, PeerId, Info};
             {_Pid, '$none'} ->
                 {del, PeerId};
             _ ->
                 nothing
         end || {PeerId, Change} <- Delta],
    Changes2 = [Change || Change <- Changes,
                          Change =/= nothing],
    Changes2.

-spec ensemble_data(cluster_state()) -> ensembles().
ensemble_data(CS) ->
    Ensembles = riak_ensemble_state:ensembles(CS),
    Pending = riak_ensemble_state:pending(CS),
    Data = [{Ensemble, {Ensemble, Leader, {Vsn,Views}, pending(Ensemble, Pending)}}
            || {Ensemble, #ensemble_info{leader=Leader, views=Views, vsn=Vsn}} <- Ensembles],
    orddict_from_list(Data).

-spec wanted_peers(cluster_state()) -> orddict(Peer, Info) when
      Peer :: {ensemble_id(), peer_id()},
      Info :: ensemble_info().
wanted_peers(CS) ->
    Ensembles = riak_ensemble_state:ensembles(CS),
    Pending = riak_ensemble_state:pending(CS),
    ThisNode = node(),
    Wanted = [{{Ensemble, PeerId}, Info}
              || {Ensemble, Info=#ensemble_info{views=EViews}} <- Ensembles,
                 EPeers <- [compute_all_members(Ensemble, Pending, EViews)],
                 PeerId={_, Node} <- EPeers,
                 Node =:= ThisNode],
    orddict_from_list(Wanted).

%% Optimized version of orddict:from_list.
%% Much faster and generates much less garbage than orddict:from_list,
%% especially for large lists.
%%
%% Eg. from simple microbenchmark (time in us)
%% length   orddict:from_list   orddict_from_list
%%  100      139                 34
%%  1000     11973               463
%%  10000    1241925             6514
orddict_from_list(L) ->
    lists:ukeysort(1, L).
