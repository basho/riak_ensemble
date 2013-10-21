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
         get_peer_pid/2,
         get_members/1,
         get_leader/1,
         rleader_pid/0,
         rget/2,
         rmodify/3,
         update_root_ensemble/2,
         update_ensemble/2,
         check_ensemble/2,
         update_ensembles/2,
         join/2,
         create_ensemble/4,
         create_ensemble/5,
         check_quorum/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("riak_ensemble_types.hrl").

-record(state, {version,
                root :: [peer_id()],
                root_leader :: leader_id(),
                peers,
                remote_peers,
                ensembles :: [{ensemble_id(), ensemble_info()}]
               }).

-type state() :: #state{}.
-type obj()   :: any().

-type call_msg() :: {join, node()}.

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

-spec join(node(), node()) -> ok | error.
join(Node, OtherNode) ->
    case node() of
        Node ->
            join(OtherNode);
        _ ->
            typed_call(Node, {join, OtherNode}, infinity)
    end.

-spec update_root_ensemble(node(), ensemble_info()) -> ok.
update_root_ensemble(Node, RootInfo) ->
    typed_cast(Node, {update_root_ensemble, RootInfo}).

-spec update_ensembles(node(), [{ensemble_id(), ensemble_info()}]) -> ok.
update_ensembles(Node, Ensembles) ->
    typed_cast(Node, {update_ensembles, Ensembles}).

%%%===================================================================
%%% Root-based API
%%%===================================================================

-spec rleader() -> 'undefined' | peer_id().
rleader() ->
    get_leader(root).

-spec rleader_pid() -> pid() | undefined.
rleader_pid() ->
    get_peer_pid(root, rleader()).

-spec rget(any(), Default) -> {ok, obj()} | Default.
rget(Key, Default) ->
    Pid = rleader_pid(),
    case riak_ensemble_peer:kget(node(), Pid, Key, 5000) of
        {ok, Obj} ->
            riak_ensemble_peer:obj_value(Obj, Default, riak_ensemble_basic_backend);
        _ ->
            Default
    end.

-spec rmodify(_,_,_) -> std_reply().
rmodify(Key, F, Default) ->
    riak_ensemble_peer:kmodify(node(), root, Key, F, Default, 10000).

-spec root_set_ensemble(ensemble_id(), ensemble_info()) -> std_reply().
root_set_ensemble(EnsembleId, Info=#ensemble_info{leader=Leader, members=Members, seq=Seq}) ->
    rmodify(ensembles,
            fun(Ensembles) ->
                    orddict:update(EnsembleId,
                                   fun(CurInfo) ->
                                           CurInfo#ensemble_info{leader=Leader,
                                                                 members=Members,
                                                                 seq=Seq}
                                   end, Info, Ensembles)
            end,
            []).

-spec root_set_ensemble_once(ensemble_id(), ensemble_info()) -> std_reply().
root_set_ensemble_once(EnsembleId, Info) ->
    riak_ensemble_peer:kmodify(node(), root, ensembles,
                               fun(Ensembles) ->
                                       case orddict:is_key(EnsembleId, Ensembles) of
                                           true ->
                                               failed;
                                           false ->
                                               orddict:store(EnsembleId, Info, Ensembles)
                                       end
                               end, [], 10000).

-spec root_check_ensemble(ensemble_id(), ensemble_info()) -> std_reply().
root_check_ensemble(EnsembleId, #ensemble_info{members=Peers, seq=Seq}) ->
    rmodify(ensembles,
            fun(Ensembles) ->
                    io:format("########### ~p~n", [orddict:find(EnsembleId, Ensembles)]),
                    case orddict:find(EnsembleId, Ensembles) of
                        {ok, CurInfo=#ensemble_info{members=CurPeers, seq=CurSeq}}
                          when (CurPeers =/= Peers) and (CurSeq < Seq) ->
                            io:format("######## ~p/~p // ~p/~p~n", [CurSeq, CurPeers, Seq, Peers]),
                            NewInfo = CurInfo#ensemble_info{members=Peers, seq=Seq},
                            orddict:store(EnsembleId, NewInfo, Ensembles);
                        _ ->
                            failed
                    end
            end, []).

-spec update_ensemble(ensemble_id(), ensemble_info()) -> ok.
update_ensemble(EnsembleId, Info) ->
    F = fun() ->
                catch root_set_ensemble(EnsembleId, Info)
        end,
    spawn(F),
    ok.

-spec check_ensemble(ensemble_id(), ensemble_info()) -> ok.
check_ensemble(EnsembleId, Info) ->
    F = fun() ->
                catch root_check_ensemble(EnsembleId, Info)
        end,
    spawn(F),
    ok.

-spec join(node()) -> ok | error.
join(OtherNode) ->
    Reply = riak_ensemble_peer:kmodify(node(), root, members,
                                       fun(Members) ->
                                               ordsets:add_element(OtherNode, Members)
                                       end, [], 10000),
    case Reply of
        {ok, _Obj} ->
            ok;
        _ ->
            error
    end.

-spec create_ensemble(ensemble_id(), peer_id(), module(), [any()]) -> ok | error.
create_ensemble(EnsembleId, PeerId, Mod, Args) ->
    create_ensemble(EnsembleId, PeerId, [PeerId], Mod, Args).

-spec create_ensemble(ensemble_id(), leader_id(), [peer_id()], module(), [any()]) -> ok | error.
create_ensemble(EnsembleId, EnsLeader, Members, Mod, Args) ->
    Info = #ensemble_info{leader=EnsLeader, members=Members, seq={0,0}, mod=Mod, args=Args},
    io:format("Mod/Info: ~p/~p~n", [Mod, Info]),
    case root_set_ensemble_once(EnsembleId, Info) of
        {ok, _Obj} ->
            ok;
        _ ->
            error
    end.

%%%===================================================================
%%% ETS-based API
%%%===================================================================

-spec get_peer_pid(ensemble_id(), peer_id()) -> pid() | undefined.
get_peer_pid(Ensemble, PeerId) ->
    try
        ets:lookup_element(em, {pid, {Ensemble, PeerId}}, 2)
    catch
        _:_ ->
            undefined
    end.

-spec get_members(ensemble_id()) -> [peer_id()].
get_members(EnsembleId) ->
    try
        ets:lookup_element(em, EnsembleId, 3)
    catch _:_ ->
            []
    end.

-spec get_leader(ensemble_id()) -> leader_id().
get_leader(EnsembleId) ->
    try
        ets:lookup_element(em, EnsembleId, 2)
    catch _:_ ->
            undefined
    end.

-spec check_quorum(ensemble_id(), timeout()) -> boolean().
check_quorum(Ensemble, Timeout) ->
    case get_leader(Ensemble) of
        undefined ->
            false;
        Leader ->
            case get_peer_pid(Ensemble, Leader) of
                undefined ->
                    false;
                Pid ->
                    case riak_ensemble_peer:check_quorum(Pid, Timeout) of
                        ok ->
                            true;
                        _ ->
                            false
                    end
            end
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
    _ = ets:new(em, [named_table, public, {read_concurrency, true}, {write_concurrency, true}]),
    State = reload_state(),
    State2 = reload_peers(State),
    schedule_tick(),
    gen_server:cast(self(), init),
    {ok, State2}.

handle_call({join, OtherNode}, From, State) ->
    spawn_link(fun() ->
                       Reply = join(OtherNode),
                       gen_server:reply(From, Reply)
               end),
    {noreply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({peer_pid, Peer, Pid}, State=#state{remote_peers=Remote}) ->
    Remote2 = orddict:store(Peer, Pid, Remote),
    ets:insert(em, {{pid, Peer}, Pid}),
    erlang:monitor(process, Pid),
    io:format("Tracking remote peer: ~p :: ~p~n", [Peer, Pid]),
    {noreply, State#state{remote_peers=Remote2}};

handle_cast({request_peer_pid, From, PeerId}, State=#state{peers=Peers}) ->
    %% TODO: Confusing that we use {Ensemble, PeerId} as PeerId
    case orddict:find(PeerId, Peers) of
        error ->
            ok;
        {ok, Pid} ->
            typed_cast(From, {peer_pid, PeerId, Pid})
    end,
    {noreply, State};

handle_cast(init, State) ->
    State2 = state_changed(State),
    {noreply, State2};

handle_cast({update_root_ensemble, RootInfo}, State=#state{ensembles=Ensembles}) ->
    Seq = RootInfo#ensemble_info.seq,
    State2 = case lists:keyfind(root, 1, Ensembles) of
                 {_, CurInfo=#ensemble_info{seq=CurSeq}}
                   when (CurInfo =/= RootInfo) and (CurSeq =< Seq) ->
                     Ensembles2 = lists:keyreplace(root, 1, Ensembles, {root, RootInfo}),
                     do_update_ensembles(Ensembles2, State);
                 false ->
                     Ensembles2 = [{root, RootInfo}|Ensembles],
                     do_update_ensembles(Ensembles2, State);
                 _ ->
                     State
             end,
    {noreply, State2};

handle_cast({update_ensembles, Ensembles}, State) ->
    State2 = do_update_ensembles(Ensembles, State),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

do_update_ensembles(Ensembles, State) ->
    %% io:format("Updating: ~p~n", [Ensembles]),
    State2 = case lists:keyfind(root, 1, Ensembles) of
                 {_, #ensemble_info{leader=RootLeader, members=Root}} ->
                     State#state{root_leader=RootLeader,
                                 root=Root,
                                 ensembles=Ensembles};
                 _ ->
                     State#state{ensembles=Ensembles}
             end,
    State3 = state_changed(State2),
    Insert = [{Ensemble, Leader, Members}
              || {Ensemble, #ensemble_info{leader=Leader, members=Members}} <- Ensembles],
    ets:insert(em, Insert),
    ok = save_state(State3),
    State3.

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
                    ets:delete(em, {pid, Peer}),
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
            io:format("reloaded~n"),
            State;
        not_found ->
            initial_state()
    end.

-spec reload_peers(state()) -> state().
reload_peers(State) ->
    Peers = orddict:from_list(riak_ensemble_peer_sup:peers()),
    Insert = [{{pid, Id}, Pid} || {Id, Pid} <- Peers],
    ets:insert(em, Insert),
    State#state{peers=Peers}.

-spec initial_state() -> state().
initial_state() ->
    RootLeader = {root, node()},
    Members = [RootLeader],
    Root = #ensemble_info{leader=RootLeader, members=Members, seq={0,0}},
    ets:insert(em, {root, RootLeader, element(2, Root)}),
    State = #state{version=0,
                   root=Members,
                   root_leader=RootLeader,
                   remote_peers=[],
                   ensembles=[{root, Root}]},
    State.

-spec load_saved_state() -> not_found | {ok, state()}.
load_saved_state() ->
    {ok, Root} = application:get_env(riak_ensemble, data_root),
    File = filename:join([Root, "ensembles", "manager"]),
    case file:read_file(File) of
        {ok, <<CRC:32/integer, Binary/binary>>} ->
            case erlang:crc32(Binary) of
                CRC ->
                    try
                        State = binary_to_term(Binary),
                        #state{} = State,
                        {ok, State}
                    catch
                        _:_ ->
                            not_found
                    end;
                _ ->
                    not_found
            end;
        {error, _} ->
            not_found
    end.

-spec save_state(state()) -> ok | {error, term()}.
save_state(State) ->
    do_save_state(State#state{peers=[], remote_peers=[]}).

-spec do_save_state(state()) -> ok | {error, term()}.
do_save_state(State) ->
    {ok, Root} = application:get_env(riak_ensemble, data_root),
    File = filename:join([Root, "ensembles", "manager"]),
    Binary = term_to_binary(State),
    CRC = erlang:crc32(Binary),
    ok = filelib:ensure_dir(File),
    try
        ok = riak_ensemble_util:replace_file(File, [<<CRC:32/integer>>, Binary])
    catch
        _:Err ->
            error_logger:error_msg("Failed saving riak_ensemble_manager state to ~p: ~p~n",
                                   [File, Err]),
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
    State.

-spec state_changed(state()) -> state().
state_changed(State=#state{peers=Peers, ensembles=Ensembles}) ->
    ThisNode = node(),
    WantedPeers = [{{Ensemble, {Id, Node}}, Info}
                   || {Ensemble, Info=#ensemble_info{members=EPeers}} <- Ensembles,
                      {Id, Node} <- EPeers,
                      Node =:= ThisNode],
    %% TODO: Figure out why orddict_delta isn't giving us sorted results
    Delta = lists:sort(riak_ensemble_util:orddict_delta(lists:sort(Peers),
                                                        lists:sort(WantedPeers))),
    %% io:format("WP/D/P: ~p~n~p~n~p~n", [WantedPeers, Delta, Peers]),
    Peers2 =
        [case Change of
             {'$none', Info} ->
                 #ensemble_info{mod=Mod, args=Args, members=Bootstrap} = Info,
                 %% Start new peer
                 io:format("Should start: ~p~n", [{Ensemble, Id, Bootstrap, Mod, Args}]),
                 %% TODO: Make Bootstrap be views not membership
                 NewPid = riak_ensemble_peer_sup:start_peer(Mod, Ensemble, Id, [Bootstrap], Args),
                 {PeerId, NewPid};
             {undefined, '$none'} ->
                 {PeerId, undefined};
             {Pid, '$none'} ->
                 %% Stop running peer
                 io:format("Should stop: ~p~n", [{Ensemble, Id}]),
                 io:format("-- ~p~n", [orddict:fetch({Ensemble, Id}, Peers)]),
                 io:format("Stopping: ~p~n", [Pid]),
                 riak_ensemble_peer_sup:stop_peer(Ensemble, Id),
                 {PeerId, undefined};
             {Pid, _} ->
                 {PeerId, Pid}
         end || {PeerId={Ensemble, Id}, Change} <- Delta],
    Peers3 = [Peer || Peer={_PeerId, Pid} <- Peers2,
                      Pid =/= undefined],
    State2 = State#state{peers=Peers3},
    request_remote_peers(State2),
    State2.

-spec request_remote_peers(state()) -> ok.
request_remote_peers(State=#state{remote_peers=Remote}) ->
    ThisNode = node(),
    Root = case rleader() of
               undefined ->
                   [];
               Leader ->
                   [{root, [Leader]}]
           end,
    LocalEns = Root ++ local_ensembles(State),
    WantedRemote = [{Ensemble, Peer} || {Ensemble, Peers} <- LocalEns,
                                        Peer={_, Node} <- Peers,
                                        Node =/= ThisNode],
    Need = ordsets:subtract(ordsets:from_list(WantedRemote),
                            orddict:fetch_keys(Remote)),
    _ = [request_peer_pid2(Ensemble, Peer) || {Ensemble, Peer} <- Need],
    ok.

-spec request_peer_pid2(ensemble_id(), peer_id()) -> ok.
request_peer_pid2(Ensemble, PeerId={_, Node}) ->
    io:format("Requesting ~p/~p~n", [Ensemble, PeerId]),
    typed_cast(Node, {request_peer_pid, self(), {Ensemble, PeerId}}).

-spec local_ensembles(state()) -> [{ensemble_id(), [peer_id()]}].
local_ensembles(#state{ensembles=Ensembles}) ->
    ThisNode = node(),
    [{Ensemble, EPeers} || {Ensemble, #ensemble_info{members=EPeers}} <- Ensembles,
                           lists:any(fun({_, Node}) -> Node =:= ThisNode end, EPeers)].
