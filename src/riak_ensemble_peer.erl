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

-module(riak_ensemble_peer).
-behaviour(gen_fsm).

-include_lib("riak_ensemble_types.hrl").

%% API
-export([start_link/4, start/4]).
-export([join/2, join/3, update_members/3, get_leader/1, backend_pong/1]).
-export([kget/4, kget/5, kupdate/6, kput_once/5, kover/5, kmodify/6, kdelete/4,
         ksafe_delete/5, obj_value/2, obj_value/3]).
-export([setup/2]).
-export([probe/2, election/2, prepare/2, leading/2, following/2,
         probe/3, election/3, prepare/3, leading/3, following/3]).
-export([pending/2, prelead/2, prefollow/2,
         pending/3, prelead/3, prefollow/3]).
-export([repair/2, exchange/2,
         repair/3, exchange/3]).
-export([valid_obj_hash/2]).

%% Support/debug API
-export([count_quorum/2, ping_quorum/2, check_quorum/2, force_state/2,
         get_info/1, stable_views/2, tree_info/1,
         watch_leader_status/1, stop_watching/1]).

%% Backdoors for unit testing
-ifdef(TEST).
-export([debug_local_get/2]).
-export([get_watchers/1]).
-endif.

%% Exported internal callback functions
-export([do_kupdate/4, do_kput_once/4, do_kmodify/4]).

-compile({pulse_replace_module,
          [{gen_fsm, pulse_gen_fsm}]}).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

%% -define(OUT(Fmt,Args), io:format(Fmt,Args)).
-define(OUT(Fmt,Args), ok).

-define(REQUEST_TIMEOUT, 30000).

-define(ALIVE,   riak_ensemble_config:alive_ticks()).
-define(WORKERS, riak_ensemble_config:peer_workers()).

-define(FOLLOWER_TIMEOUT,  riak_ensemble_config:follower_timeout()).
-define(PENDING_TIMEOUT,   riak_ensemble_config:pending_timeout()).
-define(ELECTION_TIMEOUT,  riak_ensemble_config:election_timeout()).
-define(PREFOLLOW_TIMEOUT, riak_ensemble_config:prefollow_timeout()).
-define(PROBE_DELAY,       riak_ensemble_config:probe_delay()).
-define(LOCAL_GET_TIMEOUT, riak_ensemble_config:local_get_timeout()).
-define(LOCAL_PUT_TIMEOUT, riak_ensemble_config:local_put_timeout()).

%% Supported object hashes used in synctree metadata
-define(H_OBJ_NONE, 0).

%%%===================================================================

-record(fact, {epoch    :: epoch(),
               seq      :: seq(),
               leader   :: peer_id(),

               %% The epoch/seq which committed current view
               view_vsn :: {epoch(), seq()},

               %% The epoch/seq which committed current pending view
               pend_vsn :: {epoch(), seq()},

               %% The epoch/seq of last commited view change. In other words,
               %% the pend_vsn for the last pending view that has since been
               %% transitioned to (ie. no longer pending)
               commit_vsn :: {epoch(), seq()},

               pending  :: {vsn(), views()},
               views    :: [[peer_id()]]
              }).

-type fact() :: #fact{}.

-type next_state()      :: {next_state, atom(), state()} |
                           {stop,normal,state()}.

-type sync_next_state() :: {reply, term(), atom(), state()} |
                           {next_state, atom(), state()} |
                           {stop, normal, state()}.

-type fsm_from()        :: {_,_}.

-type timer() :: term().
-type key()   :: any().
-type obj()   :: any().
-type maybe_obj() :: obj() | notfound | timeout. %% TODO: Pretty sure this can also be failed

-type target() :: pid() | ensemble_id().
-type maybe_peer_id() :: undefined | peer_id().
-type modify_fun() :: fun() | {module(), atom(), term()}.

-record(state, {id            :: peer_id(),
                ensemble      :: ensemble_id(),
                ets           :: ets:tid(),
                fact          :: fact(),
                awaiting      :: riak_ensemble_msg:msg_state(),
                preliminary   :: {peer_id(), epoch()},
                abandoned     :: {epoch(), seq()},
                timer         :: timer(),
                ready = false :: boolean(),
                members       :: [peer_id()],
                peers         :: [{peer_id(), pid()}],
                mod           :: module(),
                modstate      :: any(),
                workers       :: tuple(),
                tree_trust    :: boolean(),
                tree_ready    :: boolean(),
                alive         :: integer(),
                last_views    :: [[peer_id()]],
                async         :: pid(),
                tree          :: pid(),
                lease         :: riak_ensemble_lease:lease_ref(),
                watchers = [] :: [{pid(), reference()}],
                self          :: pid()
               }).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(module(), ensemble_id(), peer_id(), [any()])
                -> ignore | {error, _} | {ok, pid()}.
start_link(Mod, Ensemble, Id, Args) ->
    gen_fsm:start_link(?MODULE, [Mod, Ensemble, Id, Args], []).

-spec start(module(), ensemble_id(), peer_id(), [any()])
           -> ignore | {error, _} | {ok, pid()}.
start(Mod, Ensemble, Id, Args) ->
    gen_fsm:start(?MODULE, [Mod, Ensemble, Id, Args], []).

%% TODO: Do we want this to be routable by ensemble/id instead?
-spec join(pid(), peer_id()) -> ok | timeout | {error, [{already_member, peer_id()}]}.
join(Pid, Id) ->
    join(Pid, Id, 1000).

-spec join(pid(), peer_id(), timeout())
          -> ok | timeout | {error, [{already_member, peer_id()}]}.
join(Pid, Id, Timeout) when is_pid(Pid) ->
    update_members(Pid, [{add, Id}], Timeout).

-spec update_members(pid(), [peer_change()], timeout())
                    -> ok | timeout | {error, [{change_error(), peer_id()}]}.
update_members(Pid, Changes, Timeout) when is_pid(Pid) ->
    riak_ensemble_router:sync_send_event(node(), Pid, {update_members, Changes}, Timeout).

-spec check_quorum(ensemble_id(), timeout()) -> ok | timeout.
check_quorum(Ensemble, Timeout) ->
    riak_ensemble_router:sync_send_event(node(), Ensemble, check_quorum, Timeout).

-spec count_quorum(ensemble_id(), timeout()) -> integer() | timeout.
count_quorum(Ensemble, Timeout) ->
    case ping_quorum(Ensemble, Timeout) of
        timeout ->
            timeout;
        {_Leader, _, Replies} ->
            length(Replies)
    end.

-spec ping_quorum(ensemble_id(), timeout()) -> {leader_id(), boolean(), [peer_id()]} | timeout.
ping_quorum(Ensemble, Timeout) ->
    Result = riak_ensemble_router:sync_send_event(node(), Ensemble,
                                                  ping_quorum, Timeout),
    case Result of
        timeout ->
            timeout;
        {Leader, Ready, Replies} ->
            Quorum = [Peer || {Peer, ok} <- Replies],
            {Leader, Ready, Quorum}
    end.

-spec stable_views(ensemble_id(), timeout()) -> {ok, boolean()} | timeout.
stable_views(Ensemble, Timeout) ->
    riak_ensemble_router:sync_send_event(node(), Ensemble, stable_views, Timeout).

-spec get_leader(pid()) -> peer_id().
get_leader(Pid) when is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_leader, infinity).

-spec watch_leader_status(pid()) -> ok.
watch_leader_status(Pid) when is_pid(Pid) ->
    gen_fsm:send_all_state_event(Pid, {watch_leader_status, self()}).

-spec stop_watching(pid()) -> ok.
stop_watching(Pid) when is_pid(Pid) ->
    gen_fsm:send_all_state_event(Pid, {stop_watching, self()}).

-ifdef(TEST).
-spec get_watchers(pid()) -> [{pid(), reference()}].
get_watchers(Pid) when is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_watchers).
-endif.

get_info(Pid) when is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_info, infinity).

tree_info(Pid) when is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, tree_info, infinity).

backend_pong(Pid) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, backend_pong).

force_state(Pid, EpochSeq) ->
    gen_fsm:sync_send_event(Pid, {force_state, EpochSeq}).

%%%===================================================================
%%% K/V API
%%%===================================================================

-spec kget(node(), target(), key(), timeout()) -> std_reply().
kget(Node, Target, Key, Timeout) ->
    kget(Node, Target, Key, Timeout, []).

-spec kget(node(), target(), key(), timeout(), _) -> std_reply().
kget(Node, Target, Key, Timeout, Opts) ->
    Result = riak_ensemble_router:sync_send_event(Node, Target, {get, Key, Opts}, Timeout),
    ?OUT("get(~p): ~p~n", [Key, Result]),
    Result.

-spec kupdate(node(), target(), key(), obj(), term(), timeout()) -> std_reply().
kupdate(Node, Target, Key, Current, New, Timeout) ->
    F = fun ?MODULE:do_kupdate/4,
    Result = riak_ensemble_router:sync_send_event(Node, Target, {put, Key, F, [Current, New]}, Timeout),
    ?OUT("update(~p): ~p~n", [Key, Result]),
    Result.

do_kupdate(Obj, _NextSeq, State, [Current, New]) ->
    Expected = {get_obj(epoch, Current, State), get_obj(seq, Current, State)},
    Epoch = get_obj(epoch, Obj, State),
    Seq = get_obj(seq, Obj, State),
    case {Epoch, Seq} of
        Expected ->
            {ok, set_obj(value, New, Obj, State)};
        _ ->
            %% io:format("Failed: ~p~nA: ~p~nB: ~p~n",
            %%           [Obj, Expected, {Epoch,Seq}]),
            failed
    end.

-spec kput_once(node(), target(), key(), obj(), timeout()) -> std_reply().
kput_once(Node, Target, Key, New, Timeout) ->
    F = fun ?MODULE:do_kput_once/4,
    Result = riak_ensemble_router:sync_send_event(Node, Target, {put, Key, F, [New]}, Timeout),
    ?OUT("put_once(~p): ~p~n", [Key, Result]),
    Result.

do_kput_once(Obj, _NextSeq, State, [New]) ->
    case get_obj(value, Obj, State) of
        notfound ->
            {ok, set_obj(value, New, Obj, State)};
        _ ->
            failed
    end.

-spec kover(node(), target(), key(), obj(), timeout()) -> std_reply().
kover(Node, Target, Key, New, Timeout) ->
    Result = riak_ensemble_router:sync_send_event(Node, Target,
                                                  {overwrite, Key, New}, Timeout),
    ?OUT("kover(~p): ~p~n", [Key, Result]),
    Result.

-spec kmodify(node(), target(), key(), modify_fun(), term(), timeout()) -> std_reply().
kmodify(Node, Target, Key, ModFun, Default, Timeout) ->
    F = fun ?MODULE:do_kmodify/4,
    Result = riak_ensemble_router:sync_send_event(Node, Target, {put, Key, F, [ModFun, Default]}, Timeout),
    ?OUT("kmodify(~p): ~p~n", [Key, Result]),
    Result.

do_kmodify(Obj, NextSeq, State, [ModFun, Default]) ->
    Value = get_value(Obj, Default, State),
    Vsn = {epoch(State), NextSeq},
    New = case ModFun of
              {Mod, Fun, Args} ->
                  Mod:Fun(Vsn, Value, Args);
              _ ->
                  ModFun(Vsn, Value)
          end,
    case New of
        failed ->
            failed;
        _ ->
            {ok, set_obj(value, New, Obj, State)}
    end.

-spec kdelete(node(), target(), key(), timeout()) -> std_reply().
kdelete(Node, Target, Key, Timeout) ->
    Result = riak_ensemble_router:sync_send_event(Node, Target, {overwrite, Key, notfound}, Timeout),
    ?OUT("kdelete(~p): ~p~n", [Key, Result]),
    Result.

-spec ksafe_delete(node(), target(), key(), obj(), timeout()) -> std_reply().
ksafe_delete(Node, Target, Key, Current, Timeout) ->
    kupdate(Node, Target, Key, Current, notfound, Timeout).

-spec obj_value(obj(), atom() | state()) -> any().
obj_value(Obj, Mod) ->
    get_obj(value, Obj, Mod).
-spec obj_value(obj(), term(), atom() | state()) -> any().
obj_value(Obj, Default, Mod) ->
    case obj_value(Obj, Mod) of
        notfound ->
            Default;
        Val ->
            Val
    end.

-spec local_get(pid(), term(), timeout()) -> fixme().
local_get(Pid, Key, Timeout) when is_pid(Pid) ->
    riak_ensemble_router:sync_send_event(Pid, {local_get, Key}, Timeout).

-spec local_put(pid(), term(), term(), timeout()) -> fixme().
local_put(Pid, Key, Obj, Timeout) when is_pid(Pid) ->
    riak_ensemble_router:sync_send_event(Pid, {local_put, Key, Obj}, Timeout).

-ifdef(TEST).
%% Acts like local_get, but can be used for any peer, not just the leader.
%% Should only be used for testing purposes, since values obtained via
%% this function provide no consistency guarantees whatsoever.
-spec debug_local_get(pid(), term()) -> std_reply().
debug_local_get(Pid, Key) ->
    gen_fsm:sync_send_all_state_event(Pid, {debug_local_get, Key}).
-endif.

%%%===================================================================
%%% Core Protocol
%%%===================================================================

-spec probe(_, state()) -> next_state().
probe(init, State) ->
    lager:debug("~p: probe init", [State#state.id]),
    State2 = set_leader(undefined, State),
    case is_pending(State2) of
        true ->
            pending(init, State2);
        false ->
            State3 = send_all(probe, State2),
            {next_state, probe, State3}
    end;
probe({quorum_met, Replies}, State=#state{fact=Fact, abandoned=Abandoned}) ->
    Latest = latest_fact(Replies, Fact),
    Existing = existing_leader(Replies, Abandoned, Latest),
    State2 = State#state{fact=Latest,
                         members=compute_members(Latest#fact.views)},
    %% io:format("Latest: ~p~n", [Latest]),
    maybe_follow(Existing, State2);
probe({timeout, Replies}, State=#state{fact=Fact}) ->
    Latest = latest_fact(Replies, Fact),
    State2 = State#state{fact=Latest},
    State3 = check_views(State2),
    probe(delay, State3);
probe(delay, State) ->
    State2 = set_timer(?PROBE_DELAY, probe_continue, State),
    {next_state, probe, State2};
probe(probe_continue, State) ->
    probe(init, State);
probe(Msg, State) ->
    common(Msg, State, probe).

-spec probe(_, fsm_from(), state()) -> {next_state, probe, state()}.
probe(Msg, From, State) ->
    common(Msg, From, State, probe).

pending(init, State) ->
    lager:debug("~p: pending init", [State#state.id]),
    State2 = set_timer(?PENDING_TIMEOUT, pending_timeout, State),
    {next_state, pending, State2#state{tree_trust=false}};
pending(pending_timeout, State) ->
    lager:debug("~p: pending_timeout", [State#state.id]),
    probe({timeout, []}, State);
pending({prepare, Id, NextEpoch, From}, State=#state{fact=Fact}) ->
    Epoch = epoch(State),
    case NextEpoch > Epoch of
        true ->
            lager:debug("~p: accepting ~p from ~p (~p)", [Id, NextEpoch, Id, Epoch]),
            reply(From, Fact, State),
            State2 = cancel_timer(State),
            prefollow({init, Id, NextEpoch}, State2);
        false ->
            lager:debug("~p: rejecting ~p from ~p (~p)", [Id, NextEpoch, Id, Epoch]),
            {next_state, pending, State}
    end;
pending({commit, NewFact, From}, State) ->
    Epoch = epoch(State),
    case NewFact#fact.epoch >= Epoch of
        true ->
            reply(From, ok, State),
            lager:debug("~p: accepting commit from ~p for epoch ~p",
                        [State#state.id, From, NewFact#fact.epoch]),
            State2 = local_commit(NewFact, State),
            State3 = cancel_timer(State2),
            following(init, State3);
        false ->
            lager:debug("~p: ignoring outdated commit from ~p (~p < ~p)",
                        [State#state.id, From, NewFact#fact.epoch, Epoch]),
            {next_state, pending, State}
    end;
pending(Msg, State) ->
    common(Msg, State, pending).

pending(Msg, From, State) ->
    common(Msg, From, State, pending).

maybe_follow(_, State=#state{tree_trust=false}) ->
    %% This peer is untrusted and must perform an exchange
    exchange(init, State);
maybe_follow(undefined, State) ->
    election(init, set_leader(undefined, State));
maybe_follow(Leader, State=#state{id=Leader}) ->
    election(init, set_leader(undefined, State));
maybe_follow(Leader, State) ->
    %% TODO: Should we use prefollow instead of following(not_ready)?
    following(not_ready, set_leader(Leader, State)).

%%%===================================================================
%%% tree verification/exchange
%%%===================================================================

repair(init, State=#state{tree=Tree}) ->
    lager:debug("~p: repair", [State#state.id]),
    riak_ensemble_peer_tree:async_repair(Tree),
    {next_state, repair, State#state{tree_trust=false}};
repair(repair_complete, State) ->
    exchange(init, State);
repair(Msg, State) ->
    common(Msg, State, repair).

-spec repair(_, fsm_from(), state()) -> {next_state, repair, state()}.
repair(Msg, From, State) ->
    common(Msg, From, State, repair).

%%%===================================================================

exchange(init, State) ->
    start_exchange(State),
    {next_state, exchange, State};
exchange(exchange_complete, State) ->
    election(init, State#state{tree_trust=true});
exchange(exchange_failed, State) ->
    %% Asynchronous exchange failed
    probe(delay, State);
exchange(Msg, State) ->
    common(Msg, State, exchange).

exchange(tree_corrupted, From, State) ->
    gen_fsm:reply(From, ok),
    repair(init, State);
exchange(Msg, From, State) ->
    common(Msg, From, State, exchange).

%%%===================================================================

start_exchange(State=#state{id=Id, ensemble=Ensemble, tree=Tree, members=Members,
                            tree_trust=Trusted}) ->
    Peers = get_peers(Members, State),
    Views = views(State),
    riak_ensemble_exchange:start_exchange(Ensemble, self(), Id, Tree, Peers, Views, Trusted),
    ok.

%%%===================================================================

-spec election(_, state()) -> next_state().
election(init, State) ->
    %% io:format("~p/~p: starting election~n", [self(), State#state.id]),
    ?OUT("~p: starting election~n", [State#state.id]),
    State2 = set_timer(?ELECTION_TIMEOUT, election_timeout, State),
    {next_state, election, State2};
election(election_timeout, State) ->
    case mod_ping(State) of
        {ok, State2} ->
            prepare(init, State2#state{timer=undefined});
        {failed, State2} ->
            election(init, State2)
    end;
election({prepare, Id, NextEpoch, From}, State=#state{fact=Fact}) ->
    Epoch = epoch(State),
    case NextEpoch > Epoch of
        true ->
            ?OUT("~p: accepting ~p from ~p (~p)~n",
                 [State#state.id, NextEpoch, Id, Epoch]),
            reply(From, Fact, State),
            State2 = cancel_timer(State),
            prefollow({init, Id, NextEpoch}, State2);
        false ->
            ?OUT("~p: rejecting ~p from ~p (~p)~n",
                 [State#state.id, NextEpoch, Id, Epoch]),
            {next_state, election, State}
    end;
election({commit, NewFact, From}, State) ->
    %% io:format("##### ~p: commit :: ~p vs ~p~n",
    %%           [State#state.id, NewFact#fact.epoch, epoch(State)]),
    Epoch = epoch(State),
    case NewFact#fact.epoch >= Epoch of
        true ->
            reply(From, ok, State),
            State2 = local_commit(NewFact, State),
            State3 = cancel_timer(State2),
            following(init, State3);
        false ->
            {next_state, election, State}
    end;
election(Msg, State) ->
    common(Msg, State, election).

-spec election(_, fsm_from(), state()) -> {next_state, election, state()}.
election(Msg, From, State) ->
    common(Msg, From, State, election).

prefollow({init, Id, NextEpoch}, State) ->
    Prelim = {Id, NextEpoch},
    State2 = State#state{preliminary=Prelim},
    State3 = set_timer(?PREFOLLOW_TIMEOUT, prefollow_timeout, State2),
    {next_state, prefollow, State3};
%% prefollow({commit, Fact, From}, State=#state{preliminary=Prelim}) ->
%%     %% TODO: Shouldn't we check that this is from preliminary leader?
%%     {_PreLeader, PreEpoch} = Prelim,
%%     case Fact#fact.epoch >= PreEpoch of
%%         true ->
%%             State2 = cancel_timer(State),
%%             State3 = local_commit(Fact, State2),
%%             reply(From, ok, State),
%%             following(init, State3);
%%         false ->
%%             {next_state, prefollow, State}
%%     end;
prefollow({new_epoch, Id, NextEpoch, From}, State=#state{preliminary=Prelim}) ->
    case {Id, NextEpoch} == Prelim of
        true ->
            State2 = set_leader(Id, set_epoch(NextEpoch, State)),
            State3 = cancel_timer(State2),
            reply(From, ok, State),
            following(not_ready, State3);
        false ->
            %% {next_state, prefollow, State}
            State2 = cancel_timer(State),
            probe(init, State2)
    end;
prefollow(prefollow_timeout, State) ->
    %% TODO: Should this be election instead?
    probe(init, State);
%% TODO: Should we handle prepare messages?
prefollow(Msg, State) ->
    common(Msg, State, prefollow).

prefollow(Msg, From, State) ->
    common(Msg, From, State, prefollow).

-spec prepare(_, state()) -> next_state().
prepare(init, State=#state{id=Id}) ->
    %% TODO: Change this hack where we keep old state and reincrement
    lager:debug("~p: prepare", [State#state.id]),
    {NextEpoch, _} = increment_epoch(State),
    %% io:format("Preparing ~p to ~p :: ~p~n", [NextEpoch,
    %%                                          views(State),
    %%                                          get_peers(State#state.members, State)]),
    State2 = send_all({prepare, Id, NextEpoch}, State),
    {next_state, prepare, State2};
prepare({quorum_met, Replies}, State=#state{id=Id, fact=Fact}) ->
    %% TODO: Change this hack where we keep old state and reincrement
    Latest = latest_fact(Replies, Fact),
    {NextEpoch, _} = increment_epoch(State),
    State3 = State#state{fact=Latest,
                         preliminary={Id, NextEpoch},
                         members=compute_members(Latest#fact.views)},
    prelead(init, State3);
prepare({timeout, _Replies}, State) ->
    %% TODO: Change this hack where we keep old state and reincrement
    %% io:format("PREPARE FAILED: ~p~n", [_Replies]),
    %% {_, State2} = increment_epoch(State),
    probe(init, State);
prepare(Msg, State) ->
    common(Msg, State, prepare).

-spec prepare(_, fsm_from(), state()) -> {next_state, prepare, state()}.
prepare(Msg, From, State) ->
    common(Msg, From, State, prepare).

prelead(init, State=#state{id=Id, preliminary=Prelim}) ->
    {Id, NextEpoch} = Prelim,
    State2 = send_all({new_epoch, Id, NextEpoch}, State),
    {next_state, prelead, State2};
prelead({quorum_met, _Replies}, State=#state{id=Id, preliminary=Prelim, fact=Fact}) ->
    {Id, NextEpoch} = Prelim,
    NewFact = Fact#fact{leader=Id,
                        epoch=NextEpoch,
                        seq=0,
                        view_vsn={NextEpoch, -1}},
    State2 = State#state{fact=NewFact},
    leading(init, State2);
prelead({timeout, _Replies}, State) ->
    probe(init, State);
prelead(Msg, State) ->
    common(Msg, State, prelead).

prelead(Msg, From, State) ->
    common(Msg, From, State, prelead).

-spec leading(_, state()) -> next_state().
leading(init, State=#state{id=_Id, watchers=Watchers}) ->
    _ = lager:info("~p: Leading~n", [_Id]),
    start_exchange(State),
    _ = notify_leader_status(Watchers, leading, State),
    leading(tick, State#state{alive=?ALIVE, tree_ready=false});
leading(tick, State) ->
    leader_tick(State);
leading(exchange_complete, State) ->
    %% io:format(user, "~p: ~p leader trusted!~n", [os:timestamp(), State#state.id]),
    State2 = State#state{tree_trust=true, tree_ready=true},
    {next_state, leading, State2};
leading(exchange_failed, State) ->
    step_down(State);
leading({forward, From, Msg}, State) ->
    case leading(Msg, From, State) of
        %% {reply, Reply, StateName, State2} ->
        %%     send_reply(From, Reply),
        %%     {next_state, StateName, State2};
        {next_state, StateName, State2} ->
            {next_state, StateName, State2}
    end;
leading(Msg, State) ->
    common(Msg, State, leading).

-spec leading(_, fsm_from(), state()) -> sync_next_state().
leading({update_members, Changes}, From, State=#state{fact=Fact,
                                                      members=Members}) ->
    Cluster = riak_ensemble_manager:cluster(),
    Views = Fact#fact.views,
    case update_view(Changes, Members, hd(Views), Cluster) of
        {[], NewView} ->
            Views2 = [NewView|Views],
            NewFact = change_pending(Views2, State),
            case try_commit(NewFact, State) of
                {ok, State2} ->
                    {reply, ok, leading, State2};
                {failed, State2} ->
                    send_reply(From, timeout),
                    step_down(State2)
            end;
        {Errors, _NewView} ->
            {reply, {error, Errors}, leading, State}
    end;
leading(check_quorum, From, State) ->
    case try_commit(State#state.fact, State) of
        {ok, State2} ->
            {reply, ok, leading, State2};
        {failed, State2} ->
            send_reply(From, timeout),
            step_down(State2)
    end;
leading(ping_quorum, From, State=#state{fact=Fact, id=Id, members=Members,
                                        tree_ready=TreeReady}) ->
    NewFact = increment_sequence(Fact),
    State2 = local_commit(NewFact, State),
    {Future, State3} = blocking_send_all({commit, NewFact}, State2),
    Extra = case lists:member(Id, Members) of
                true  -> [{Id,ok}];
                false -> []
            end,
    spawn_link(fun() ->
                       %% TODO: Should this be hardcoded?
                       timer:sleep(1000),
                       Result = case wait_for_quorum(Future) of
                                    {quorum_met, Replies} ->
                                        %% io:format("met: ~p~n", [Replies]),
                                        Extra ++ Replies;
                                    {timeout, _Replies} ->
                                        %% io:format("timeout~n"),
                                        Extra
                                end,
                       gen_fsm:reply(From, {Id, TreeReady, Result})
               end),
    {next_state, leading, State3};
leading(stable_views, _From, State=#state{fact=Fact}) ->
    #fact{pending=Pending, views=Views} = Fact,
    Reply = case {Pending, Views} of
                {undefined, [_]} ->
                    {ok, true};
                {{_, []}, [_]} ->
                    {ok, true};
                _ ->
                    {ok, false}
            end,
    {reply, Reply, leading, State};
leading(Msg, From, State) ->
    case leading_kv(Msg, From, State) of
        false ->
            common(Msg, From, State, leading);
        Return ->
            Return
    end.

-spec change_pending(views(), state()) -> fact().
change_pending(Views, #state{fact=Fact}) ->
    Vsn = {Fact#fact.epoch, Fact#fact.seq},
    Fact#fact{pending={Vsn, Views}}.

update_view(Changes, Members, View, Cluster) ->
    update_view(Changes, [], Members, View, Cluster).

update_view([], Errors, _Members, View, _Cluster) ->
    {lists:reverse(Errors), lists:usort(View)};
update_view([{add, Id}|Rest], Errors, Members, View, Cluster) ->
    InCluster = in_cluster(Id, Cluster),
    IsMember = lists:member(Id, Members),
    if not InCluster ->
            update_view(Rest, [{not_in_cluster, Id}|Errors], Members, View, Cluster);
       IsMember ->
            update_view(Rest, [{already_member, Id}|Errors], Members, View, Cluster);
       true ->
            update_view(Rest, Errors, [Id|Members], [Id|View], Cluster)
    end;
update_view([{del, Id}|Rest], Errors, Members, View, Cluster) ->
    case lists:member(Id, Members) of
        false ->
            update_view(Rest, [{not_member, Id}|Errors], Members, View, Cluster);
        true ->
            update_view(Rest, Errors, Members -- [Id], View -- [Id], Cluster)
    end.

-spec should_transition(state()) -> boolean().
should_transition(State=#state{last_views=LastViews}) ->
    Views = views(State),
    (Views =:= LastViews) and (tl(views(State)) =/= []).

-spec transition(state()) -> {ok, state()}       |
                             {shutdown, state()} |
                             {failed, state()}.
transition(State=#state{id=Id, fact=Fact}) ->
    Latest = hd(Fact#fact.views),
    ViewVsn = {Fact#fact.epoch, Fact#fact.seq},
    PendVsn = Fact#fact.pend_vsn,
    NewFact = Fact#fact{views=[Latest], view_vsn=ViewVsn, commit_vsn=PendVsn},
    case try_commit(NewFact, State) of
        {ok, State3} ->
            case lists:member(Id, Latest) of
                false ->
                    {shutdown, State3};
                true ->
                    {ok, State3}
            end;
        {failed, _}=Failed ->
            Failed
    end.

-spec try_commit(fact(), state()) -> {failed, state()} | {ok, state()}.
try_commit(NewFact0, State) ->
    Views = views(State),
    NewFact = increment_sequence(NewFact0),
    State2 = local_commit(NewFact, State),
    {Future, State3} = blocking_send_all({commit, NewFact}, State2),
    case wait_for_quorum(Future) of
        {quorum_met, _Replies} ->
            State4 = State3#state{last_views=Views},
            {ok, State4};
        {timeout, _Replies} ->
            {failed, set_leader(undefined, State3)}
    end.

-spec reset_follower_timer(state()) -> state().
reset_follower_timer(State) ->
    set_timer(?FOLLOWER_TIMEOUT, follower_timeout, State).

-spec following(_, state()) -> next_state().
following(not_ready, State) ->
    following(init, State#state{ready=false});
following(init, State) ->
    lager:debug("~p: Following: ~p", [State#state.id, leader(State)]),
    start_exchange(State),
    State2 = reset_follower_timer(State),
    {next_state, following, State2};
following(exchange_complete, State) ->
    %% io:format(user, "~p: ~p follower trusted!~n", [os:timestamp(), State#state.id]),
    State2 = State#state{tree_trust=true},
    {next_state, following, State2};
following(exchange_failed, State) ->
    lager:debug("~p: exchange failed", [State#state.id]),
    probe(init, State);
following({commit, Fact, From}, State) ->
    State3 = case Fact#fact.epoch >= epoch(State) of
                 true ->
                     State2 = local_commit(Fact, State),
                     reply(From, ok, State),
                     reset_follower_timer(State2);
                 false ->
                     State
             end,
    {next_state, following, State3};
%% following({prepare, Id, NextEpoch, From}=Msg, State=#state{fact=Fact}) ->
%%     Epoch = epoch(State),
%%     case (Id =:= leader(State)) and (NextEpoch > Epoch) of
%%         true ->
%%             ?OUT("~p: reaccepting ~p from ~p (~p)~n",
%%                  [State#state.id, NextEpoch, Id, Epoch]),
%%             reply(From, Fact, State),
%%             State2 = set_epoch(NextEpoch, State),
%%             State3 = reset_follower_timer(State2),
%%             {next_state, following, State3};
%%         false ->
%%             ?OUT("~p: following/ignoring: ~p~n", [State#state.id, Msg]),
%%             nack(Msg, State),
%%             {next_state, following, State}
%%     end;
following(follower_timeout, State) ->
    lager:debug("~p: follower_timeout from ~p", [State#state.id, leader(State)]),
    abandon(State#state{timer=undefined});
following({check_epoch, Leader, Epoch, From}, State) ->
    case check_epoch(Leader, Epoch, State) of
        true ->
            reply(From, ok, State);
        false ->
            reply(From, nack, State)
    end,
    {next_state, following, State};
following(Msg, State) ->
    case following_kv(Msg, State) of
        false ->
            common(Msg, State, following);
        Return ->
            Return
    end.

-spec following(_, fsm_from(), state()) -> {next_state, following, state()}.
following({join, _Id}=Msg, From, State) ->
    forward(Msg, From, State);
following(Msg, From, State) ->
    case following_kv(Msg, From, State) of
        false ->
            common(Msg, From, State, following);
        Return ->
            Return
    end.

-spec forward(_, fsm_from(), state()) -> {next_state, following, state()}.
forward(Msg, From, State) ->
    catch gen_fsm:send_event(peer(leader(State), State), {forward, From, Msg}),
    {next_state, following, State}.

-spec valid_request(_,_,state()) -> boolean().
valid_request(Peer, ReqEpoch, State=#state{ready=Ready}) ->
    Ready and (ReqEpoch =:= epoch(State)) and (Peer =:= leader(State)).

-spec check_epoch(peer_id(), epoch(), state()) -> boolean().
check_epoch(Leader, Epoch, State) ->
    (Epoch =:= epoch(State)) and (Leader =:= leader(State)).

-spec increment_epoch(fact() | state()) -> {pos_integer(), fact() | state()}.
increment_epoch(Fact=#fact{epoch=Epoch}) ->
    NextEpoch = Epoch + 1,
    Fact2 = Fact#fact{epoch=NextEpoch, seq=0},
    {NextEpoch, Fact2};
increment_epoch(State=#state{fact=Fact}) ->
    {NextEpoch, Fact2} = increment_epoch(Fact),
    State2 = State#state{fact=Fact2},
    {NextEpoch, State2}.

-spec increment_sequence(fact()) -> fact().
increment_sequence(Fact=#fact{seq=Seq}) ->
    Fact#fact{seq=Seq+1}.

-spec local_commit(fact(), state()) -> state().
local_commit(Fact=#fact{leader=_Leader, epoch=Epoch, seq=Seq, views=Views},
             State=#state{ets=ETS}) ->
    ?OUT("~p: committing (~b,~b): ~p :: ~p :: T=~p~n",
         [State#state.id, Epoch, Seq, _Leader, Views, State#state.timer]),
    State2 = State#state{fact=Fact},
    ok = maybe_save_fact(State2),
    case ets:member(ETS, {obj_seq, Epoch}) of
        true ->
            ets:insert(ETS, [{epoch, Epoch},
                             {seq, Seq}]);
        false ->
            ets:delete_all_objects(ETS),
            ets:insert(ETS, [{epoch, Epoch},
                             {seq, Seq},
                             {{obj_seq, Epoch}, 0}])
    end,
    State2#state{ready=true,
                 members=compute_members(Views)}.

step_down(State) ->
    step_down(probe, State).

step_down(Next, State=#state{lease=Lease, watchers=Watchers}) ->
    lager:debug("~p: stepping down", [State#state.id]),
    _ = notify_leader_status(Watchers, Next, State),
    riak_ensemble_lease:unlease(Lease),
    State2 = cancel_timer(State),
    reset_workers(State),
    State3 = set_leader(undefined, State2),
    case Next of
        probe ->
            probe(init, State3);
        prepare ->
            prepare(init, State3);
        repair ->
            repair(init, State3);
        stop ->
            {stop, normal, State3}
    end.

abandon(State) ->
    Abandoned = {epoch(State), seq(State)},
    State2 = set_leader(undefined, State#state{abandoned=Abandoned}),
    probe(init, State2).

-spec is_pending(state()) -> boolean().
is_pending(#state{ensemble=Ensemble, id=Id, members=Members}) ->
    case riak_ensemble_manager:get_pending(Ensemble) of
        {_, PendingViews} ->
            Pending = compute_members(PendingViews),
            (not lists:member(Id, Members)) andalso lists:member(Id, Pending);
        _ ->
            false
    end.

-spec in_cluster(peer_id(), [node()]) -> boolean().
in_cluster({_, Node}, Cluster) ->
    lists:member(Node, Cluster).

-spec check_views(state()) -> state().
check_views(State=#state{ensemble=Ensemble, fact=Fact}) ->
    %% TODO: Should we really be checking views based on epoch/seq rather than view_vsn/etc?
    Views = Fact#fact.views,
    Vsn = {Fact#fact.epoch, Fact#fact.seq},
    case riak_ensemble_manager:get_views(Ensemble) of
        {CurVsn, CurViews} when (CurVsn > Vsn) or (Views == undefined) ->
            NewFact = Fact#fact{views=CurViews},
            State#state{members=compute_members(CurViews),
                        fact=NewFact};
        _ ->
            State#state{members=compute_members(Views)}
    end.

%%%===================================================================

-spec set_leader(undefined | {_,atom()},state()) -> state().
set_leader(Leader, State=#state{fact=Fact}) ->
    State#state{fact=Fact#fact{leader=Leader}}.

-spec set_epoch(undefined | non_neg_integer(),state()) -> state().
set_epoch(Epoch, State=#state{fact=Fact}) ->
    State#state{fact=Fact#fact{epoch=Epoch}}.

-spec set_seq(undefined | non_neg_integer(),state()) -> state().
set_seq(Seq, State=#state{fact=Fact}) ->
    State#state{fact=Fact#fact{seq=Seq}}.

-spec leader(state()) -> undefined | {_,atom()}.
leader(State) ->
    (State#state.fact)#fact.leader.

-spec epoch(state()) -> undefined | non_neg_integer().
epoch(State) ->
    (State#state.fact)#fact.epoch.

-spec seq(state()) -> undefined | non_neg_integer().
seq(State) ->
    (State#state.fact)#fact.seq.

-spec views(state()) -> undefined | [[{_,atom()}]].
views(State) ->
    (State#state.fact)#fact.views.

%%%===================================================================

-spec common(_, state(), StateName) -> {next_state, StateName, state()}.
common({probe, From}, State=#state{fact=Fact}, StateName) ->
    reply(From, Fact, State),
    {next_state, StateName, State};
common({exchange, From}, State, StateName) ->
    case State#state.tree_trust of
        true ->
            reply(From, ok, State);
        false ->
            reply(From, nack, State)
    end,
    {next_state, StateName, State};
common({all_exchange, From}, State, StateName) ->
    reply(From, ok, State),
    {next_state, StateName, State};
common(tick, State, StateName) ->
    %% TODO: Fix it so we don't have errant tick messages
    {next_state, StateName, State};
common({forward, _From, _Msg}, State, StateName) ->
    {next_state, StateName, State};
common(backend_pong, State, StateName) ->
    State2 = State#state{alive=?ALIVE},
    {next_state, StateName, State2};
common({update_hash, _, _, MaybeFrom}, State, StateName) ->
    maybe_reply(MaybeFrom, nack, State),
    {next_state, StateName, State};
common(Msg, State, StateName) ->
    nack(Msg, State),
    {next_state, StateName, State}.

-spec common(_, fsm_from(), state(), StateName) -> {next_state, StateName, state()}.
common({force_state, {Epoch, Seq}}, From, State, StateName) ->
    State2 = set_epoch(Epoch, set_seq(Seq, State)),
    gen_fsm:reply(From, ok),
    {next_state, StateName, State2};
common(tree_pid, From, State, StateName) ->
    gen_fsm:reply(From, State#state.tree),
    {next_state, StateName, State};
common(tree_corrupted, From, State, StateName) ->
    gen_fsm:reply(From, ok),
    lager:debug("~p: tree_corrupted in state ~p", [State#state.id, StateName]),
    repair(init, State);
common(_Msg, From, State, StateName) ->
    send_reply(From, nack),
    {next_state, StateName, State}.

-spec nack(_, state()) -> ok.
nack({probe, From}, State) ->
    ?OUT("~p: sending nack to ~p~n", [State#state.id, From]),
    %% io:format("~p: sending nack to ~p~n", [State#state.id, From]),
    reply(From, nack, State);
nack({prepare, _, _, From}, State) ->
    ?OUT("~p: sending nack to ~p~n", [State#state.id, From]),
    reply(From, nack, State);
nack({commit, _, From}, State) ->
    ?OUT("~p: sending nack to ~p~n", [State#state.id, From]),
    reply(From, nack, State);
nack({get, _, _, _, From}, State) ->
    ?OUT("~p: sending nack to ~p~n", [State#state.id, From]),
    %% io:format("~p: sending nack to ~p~n", [State#state.id, From]),
    reply(From, nack, State);
nack({put, _, _, _, _, From}, State) ->
    ?OUT("~p: sending nack to ~p~n", [State#state.id, From]),
    reply(From, nack, State);
nack({new_epoch, _, _, From}, State) ->
    reply(From, nack, State);
nack(_Msg, _State) ->
    ?OUT("~p: unable to nack unknown message: ~p~n", [_State#state.id, _Msg]),
    ok.

%%%===================================================================
%%% Ensemble Manager Integration
%%%===================================================================

-type m_tick() :: {ok|failed|changed|shutdown, state()}.
-type m_tick_fun() :: fun((state()) -> m_tick()).

leader_tick(State=#state{ensemble=Ensemble, id=Id, lease=Lease}) ->
    State2 = mod_tick(State),
    M1 = {ok, State2},
    M2 = continue(M1, fun maybe_ping/1),
    M3 = continue(M2, fun maybe_change_views/1),
    M4 = continue(M3, fun maybe_clear_pending/1),
    M5 = continue(M4, fun maybe_update_ensembles/1),
    M6 = continue(M5, fun maybe_transition/1),
    case M6 of
        {failed, State3} ->
            step_down(State3);
        {shutdown, State3} ->
            %% io:format("Shutting down...~n"),
            spawn(fun() ->
                          riak_ensemble_peer_sup:stop_peer(Ensemble, Id)
                  end),
            timer:sleep(1000),
            step_down(stop, State3);
        {_, State3} ->
            riak_ensemble_lease:lease(Lease, riak_ensemble_config:lease()),
            State4 = set_timer(?ENSEMBLE_TICK, tick, State3),
            {next_state, leading, State4}
    end.

-spec continue(m_tick(), m_tick_fun()) -> m_tick().
continue({ok, State}, Fun) ->
    Fun(State);
continue(M={_, _}, _Fun) ->
    M.

-spec maybe_ping(state()) -> {ok|failed, state()}.
maybe_ping(State=#state{id=Id}) ->
    Result = mod_ping(State),
    case Result of
        {ok, _State2} ->
            Result;
        {_, State2} ->
            _ = lager:info("Ping failed. Stepping down: ~p", [Id]),
            {failed, State2}
    end.

-spec maybe_change_views(state()) -> {ok|failed|changed, state()}.
maybe_change_views(State=#state{ensemble=Ensemble, fact=Fact}) ->
    PendVsn = Fact#fact.pend_vsn,
    case riak_ensemble_manager:get_pending(Ensemble) of
        {_, []} ->
            {ok, State};
        {Vsn, Views}
          when (PendVsn =:= undefined) orelse (Vsn > PendVsn) ->
            ViewVsn = {Fact#fact.epoch, Fact#fact.seq},
            NewFact = Fact#fact{views=Views, pend_vsn=Vsn, view_vsn=ViewVsn},
            pause_workers(State),
            case try_commit(NewFact, State) of
                {ok, State2} ->
                    unpause_workers(State),
                    {changed, State2};
                {failed, State2} ->
                    {failed, State2}
            end;
        _ ->
            {ok, State}
    end.

-spec maybe_clear_pending(state()) -> {ok|failed|changed, state()}.
maybe_clear_pending(State=#state{ensemble=Ensemble, fact=Fact}) ->
    #fact{pending=Pending, pend_vsn=PendVsn,
          commit_vsn=CommitVsn, views=Views} = Fact,
    case Pending of
        {_, []} ->
            {ok, State};
        {Vsn, _} when Vsn == PendVsn, Vsn == CommitVsn ->
            case riak_ensemble_manager:get_views(Ensemble) of
                {_, CurViews} when CurViews == Views ->
                    NewFact = change_pending([], State),
                    case try_commit(NewFact, State) of
                        {ok, State2} ->
                            {changed, State2};
                        {failed, State2} ->
                            {failed, State2}
                    end;
                _ ->
                    {ok, State}
            end;
        _ ->
            {ok, State}
    end.

-spec maybe_update_ensembles(state()) -> {ok, state()}.
maybe_update_ensembles(State=#state{ensemble=Ensemble, id=Id, fact=Fact}) ->
    Vsn = Fact#fact.view_vsn,
    Views = Fact#fact.views,
    State2 = case Ensemble of
                 root ->
                     riak_ensemble_root:gossip(self(), Vsn, Id, Views),
                     State;
                 _ ->
                     maybe_async_update(Ensemble, Id, Views, Vsn, State)
             end,
    case Fact#fact.pending of
        {PendingVsn, PendingViews} ->
            riak_ensemble_manager:gossip_pending(Ensemble, PendingVsn, PendingViews);
        _ ->
            ok
    end,
    {ok, State2}.

%% This function implements a non-blocking w/ backpressure approach to sending
%% a message to the ensemble manager. Directly calling _manager:update_ensemble
%% would block the peer. Changing _manager:update_ensemble to use a cast would
%% provide no backpressure. Instead, the peer spawns a singleton process that
%% blocks on the call. As long as the singleton helper is still alive, no new
%% process will be spawned.
-spec maybe_async_update(ensemble_id(), peer_id(), views(), vsn(), state()) -> state().
maybe_async_update(Ensemble, Id, Views, Vsn, State=#state{async=Async}) ->
    CurrentAsync = is_pid(Async) andalso is_process_alive(Async),
    case CurrentAsync of
        true ->
            State;
        false ->
            Async2 = spawn(fun() ->
                                   riak_ensemble_manager:update_ensemble(Ensemble, Id, Views, Vsn)
                           end),
            State#state{async=Async2}
    end.

-spec maybe_transition(state()) -> {ok|failed|shutdown, state()}.
maybe_transition(State=#state{fact=Fact}) ->
    Result = case should_transition(State) of
                 true ->
                     transition(State);
                 false ->
                     try_commit(Fact, State)
             end,
    case Result of
        {ok, _} ->
            Result;
        {failed, _} ->
            Result;
        {shutdown, _} ->
            Result
    end.

%%%===================================================================
%%% K/V Protocol
%%%===================================================================

async(Key, State, Fun) ->
    Workers = State#state.workers,
    Pick = erlang:phash2(Key, tuple_size(Workers)),
    Worker = element(Pick+1, Workers),
    Worker ! {async, Fun},
    ok.

start_worker(ETS) ->
    {ok, Pid} = riak_ensemble_peer_worker:start(ETS),
    monitor(process, Pid),
    Pid.

start_workers(NumWorkers, ETS) ->
    Workers = [start_worker(ETS) || _ <- lists:seq(1, NumWorkers)],
    Workers.

maybe_restart_worker(Pid, State=#state{workers=Workers, ets=ETS}) ->
    WL1 = tuple_to_list(Workers),
    WL2 = [case WorkerPid of
               Pid ->
                   %% io:format("Restarting worker~n"),
                   start_worker(ETS);
               _ ->
                   WorkerPid
           end || WorkerPid <- WL1],
    State#state{workers=list_to_tuple(WL2)}.

reset_workers(#state{workers=Workers}) ->
    WL = tuple_to_list(Workers),
    _ = [begin
             Ref = monitor(process, Pid),
             exit(Pid, kill),
             receive
                 {'DOWN', Ref, _, _, _} ->
                     ok
             end
         end || Pid <- WL],
    %% Pre-existing monitors will also fire, re-creating workers in handle_info
    %% io:format("Killed all workers~n"),
    ok.

pause_workers(#state{workers=Workers, ets=ETS}) ->
    ok = riak_ensemble_peer_worker:pause_workers(tuple_to_list(Workers), ETS).

unpause_workers(#state{workers=Workers, ets=ETS}) ->
    ok = riak_ensemble_peer_worker:unpause_workers(tuple_to_list(Workers), ETS).

-spec leading_kv(_,_,_) -> false | next_state().
leading_kv({get, _Key, _Opts}, From, State=#state{tree_ready=false}) ->
    fail_request(From, State);
leading_kv({get, Key, Opts}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_get_fsm(Key, From, Self, Opts, State) end),
    {next_state, leading, State};
leading_kv(request_failed, _From, State) ->
    step_down(prepare, State);
leading_kv(tree_corrupted, _From, State) ->
    step_down(repair, State#state{tree_trust=false});
leading_kv({local_get, Key}, From, State) ->
    State2 = do_local_get(From, Key, State),
    {next_state, leading, State2};
leading_kv({local_put, Key, Obj}, From, State) ->
    State2 = do_local_put(From, Key, Obj, State),
    {next_state, leading, State2};
leading_kv({put, _Key, _Fun, _Args}, From, State=#state{tree_ready=false}) ->
    fail_request(From, State);
leading_kv({put, Key, Fun, Args}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_put_fsm(Key, Fun, Args, From, Self, State) end),
    {next_state, leading, State};
leading_kv({overwrite, _Key, _Val}, From, State=#state{tree_ready=false}) ->
    fail_request(From, State);
leading_kv({overwrite, Key, Val}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_overwrite_fsm(Key, Val, From, Self, State) end),
    {next_state, leading, State};
leading_kv(_, _From, _State) ->
    false.

fail_request(From, State) ->
    send_reply(From, failed),
    {next_state, leading, State}.

-spec following_kv(_,_) -> false | {next_state,following,state()}.
following_kv({get, Key, Peer, Epoch, From}, State) ->
    case valid_request(Peer, Epoch, State) of
        true ->
            State2 = do_local_get(From, Key, State),
            {next_state, following, State2};
        false ->
            ?OUT("~p: sending nack to ~p for invalid request: ~p != ~p~n", [State#state.id, Peer,
                                                                            {Peer, Epoch},
                                                                            {leader(State),
                                                                             epoch(State)}]),
            %% io:format("~p: sending nack to ~p for invalid request: ~p != ~p~n", [State#state.id, Peer,
            %%                                                                      {Peer, Epoch},
            %%                                                                      {leader(State),
            %%                                                                       epoch(State)}]),
            reply(From, nack, State),
            {next_state, following, State}
    end;
following_kv({put, Key, Obj, Peer, Epoch, From}, State) ->
    case valid_request(Peer, Epoch, State) of
        true ->
            State2 = do_local_put(From, Key, Obj, State),
            {next_state, following, State2};
        false ->
            ?OUT("~p: sending nack to ~p for invalid request: ~p != ~p~n", [State#state.id, Peer,
                                                                            {Peer, Epoch},
                                                                            {leader(State),
                                                                             epoch(State)}]),
            reply(From, nack, State),
            {next_state, following, State}
    end;
following_kv({update_hash, Key, ObjHash, MaybeFrom}, State) ->
    %% TODO: Should this be async?
    case update_hash(Key, ObjHash, State) of
        {corrupted, State2} ->
            maybe_reply(MaybeFrom, nack, State),
            repair(init, State2);
        {ok, State2} ->
            maybe_reply(MaybeFrom, ok, State),
            {next_state, following, State2}
    end;
following_kv(_, _State) ->
    false.

-spec following_kv(_,_,_) -> false | {next_state,following,state()}.
following_kv({get, _Key, _Opts}=Msg, From, State) ->
    forward(Msg, From, State);
following_kv({put, _Key, _Fun, _Args}=Msg, From, State) ->
    forward(Msg, From, State);
following_kv({overwrite, _Key, _Val}=Msg, From, State) ->
    forward(Msg, From, State);
following_kv(_, _From, _State) ->
    false.

-spec send_reply(fsm_from(), std_reply()) -> ok.
send_reply(From, Reply) ->
    case Reply of
        timeout -> ok;
        failed -> ok;
        unavailable -> ok;
        nack -> ok;
        {ok,_} -> ok
    end,
    gen_fsm:reply(From, Reply),
    ok.

do_put_fsm(Key, Fun, Args, From, Self, State=#state{tree=Tree}) ->
    case riak_ensemble_peer_tree:get(Key, Tree) of
        corrupted ->
            %% io:format("Tree corrupted (put)!~n"),
            send_reply(From, failed),
            gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
        KnownHash ->
            do_put_fsm(Key, Fun, Args, From, Self, KnownHash, State)
    end.

do_put_fsm(Key, Fun, Args, From, Self, KnownHash, State) ->
    %% TODO: Timeout should be configurable per request
    Local = local_get(Self, Key, ?LOCAL_GET_TIMEOUT),
    State2 = State#state{self=Self},
    case is_current(Local, Key, KnownHash, State2) of
        local_timeout ->
            %% TODO: Should this send a request_failed?
            %% gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, unavailable);
        true ->
            do_modify_fsm(Key, Local, Fun, Args, From, State2);
        false ->
            case update_key(Key, Local, KnownHash, State2) of
                {ok, Current, _State3} ->
                    do_modify_fsm(Key, Current, Fun, Args, From, State2);
                {corrupted, _State2} ->
                    send_reply(From, failed),
                    gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
                {failed, _State3} ->
                    gen_fsm:sync_send_event(Self, request_failed, infinity),
                    send_reply(From, unavailable)
            end
    end.

%% -spec do_modify_fsm(_,_,fun((_,_) -> any()),{_,_},state()) -> ok.
do_modify_fsm(Key, Current, Fun, Args, From, State=#state{self=Self}) ->
    case modify_key(Key, Current, Fun, Args, State) of
        {ok, New, _State2} ->
            send_reply(From, {ok, New});
        {corrupted, _State2} ->
            send_reply(From, failed),
            gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
        {precondition, _State2} ->
            send_reply(From, failed);
        {failed, _State2} ->
            gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, timeout)
    end.

do_overwrite_fsm(Key, Val, From, Self, State0=#state{ets=ETS}) ->
    State = State0#state{self=Self},
    Epoch = epoch(State),
    Seq = obj_sequence(ETS, Epoch),
    Obj = new_obj(Epoch, Seq, Key, Val, State),
    case put_obj(Key, Obj, State) of
        {ok, Result, _State2} ->
            send_reply(From, {ok, Result});
        {corrupted, _State2} ->
            send_reply(From, timeout),
            gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
        {failed, _State2} ->
            gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, timeout)
    end.

-spec do_get_fsm(_,{_,_},pid(),_,state()) -> ok.
do_get_fsm(Key, From, Self, Opts, State=#state{tree=Tree}) ->
    case riak_ensemble_peer_tree:get(Key, Tree) of
        corrupted ->
            %% io:format("Tree corrupted (get)!~n"),
            send_reply(From, failed),
            gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
        KnownHash ->
            do_get_fsm(Key, From, Self, KnownHash, Opts, State)
    end.

-spec do_get_fsm(_,{_,_},pid(),_,_,state()) -> ok.
do_get_fsm(Key, From, Self, KnownHash, Opts, State0) ->
    State = State0#state{self=Self},
    Local = local_get(Self, Key, ?LOCAL_GET_TIMEOUT),
    %% TODO: Allow get to return errors. Make consistent with riak_kv_vnode
    %% TODO: Returning local directly only works if we ensure leader lease
    LocalOnly = not lists:member(read_repair, Opts),
    case is_current(Local, Key, KnownHash, State) of
        local_timeout ->
            %% TODO: Should this send a request_failed?
            %% gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, timeout);
        true ->
            case LocalOnly of
                true ->
                    case check_lease(State) of
                        true ->
                            send_reply(From, {ok, Local});
                        false ->
                            %% TODO: If there's a new leader, we could forward
                            %%       instead of timeout.
                            send_reply(From, timeout),
                            gen_fsm:sync_send_event(Self, request_failed, infinity)
                    end;
                false ->
                    case get_latest_obj(Key, Local, KnownHash, State) of
                        {ok, Latest, Replies, _State2} ->
                            maybe_repair(Key, Latest, Replies, State),
                            send_reply(From, {ok, Latest});
                        {failed, _State2} ->
                            send_reply(From, timeout)
                    end
            end;
        false ->
            ?OUT("~p :: not current~n", [Key]),
            case update_key(Key, Local, KnownHash, State) of
                {ok, Current, _State2} ->
                    send_reply(From, {ok, Current});
                {corrupted, _State2} ->
                    send_reply(From, failed),
                    gen_fsm:sync_send_event(Self, tree_corrupted, infinity);
                {failed, _State2} ->
                    %% TODO: Should this be failed or unavailable?
                    send_reply(From, failed),
                    gen_fsm:sync_send_event(Self, request_failed, infinity)
            end
    end.

-spec check_lease(state()) -> boolean().
check_lease(State=#state{id=Id}) ->
    case valid_lease(State) of
        true ->
            true;
        false ->
            Epoch = epoch(State),
            {Future, _State2} = blocking_send_all({check_epoch, Id, Epoch}, State),
            case wait_for_quorum(Future) of
                {quorum_met, _Replies} ->
                    true;
                {timeout, _Replies} ->
                    false
            end
    end.

-spec valid_lease(state()) -> boolean().
valid_lease(#state{lease=Lease}) ->
    case riak_ensemble_config:trust_lease() of
        true ->
            riak_ensemble_lease:check_lease(Lease);
        _ ->
            false
    end.

maybe_repair(Key, Latest, Replies, State=#state{id=Id}) ->
    %% TODO: Should only send puts to peers that are actually divergent.
    ShouldRepair = lists:any(fun({_, nack}) ->
                                     false;
                                ({_Peer, Obj}) when (Obj =:= Latest) ->
                                     false;
                                ({_Peer, _Obj}) ->
                                     true
                             end, Replies),
    case ShouldRepair of
        true ->
            %% TODO: Following is kinda ugly, but works without code change.
            Epoch = epoch(State),
            Dummy = spawn(fun() -> ok end),
            DummyFrom = {Dummy, undefined},
            ok = cast_all({put, Key, Latest, Id, Epoch, DummyFrom}, State);
        false ->
            ok
    end.

-spec do_local_get(_, _, state()) -> state().
do_local_get(From, Key, State) ->
    %% Note: backend module is responsible for replying
    State2 = mod_get(Key, From, State),
    State2.

-spec do_local_put(_, _, obj(), state()) -> state().
do_local_put(From, Key, Value, State) ->
    %% Note: backend module is responsible for replying
    State2 = mod_put(Key, Value, From, State),
    State2.

-spec is_current(maybe_obj(), key(), _, state()) -> false | local_timeout | true.
is_current(timeout, _Key, _KnownHash, _State) ->
    local_timeout;
is_current(notfound, _Key, _KnownHash, _State) ->
    false;
is_current(Obj, Key, KnownHash, State) ->
    case verify_hash(Key, Obj, KnownHash, State) of
        true ->
            Epoch = get_obj(epoch, Obj, State),
            Epoch =:= epoch(State);
        false ->
            false
    end.

-spec update_key(_,_,_,state()) -> {ok, obj(), state()} | {failed,state()} | {corrupted,state()}.
update_key(Key, Local, KnownHash, State) ->
    NumPeers = length(get_peers(State#state.members, State)),
    case get_latest_obj(Key, Local, KnownHash, State) of
        {ok, Latest, Replies, State2} when
              Latest =:= notfound andalso
              (length(Replies) + 1) =:= NumPeers ->
            %% If we get a reply back from every other node and find that
            %% nobody has a copy, we can safely skip writing a tombstone.
            %% (The + 1 in the guard above is due to the fact that we don't
            %% expect a reply from ourselves, since if we get here then we
            %% already did a local get directly and got notfound.)
            ?OUT("Got back notfound from every peer! Skipping tombstone for key ~p", [Key]),

            %% The client expects an object, but in this case we don't have
            %% one, so create a "fake" notfound object to pass back.
            Seq = obj_sequence(State2),
            Epoch = epoch(State2),
            New = new_obj(Epoch, Seq, Key, notfound, State2),

            {ok, New, State2};
        {ok, Latest, _Replies, State2} ->
            case put_obj(Key, Latest, State2) of
                {ok, New, State3} ->
                    {ok, New, State3};
                {corrupted, State3} ->
                    {corrupted, State3};
                {failed, State3} ->
                    {failed, State3}
            end;
        {failed, State2} ->
            {failed, State2}
    end.

%% -spec modify_key(_,_,fun((_,_) -> any()), state()) -> {failed,state()} |
%%                                                       {precondition,state()} |
%%                                                       {ok,obj(),state()}.
modify_key(Key, Current, Fun, Args, State) ->
    Seq = obj_sequence(State),
    FunResult = case Args of
                    [] ->
                        Fun(Current, Seq, State);
                    _ ->
                        Fun(Current, Seq, State, Args)
                end,
    case FunResult of
        {ok, New} ->
            case put_obj(Key, New, Seq, State) of
                {ok, Result, State2} ->
                    {ok, Result, State2};
                {corrupted, State2} ->
                    {corrupted, State2};
                {failed, State2} ->
                    {failed, State2}
            end;
        failed ->
            {precondition, State}
    end.

-spec get_latest_obj(_,_,_,state()) -> {ok, obj(), _, state()} | {failed, state()}.
get_latest_obj(Key, Local, KnownHash, State=#state{id=Id, members=Members}) ->
    Epoch = epoch(State),
    Peers = get_peers(Members, State),
    %% In addition to meeting quorum, we must know of at least one object
    %% that is valid according to the object hash.
    Check = fun(Replies) ->
                    T = lists:any(fun({_, nack}) ->
                                          false;
                                     ({_, notfound}) ->
                                          KnownHash =:= notfound;
                                     ({_, Obj}) ->
                                          ObjHash = get_obj_hash(Key, Obj, State),
                                          valid_obj_hash(ObjHash, KnownHash)
                                  end, Replies),
                    T
            end,
    Check2 = case verify_hash(Key, Local, KnownHash, State) of
                 true ->
                     undefined;
                 false ->
                     Check
             end,
    Required = case KnownHash of
                   notfound -> all_or_quorum;
                   _ ->        quorum
               end,
    {Future, State2} = blocking_send_all({get, Key, Id, Epoch}, Peers, Required, Check2, State),
    case wait_for_quorum(Future) of
        {quorum_met, Replies} ->
            Latest = latest_obj(Replies, Local, State),
            case verify_hash(Key, Latest, KnownHash, State) of
                true ->
                    {ok, Latest, Replies, State2};
                false ->
                    {failed, State2}
            end;
        {timeout, _Replies} ->
            {failed, State2}
    end.

-spec put_obj(_,obj(),state()) -> {ok, obj(), state()} | {failed,state()} | {corrupted,state()}.
put_obj(Key, Obj, State) ->
    Seq = obj_sequence(State),
    put_obj(Key, Obj, Seq, State).

-spec put_obj(_,obj(),seq(),state()) -> {ok, obj(), state()} | {failed,state()} | {corrupted,state()}.
put_obj(Key, Obj, Seq, State=#state{id=Id, members=Members, self=Self}) ->
    Epoch = epoch(State),
    Obj2 = increment_obj(Key, Obj, Seq, State),
    Peers = get_peers(Members, State),
    {Future, State2} = blocking_send_all({put, Key, Obj2, Id, Epoch}, Peers, State),
    case local_put(Self, Key, Obj2, ?LOCAL_PUT_TIMEOUT) of
        failed ->
            lager:warning("Failed local_put for Key ~p, Id = ~p", [Key, Id]),
            gen_fsm:sync_send_event(Self, request_failed, infinity),
            {failed, State2};
        Local ->
            case wait_for_quorum(Future) of
                {quorum_met, _Replies} ->
                    ObjHash = get_obj_hash(Key, Local, State2),
                    case update_hash(Key, ObjHash, State2) of
                        {ok, State3} ->
                            case send_update_hash(Key, ObjHash, State3) of
                                {ok, State4} ->
                                    {ok, Local, State4};
                                {failed, State4} ->
                                    {failed, State4}
                            end;
                        {corrupted, State3} ->
                            {corrupted, State3}
                    end;
                {timeout, _Replies} ->
                    {failed, State2}
            end
    end.

send_update_hash(Key, ObjHash, State) ->
    case riak_ensemble_config:synchronous_tree_updates() of
        false ->
            Msg = {update_hash, Key, ObjHash, undefined},
            cast_all(Msg, State),
            {ok, State};
        true ->
            Msg = {update_hash, Key, ObjHash},
            {Future, State2} = blocking_send_all(Msg, State),
            case wait_for_quorum(Future) of
                {quorum_met, _Replies} ->
                    {ok, State2};
                {timeout, _Replies} ->
                    {failed, State2}
            end
    end.

get_obj_hash(_Key, Obj, State) ->
    ObjEpoch = get_obj(epoch, Obj, State),
    ObjSeq = get_obj(seq, Obj, State),
    %% TODO: For now, we simply store the epoch/sequence and rely upon
    %%       backend CRCs to ensure objects with the proper epoch/seq
    %%       have the proper data. In the future, we should support
    %%       actual hashing here for stronger guarantees.
    <<?H_OBJ_NONE, ObjEpoch:64/integer, ObjSeq:64/integer>>.

valid_obj_hash(ActualHash = <<?H_OBJ_NONE, _/binary>>,
               KnownHash  = <<?H_OBJ_NONE, _/binary>>) ->
    %% No actual hash to verify, just ensure epoch/seq is equal or newer
    ActualHash >= KnownHash.

update_hash(Key, ObjHash, State=#state{tree=Tree}) ->
    case riak_ensemble_peer_tree:insert(Key, ObjHash, Tree) of
        corrupted ->
            %% io:format("Tree corrupted (update_hash)!~n"),
            {corrupted, State};
        ok ->
            {ok, State}
    end.

verify_hash(Key, notfound, KnownHash, _State) ->
    case KnownHash of
        notfound ->
            true;
        _ ->
            lager:warning("~p detected as corrupted", [Key]),
            false
    end;
verify_hash(Key, Obj, KnownHash, State) ->
    ObjHash = get_obj_hash(Key, Obj, State),
    case KnownHash of
        notfound ->
            %% An existing object is by definition newer than notfound.
            true;
        _ ->
            case valid_obj_hash(ObjHash, KnownHash) of
                true ->
                    true;
                false ->
                    lager:warning("~p detected as corrupted :: ~p",
                                  [Key, {ObjHash, KnownHash}]),
                    false
            end
    end.

-spec increment_obj(key(), obj(), seq(), state()) -> obj().
increment_obj(Key, Obj, Seq, State) ->
    Epoch = epoch(State),
    case Obj of
        notfound ->
            new_obj(Epoch, Seq, Key, notfound, State);
        _ ->
            set_obj(epoch, Epoch,
                    set_obj(seq, Seq, Obj, State), State)
    end.

-spec obj_sequence(state()) -> seq().
obj_sequence(State=#state{ets=ETS}) ->
    Epoch = epoch(State),
    obj_sequence(ETS, Epoch).

-spec obj_sequence(atom() | ets:tid(), epoch()) -> seq().
obj_sequence(ETS, Epoch) ->
    try
        Seq = ets:update_counter(ETS, seq, 0),
        ObjSeq = ets:update_counter(ETS, {obj_seq, Epoch}, 1),
        Seq + ObjSeq
    catch
        _:_ ->
            %% io:format("EE: ~p~n", [ets:tab2list(ETS)]),
            throw(die)
    end.

-spec latest_obj([{_,_}],_,_) -> any().
latest_obj([], Latest, _State) ->
    Latest;
latest_obj([{_,notfound}|L], ObjA, State) ->
    latest_obj(L, ObjA, State);
latest_obj([{_,ObjB}|L], notfound, State) ->
    latest_obj(L, ObjB, State);
latest_obj([{_,ObjB}|L], ObjA, State=#state{mod=Mod}) ->
    LatestObj = riak_ensemble_backend:latest_obj(Mod, ObjA, ObjB),
    latest_obj(L, LatestObj, State).

-spec get_value(_,_,atom() | state()) -> any().
get_value(Obj, Default, State) ->
    case get_obj(value, Obj, State) of
        notfound ->
            Default;
        Value ->
            Value
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init([any(),...]) -> {ok, setup, state()}.
init([Mod, Ensemble, Id, Args]) ->
    lager:debug("~p: starting peer", [Id]),
    _ = riak_ensemble_util:random_seed(Id),
    ETS = ets:new(x, [public, {read_concurrency, true}, {write_concurrency, true}]),
    TreeTrust = case riak_ensemble_config:tree_validation() of
                    false ->
                        true;
                    _ ->
                        false
                end,
    State = #state{id=Id,
                   ensemble=Ensemble,
                   ets=ETS,
                   peers=[],
                   tree_trust=TreeTrust,
                   alive=?ALIVE,
                   mod=Mod},
    gen_fsm:send_event(self(), {init, Args}),
    riak_ensemble_peer_sup:register_peer(Ensemble, Id, self(), ETS),
    {ok, setup, State}.

setup({init, Args}, State0=#state{id=Id, ensemble=Ensemble, ets=ETS, mod=Mod}) ->
    lager:debug("~p: setup", [Id]),
    NumWorkers = ?WORKERS,
    {TreeId, Path} = mod_synctree(State0),
    Tree = open_hashtree(Ensemble, Id, TreeId, Path),
    Saved = reload_fact(Ensemble, Id),
    Workers = start_workers(NumWorkers, ETS),
    Members = compute_members(Saved#fact.views),
    {ok, Lease} = riak_ensemble_lease:start_link(),
    State = State0#state{workers=list_to_tuple(Workers),
                         tree=Tree,
                         fact=Saved,
                         members=Members,
                         lease=Lease,
                         modstate=riak_ensemble_backend:start(Mod, Ensemble, Id, Args)},
    State2 = check_views(State),
    %% TODO: Why are we local commiting on startup?
    State3 = local_commit(State2#state.fact, State2),
    probe(init, State3).

-spec handle_event(_, atom(), state()) -> {next_state, atom(), state()}.
handle_event({watch_leader_status, Pid}, StateName, State) when node(Pid) =/= node() ->
    lager:debug("Remote pid ~p not allowed to watch_leader_status on ensemble peer ~p",
                [Pid, State#state.id]),
    {next_state, StateName, State};
handle_event({watch_leader_status, Pid}, StateName, State = #state{watchers = Watchers}) ->
    case is_watcher(Pid, Watchers) of
        true ->
            lager:debug("Got watch_leader_status for ~p, but pid already in watchers list"),
            {next_state, StateName, State};
        false ->
            _ = notify_leader_status(Pid, StateName, State),
            MRef = erlang:monitor(process, Pid),
            {next_state, StateName, State#state{watchers = [{Pid, MRef} | Watchers]}}
    end;
handle_event({stop_watching, Pid}, StateName, State = #state{watchers = Watchers}) ->
    case remove_watcher(Pid, Watchers) of
        not_found ->
            lager:debug("Tried to stop watching for pid ~p, but did not find it in watcher list"),
            {next_state, StateName, State};
        {MRef, NewWatcherList} ->
            erlang:demonitor(MRef, [flush]),
            {next_state, StateName, State#state{watchers = NewWatcherList}}
    end;
handle_event({reply, ReqId, Peer, Reply}, StateName, State) ->
    State2 = handle_reply(ReqId, Peer, Reply, State),
    {next_state, StateName, State2};
handle_event({peer_pid, PeerId, Pid}, StateName, State) ->
    {_Ensemble, Id} = PeerId,
    Peers = orddict:store(Id, Pid, State#state.peers),
    {next_state, StateName, State#state{peers=Peers}};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

-spec handle_sync_event(_, _, atom(), state()) -> {reply, ok, atom(), state()} |
                                                  {reply, ensemble_id(), atom(), state()} |
                                                  {stop, normal, ok, state()}.
handle_sync_event(get_leader, _From, StateName, State) ->
    {reply, leader(State), StateName, State};
handle_sync_event(get_info, _From, StateName, State=#state{tree_trust=Trust}) ->
    Epoch = epoch(State),
    Info = {StateName, Trust, Epoch},
    {reply, Info, StateName, State};
handle_sync_event(tree_info, _From, StateName, State=#state{tree_trust=Trust,
                                                            tree_ready=Ready,
                                                            tree=Pid}) ->
    TopHash = riak_ensemble_peer_tree:top_hash(Pid),
    Info = {Trust, Ready, TopHash},
    {reply, Info, StateName, State};
handle_sync_event({debug_local_get, Key}, From, StateName, State) ->
    State2 = do_local_get(From, Key, State),
    {next_state, StateName, State2};
handle_sync_event(get_watchers, _From, StateName, State) ->
    {reply, State#state.watchers, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% -spec handle_info(_, atom(), state()) -> next_state().
handle_info({'DOWN', MRef, _, Pid, Reason}, StateName, State) ->
    Watchers = State#state.watchers,
    case remove_watcher(Pid, Watchers) of
        {MRef, NewWatcherList} ->
            {next_state, StateName, State#state{watchers = NewWatcherList}};
        not_found ->
            %% If the DOWN signal was not for a watcher, we must pass it through
            %% to the callback module in case that's where it's supposed to go
            module_handle_down(MRef, Pid, Reason, StateName, State)
    end;
handle_info(quorum_timeout, StateName, State) ->
    State2 = quorum_timeout(State),
    {next_state, StateName, State2};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

module_handle_down(MRef, Pid, Reason, StateName, State) ->
    #state{mod=Mod, modstate=ModState} = State,
    case Mod:handle_down(MRef, Pid, Reason, ModState) of
        false ->
            State2 = maybe_restart_worker(Pid, State),
            {next_state, StateName, State2};
        {ok, ModState2} ->
            {next_state, StateName, State#state{modstate=ModState2}};
        {reset, ModState2} ->
            State2 = State#state{modstate=ModState2},
            step_down(State2)
    end.

-spec terminate(_,_,_) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

-spec code_change(_, atom(), state(), _) -> {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec cast_all(_, state()) -> ok.
cast_all(Msg, State=#state{id=Id, members=Members}) ->
    Peers = get_peers(Members, State),
    riak_ensemble_msg:cast_all(Msg, Id, Peers).

-spec send_all(_,state()) -> state().
send_all(Msg, State) ->
    send_all(Msg, quorum, State).

send_all(Msg, Required, State=#state{members=Members}) ->
    send_peers(Msg, Members, Required, State).

send_peers(Msg, Members, Required, State) ->
    Views = views(State),
    send_peers(Msg, Members, Required, Views, State).

send_peers(Msg, Members, Required, Views, State=#state{id=Id}) ->
    Peers = get_peers(Members, State),
    Awaiting = riak_ensemble_msg:send_all(Msg, Id, Peers, Views, Required),
    State#state{awaiting=Awaiting}.

-spec blocking_send_all(any(), state()) -> {riak_ensemble_msg:future(), state()}.
blocking_send_all(Msg, State=#state{members=Members}) ->
    Peers = get_peers(Members, State),
    blocking_send_all(Msg, Peers, State).

-spec blocking_send_all(any(), peer_pids(), state()) -> {riak_ensemble_msg:future(), state()}.
blocking_send_all(Msg, Peers, State) ->
    blocking_send_all(Msg, Peers, quorum, State).

-spec blocking_send_all(any(), peer_pids(), _, state()) -> Result when
      Result :: {riak_ensemble_msg:future(), state()}.
blocking_send_all(Msg, Peers, Required, State) ->
    blocking_send_all(Msg, Peers, Required, undefined, State).

-spec blocking_send_all(any(), peer_pids(), _, Extra, state()) -> Result when
      Extra  :: riak_ensemble_msg:extra_check(),
      Result :: {riak_ensemble_msg:future(), state()}.
blocking_send_all(Msg, Peers, Required, Extra, State=#state{id=Id}) ->
    Views = views(State),
    {Future, Awaiting} = riak_ensemble_msg:blocking_send_all(Msg, Id, Peers, Views, Required, Extra),
    State2 = State#state{awaiting=Awaiting},
    {Future, State2}.

-spec wait_for_quorum(riak_ensemble_msg:future()) -> {quorum_met, [peer_reply()]} |
                                                     {timeout, [peer_reply()]}.
wait_for_quorum(Future) ->
    riak_ensemble_msg:wait_for_quorum(Future).

quorum_timeout(State=#state{awaiting=undefined}) ->
    State;
quorum_timeout(State=#state{awaiting=Awaiting}) ->
    Awaiting2 = riak_ensemble_msg:quorum_timeout(Awaiting),
    State#state{awaiting=Awaiting2}.

maybe_reply(undefined, _, _) ->
    ok;
maybe_reply(From, Reply, State) ->
    reply(From, Reply, State).

-spec reply(riak_ensemble_msg:msg_from(), any(), state()) -> ok.
reply(From, Reply, #state{id=Id}) ->
    riak_ensemble_msg:reply(From, Id, Reply).

-spec handle_reply(any(), peer_id(), any(), state()) -> state().
handle_reply(ReqId, Peer, Reply, State=#state{awaiting=Awaiting}) ->
    Awaiting2 = riak_ensemble_msg:handle_reply(ReqId, Peer, Reply, Awaiting),
    State#state{awaiting=Awaiting2}.

-spec latest_fact([{_,{_,_,_,_,_}}],_) -> any().
latest_fact([], Fact) ->
    Fact;
latest_fact([{_,FactB}|L], FactA) ->
    A = {FactA#fact.epoch, FactA#fact.seq},
    B = {FactB#fact.epoch, FactB#fact.seq},
    case B > A of
        true  -> latest_fact(L, FactB);
        false -> latest_fact(L, FactA)
    end.

existing_leader(Replies, Abandoned, #fact{leader=undefined, views=Views}) ->
    Members = compute_members(Views),
    Counts = lists:foldl(fun({_, #fact{epoch=Epoch, seq=Seq, leader=Leader}}, Counts) ->
                                 Vsn = {Epoch, Seq},
                                 Valid = (Abandoned =:= undefined) or (Vsn > Abandoned),
                                 case Valid andalso lists:member(Leader, Members) of
                                     true ->
                                         dict:update_counter({Epoch, Leader}, 1, Counts);
                                     false ->
                                         Counts
                                 end
                         end, dict:new(), Replies),
    Choices = lists:reverse(lists:keysort(2, dict:to_list(Counts))),
    case Choices of
        [] ->
            undefined;
        [{{_, Leader}, _Count}|_] ->
            %% io:format("----~n~p~n~p~n-----~n", [Replies, Leader]),
            Leader
    end;
existing_leader(_Replies, Abandoned, #fact{epoch=Epoch, seq=Seq, leader=Leader}) ->
    case {Epoch, Seq} > Abandoned of
        true ->
            Leader;
        false ->
            undefined
    end.

notify_leader_status(WatcherList, StateName, State) when is_list(WatcherList) ->
    [notify_leader_status(Pid, StateName, State) || {Pid, _MRef} <- WatcherList];
notify_leader_status(Pid, leading, State = #state{id = Id, ensemble = Ensemble}) ->
    Pid ! {is_leading, self(), Id, Ensemble, epoch(State)};
notify_leader_status(Pid, _, State = #state{id = Id, ensemble = Ensemble}) ->
    Pid ! {is_not_leading, self(), Id, Ensemble, epoch(State)}.

-spec compute_members([[any()]]) -> [any()].
compute_members(undefined) ->
    [];
compute_members(Views) ->
    lists:usort(lists:append(Views)).

-spec get_peers([maybe_peer_id()], state()) -> [{maybe_peer_id(), maybe_pid()}].
get_peers(Members, State=#state{id=_Id}) ->
    %% [{Peer, peer(Peer, State)} || Peer <- Members,
    %%                               Peer =/= Id].
    [{Peer, peer(Peer, State)} || Peer <- Members].

-spec peer(maybe_peer_id(), state()) -> maybe_pid().
peer(Id, #state{id=Id}) ->
    self();
peer(Id, #state{ensemble=Ensemble}) ->
    riak_ensemble_manager:get_peer_pid(Ensemble, Id).

is_watcher(Pid, WatcherList) ->
    case lists:keyfind(Pid, 1, WatcherList) of
        false ->
            false;
        _ ->
            true
    end.

remove_watcher(Pid, WatcherList) ->
    case lists:keytake(Pid, 1, WatcherList) of
        false ->
            not_found;
        {value, {_Pid, MRef}, NewWatcherList} ->
            {MRef, NewWatcherList}
    end.

%%%===================================================================
%%% Behaviour Interface
%%%===================================================================

mod_ping(State=#state{mod=Mod, modstate=ModState, alive=Alive}) ->
    {Result, ModState2} = Mod:ping(self(), ModState),
    {Reply, Alive2} = case Result of
                          ok ->
                              {ok, Alive};
                          failed ->
                              {failed, Alive};
                          async when (Alive > 0) ->
                              {ok, Alive - 1};
                          async ->
                              {failed, Alive}
                      end,
    State2 = State#state{modstate=ModState2, alive=Alive2},
    {Reply, State2}.

mod_tick(State=#state{mod=Mod, modstate=ModState, fact=Fact}) ->
    #fact{epoch=Epoch, seq=Seq, leader=Leader, views=Views} = Fact,
    ModState2 = Mod:tick(Epoch, Seq, Leader, Views, ModState),
    State#state{modstate=ModState2}.

mod_get(Key, From, State=#state{mod=Mod, modstate=ModState, id=Id}) ->
    ModState2 = Mod:get(Key, {From, Id}, ModState),
    State#state{modstate=ModState2}.

mod_put(Key, Obj, From, State=#state{mod=Mod, modstate=ModState, id=Id}) ->
    ModState2 = Mod:put(Key, Obj, {From, Id}, ModState),
    State#state{modstate=ModState2}.

-spec new_obj(_,_,_,_,state()) -> any().
new_obj(Epoch, Seq, Key, Value, #state{mod=Mod, modstate=_ModState}) ->
    Mod:new_obj(Epoch, Seq, Key, Value).

get_obj(X, Obj, Mod) when is_atom(Mod) ->
    riak_ensemble_backend:get_obj(Mod, X, Obj);
get_obj(X, Obj, #state{mod=Mod, modstate=_ModState}) ->
    riak_ensemble_backend:get_obj(Mod, X, Obj).

set_obj(X, Val, Obj, #state{mod=Mod, modstate=_ModState}) ->
    riak_ensemble_backend:set_obj(Mod, X, Val, Obj).

mod_synctree(#state{ensemble=Ensemble, id=Id, mod=Mod}) ->
    {TreeId, Base} = case Mod:synctree_path(Ensemble, Id) of
                         default ->
                             {<<>>, default_path(Ensemble, Id)};
                         {_, _}=Result ->
                             Result
                     end,
    {TreeId, full_path(Base)}.

default_path(Ensemble, Id) ->
    <<Name:160/integer>> = crypto:hash(sha, term_to_binary({Ensemble, Id})),
    integer_to_list(Name).

full_path(Base) ->
    {ok, Root} = application:get_env(riak_ensemble, data_root),
    filename:join([Root, "ensembles", "trees", Base]).

%%%===================================================================

open_hashtree(Ensemble, Id, TreeId, Path) ->
    %% TODO: Move retry logic into riak_ensemble_peer_tree itself?
    try
        {ok, Pid} = riak_ensemble_peer_tree:start_link({Ensemble, Id}, TreeId, Path),
        Pid
    catch A:B ->
            lager:info("Failed to open hashtree: ~p/~p", [A,B]),
            timer:sleep(1000),
            open_hashtree(Ensemble, Id, TreeId, Path)
    end.

-spec reload_fact(_,_) -> any().
reload_fact(Ensemble, Id) ->
    case load_saved_fact(Ensemble, Id) of
        {ok, Fact} ->
            Fact;
        not_found ->
            #fact{epoch=0,
                  seq=0,
                  view_vsn={0,0},
                  leader=undefined}
    end.

-spec load_saved_fact(_,_) -> not_found | {ok,_}.
load_saved_fact(Ensemble, Id) ->
    riak_ensemble_storage:get({Ensemble, Id}).

-spec maybe_save_fact(state()) -> ok.
maybe_save_fact(State=#state{ensemble=Ensemble, id=Id, fact=NewFact}) ->
    OldFact = reload_fact(Ensemble, Id),
    case should_save(NewFact, OldFact) of
        false ->
            ok;
        true ->
            ok = save_fact(State)
    end.

-spec should_save(fact(), fact()) -> boolean().
should_save(NewFact, OldFact) ->
    %% Ignore sequence number when comparing
    A = NewFact#fact{seq=undefined},
    B = OldFact#fact{seq=undefined},
    A =/= B.

-spec save_fact(state()) -> ok | {error,_}.
save_fact(#state{ensemble=Ensemble, id=Id, fact=Fact}) ->
    try
        true = riak_ensemble_storage:put({Ensemble, Id}, Fact),
        ok = riak_ensemble_storage:sync()
    catch
        _:Err ->
            %% _ = lager:error("Failed saving ensemble ~p state to ~p: ~p",
            %%                 [{Ensemble, Id}, File, Err]),
            {error, Err}
    end.

-spec set_timer(non_neg_integer(), any(), state()) -> state().
set_timer(Time, Event, State) ->
    State2 = cancel_timer(State),
    Timer = gen_fsm:send_event_after(Time, Event),
    State2#state{timer=Timer}.

-spec cancel_timer(state()) -> state().
cancel_timer(State=#state{timer=undefined}) ->
    State;
cancel_timer(State=#state{timer=Timer}) ->
    %% Note: gen_fsm cancel_timer discards timer message if already sent
    catch gen_fsm:cancel_timer(Timer),
    State#state{timer=undefined}.
