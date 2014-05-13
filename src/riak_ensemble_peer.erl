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
-export([sync_complete/2, sync_failed/1]).
-export([kget/4, kupdate/6, kput_once/5, kover/5, kmodify/6, kdelete/4,
         ksafe_delete/5, obj_value/2, obj_value/3]).
-export([setup/2]).
-export([probe/2, election/2, prepare/2, leading/2, following/2,
         probe/3, election/3, prepare/3, leading/3, following/3]).
-export([pending/2, sync/2, all_sync/2, check_sync/2, prelead/2, prefollow/2,
         pending/3, sync/3, all_sync/3, check_sync/3, prelead/3, prefollow/3]).

%% Support/debug API
-export([count_quorum/2, check_quorum/2, force_state/2]).

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
                trust         :: boolean(),
                trust_pid     :: pid(),
                alive         :: integer(),
                last_views    :: [[peer_id()]],
                async         :: pid(),
                self          :: pid()
               }).

-type state() :: #state{}.

-define(ALIVE, 1).

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
    riak_ensemble_router:sync_send_event(node(), Ensemble, count_quorum, Timeout).

-spec get_leader(pid()) -> peer_id().
get_leader(Pid) when is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_leader, infinity).

-spec sync_complete(pid(), [peer_id()]) -> ok.
sync_complete(Pid, Peers) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, {sync_complete, Peers}).

-spec sync_failed(pid()) -> ok.
sync_failed(Pid) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, sync_failed).

backend_pong(Pid) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, backend_pong).

force_state(Pid, EpochSeq) ->
    gen_fsm:sync_send_event(Pid, {force_state, EpochSeq}).

%%%===================================================================
%%% K/V API
%%%===================================================================

-spec kget(node(), target(), key(), timeout()) -> std_reply().
kget(Node, Target, Key, Timeout) ->
    Result = riak_ensemble_router:sync_send_event(Node, Target, {get, Key}, Timeout),
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

%%%===================================================================
%%% Core Protocol
%%%===================================================================

-spec probe(_, state()) -> next_state().
probe(init, State) ->
    ?OUT("~p: probe~n", [State#state.id]),
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
    State2 = set_timer(1000, probe_continue, State),
    {next_state, probe, State2};
probe(probe_continue, State) ->
    probe(init, State);
probe(Msg, State) ->
    common(Msg, State, probe).

-spec probe(_, fsm_from(), state()) -> {next_state, probe, state()}.
probe(Msg, From, State) ->
    common(Msg, From, State, probe).

pending(init, State) ->
    State2 = set_timer(?ENSEMBLE_TICK * 10, pending_timeout, State),
    %% TODO: Trusting pending peers makes ensemble vulnerable to concurrent
    %%       node failures during membership changes. Change to move to
    %%       syncing state before moving to following.
    {next_state, pending, State2#state{trust=true}};
pending(pending_timeout, State) ->
    probe({timeout, []}, State);
pending({prepare, Id, NextEpoch, From}, State=#state{fact=Fact}) ->
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
            {next_state, pending, State}
    end;
pending({commit, NewFact, From}, State) ->
    Epoch = epoch(State),
    case NewFact#fact.epoch >= Epoch of
        true ->
            reply(From, ok, State),
            State2 = local_commit(NewFact, State),
            State3 = cancel_timer(State2),
            following(init, State3);
        false ->
            {next_state, pending, State}
    end;
pending(Msg, State) ->
    common(Msg, State, pending).

pending(Msg, From, State) ->
    common(Msg, From, State, pending).

maybe_follow(_, State=#state{trust=false}) ->
    %% This peer is untrusted and must sync
    sync(init, State);
maybe_follow(undefined, State) ->
    election(init, set_leader(undefined, State));
maybe_follow(Leader, State=#state{id=Leader}) ->
    election(init, set_leader(undefined, State));
maybe_follow(Leader, State) ->
    %% io:format("~p: Following ~p~n", [State#state.id, Leader]),
    %% TODO: Should we use prefollow instead of following(not_ready)?
    following(not_ready, set_leader(Leader, State)).

sync(init, State) ->
    ?OUT("~p: sync~n", [State#state.id]),
    %% _ = lager:info("~p is UNtrusted", [State#state.id]),
    State2 = send_all(sync, other, State),
    {next_state, sync, State2};
sync({quorum_met, Replies}, State) ->
    {Result, State2} = mod_sync(Replies, State),
    %% io:format("========~n~p~n~p~n~p~n~p~n========~n", [Replies, Result, State, State2]),
    case Result of
        ok ->
            probe(init, State2#state{trust=true});
        async ->
            {next_state, sync, State2};
        {error, _} ->
            ?OUT("~p/~p: error when syncing: ~p~n", [State#state.id, self(), Result]),
            probe(init, State2)
    end;
sync({timeout, _Replies}, State) ->
    %% _ = lager:info("timeout: trying all_sync"),
    all_sync(init, State);
sync({sync_complete, Peers}, State) ->
    %% Check that the remote peers are still trusted
    check_sync({init, Peers}, State);
sync(sync_failed, State) ->
    %% Asynchronous sync failed
    probe(init, State);
sync(Msg, State) ->
    common(Msg, State, sync).

-spec sync(_, fsm_from(), state()) -> {next_state, sync, state()}.
sync(Msg, From, State) ->
    common(Msg, From, State, sync).

check_sync({init, Peers}, State) ->
    case Peers of
        [] ->
            check_sync({quorum_met, []}, State);
        _ ->
            %% TODO: Add explicit check_sync message rather than abuse sync,
            %%       in case sync implementation is not pure or is expensive.
            State2 = send_peers(sync, Peers, State),
            {next_state, check_sync, State2}
    end;
check_sync({quorum_met, _Replies}, State) ->
    %% Asynchronous sync complete
    %% _ = lager:info("~p is trusted", [State#state.id]),
    probe(init, State#state{trust=true});
check_sync({timeout, _Replies}, State) ->
    %% _ = lager:info("check_sync: timeout"),
    probe(init, State);
check_sync(Msg, State) ->
    common(Msg, State, check_sync).

check_sync(Msg, From, State) ->
    common(Msg, From, State, check_sync).

all_sync(init, State) ->
    State2 = send_all(all_sync, all, State),
    {next_state, all_sync, State2};
all_sync({timeout, _Replies}, State) ->
    probe(init, State);
all_sync({quorum_met, Replies}, State) ->
    {Result, State2} = mod_sync(Replies, State),
    case Result of
        ok ->
            probe(init, State2#state{trust=true});
        async ->
            {next_state, all_sync, State2};
        {error, _} ->
            ?OUT("~p/~p: error when syncing: ~p~n", [State#state.id, self(), Result]),
            probe(init, State2)
    end;
all_sync({sync_complete, _Peers}, State) ->
    %% Asynchronous sync complete
    %% _ = lager:info("~p is trusted (all sync)", [State#state.id]),
    probe(init, State#state{trust=true});
all_sync(sync_failed, State) ->
    %% Asynchronous sync failed
    probe(init, State);
all_sync(Msg, State) ->
    common(Msg, State, all_sync).

all_sync(Msg, From, State) ->    
    common(Msg, From, State, all_sync).

-spec election(_, state()) -> next_state().
election(init, State) ->
    %% io:format("~p/~p: starting election~n", [self(), State#state.id]),
    ?OUT("~p: starting election~n", [State#state.id]),
    State2 = set_timer(2*?ENSEMBLE_TICK + random:uniform(2*?ENSEMBLE_TICK),
                       election_timeout, State),
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
election({'DOWN', _, _, _, _}, State) ->
    election(init, State);
election(Msg, State) ->
    common(Msg, State, election).

-spec election(_, fsm_from(), state()) -> {next_state, election, state()}.
election(Msg, From, State) ->
    common(Msg, From, State, election).

prefollow({init, Id, NextEpoch}, State) ->
    Prelim = {Id, NextEpoch},
    State2 = State#state{preliminary=Prelim},
    State3 = set_timer(?ENSEMBLE_TICK * 2, prefollow_timeout, State2),
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
    ?OUT("~p: prepare~n", [State#state.id]),
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
leading(init, State=#state{id=_Id}) ->
    ?OUT("~p: Leading~n", [_Id]),
    _ = lager:info("~p: Leading~n", [_Id]),
    leading(tick, State#state{alive=?ALIVE});
leading(tick, State) ->
    leader_tick(State);
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
leading(count_quorum, From, State=#state{fact=Fact, id=Id, members=Members}) ->
    NewFact = increment_sequence(Fact),
    State2 = local_commit(NewFact, State),
    {Future, State3} = blocking_send_all({commit, NewFact}, State2),
    Extra = case lists:member(Id, Members) of
                true  -> 1;
                false -> 0
            end,
    spawn_link(fun() ->
                       timer:sleep(1000),
                       Count = case wait_for_quorum(Future) of
                                   {quorum_met, Replies} ->
                                       %% io:format("met: ~p~n", [Replies]),
                                       length(Replies) + Extra;
                                   {timeout, _Replies} ->
                                       %% io:format("timeout~n"),
                                       Extra
                               end,
                       gen_fsm:reply(From, Count)
               end),
    {next_state, leading, State3};
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
    set_timer(?ENSEMBLE_TICK*2, follower_timeout, State).

-spec following(_, state()) -> next_state().
following(not_ready, State) ->
    following(init, State#state{ready=false});
following(init, State) ->
    ?OUT("~p: Following: ~p~n", [State#state.id, leader(State)]),
    State2 = reset_follower_timer(State),
    {next_state, following, State2};
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
    ?OUT("~p: follower_timeout from ~p~n", [State#state.id, leader(State)]),
    %% io:format("~p: follower_timeout from ~p~n", [State#state.id, leader(State)]),
    abandon(State#state{timer=undefined});
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

step_down(Next, State) ->
    ?OUT("~p: stepping down~n", [State#state.id]),
    State2 = cancel_timer(State),
    reset_workers(State),
    State3 = set_leader(undefined, State2),
    case Next of
        probe ->
            probe(init, State3);
        prepare ->
            prepare(init, State3);
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
common({sync, From}, State, StateName) ->
    State2 = case State#state.trust of
                 true ->
                     mod_sync_request(From, State);
                 false ->
                     reply(From, nack, State),
                     State
             end,
    {next_state, StateName, State2};
common({all_sync, From}, State, StateName) ->
    State2 = mod_sync_request(From, State),
    {next_state, StateName, State2};
common(tick, State, StateName) ->
    %% TODO: Fix it so we don't have errant tick messages
    {next_state, StateName, State};
common({forward, _From, _Msg}, State, StateName) ->
    {next_state, StateName, State};
common(backend_pong, State, StateName) ->
    State2 = State#state{alive=?ALIVE},
    {next_state, StateName, State2};
common(Msg, State, StateName) ->
    ?OUT("~p: ~s/ignoring: ~p~n", [State#state.id, StateName, Msg]),
    %% io:format("~p/~p: ~s/ignoring: ~p~n", [State#state.id, self(), StateName, Msg]),
    nack(Msg, State),
    {next_state, StateName, State}.

-spec common(_, fsm_from(), state(), StateName) -> {next_state, StateName, state()}.
common({force_state, {Epoch, Seq}}, From, State, StateName) ->
    State2 = set_epoch(Epoch, set_seq(Seq, State)),
    gen_fsm:reply(From, ok),
    {next_state, StateName, State2};
common(_Msg, From, State, StateName) ->
    ?OUT("~p: ~s/ignoring: ~p~n", [State#state.id, StateName, _Msg]),
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

leader_tick(State=#state{ensemble=Ensemble, id=Id}) ->
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
leading_kv({get, Key}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_get_fsm(Key, From, Self, State) end),
    {next_state, leading, State};
leading_kv(request_failed, _From, State) ->
    step_down(prepare, State);
leading_kv({local_get, Key}, From, State) ->
    State2 = do_local_get(From, Key, State),
    {next_state, leading, State2};
leading_kv({local_put, Key, Obj}, From, State) ->
    State2 = do_local_put(From, Key, Obj, State),
    {next_state, leading, State2};
leading_kv({put, Key, Fun, Args}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_put_fsm(Key, Fun, Args, From, Self, State) end),
    {next_state, leading, State};
leading_kv({overwrite, Key, Val}, From, State) ->
    Self = self(),
    async(Key, State, fun() -> do_overwrite_fsm(Key, Val, From, Self, State) end),
    {next_state, leading, State};
leading_kv(_, _From, _State) ->
    false.

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
following_kv(_, _State) ->
    false.

-spec following_kv(_,_,_) -> false | {next_state,following,state()}.
following_kv({get, _Key}=Msg, From, State) ->
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

do_put_fsm(Key, Fun, Args, From, Self, State) ->
    %% TODO: Timeout should be configurable per request
    Local = local_get(Self, Key, 30000),
    State2 = State#state{self=Self},
    case is_current(Local, State2) of
        local_timeout ->
            %% TODO: Should this send a request_failed?
            %% gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, unavailable);
        true ->
            do_modify_fsm(Key, Local, Fun, Args, From, State2);
        false ->
            case update_key(Key, Local, State2) of
                {ok, Current, _State3} ->
                    do_modify_fsm(Key, Current, Fun, Args, From, State2);
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
        {failed, _State2} ->
            gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, timeout)
    end.

-spec do_get_fsm(_,{_,_},pid(),state()) -> ok.
do_get_fsm(Key, From, Self, State0) ->
    State = State0#state{self=Self},
    Local = local_get(Self, Key, 30000),
    %% TODO: Allow get to return errors. Make consistent with riak_kv_vnode
    %% TODO: Returning local directly only works if we ensure leader lease
    case is_current(Local, State) of
        local_timeout ->
            %% TODO: Should this send a request_failed?
            %% gen_fsm:sync_send_event(Self, request_failed, infinity),
            send_reply(From, timeout);
        true ->
            send_reply(From, {ok, Local});
            %% case get_latest_obj(Key, Local, State2) of
            %%     {ok, Latest, State3} ->
            %%         {ok, Latest, State3};
            %%     {failed, State3} ->
            %%         {failed, State3}
            %% end;
        false ->
            ?OUT("~p :: not current~n", [Key]),
            case update_key(Key, Local, State) of
                {ok, Current, _State2} ->
                    send_reply(From, {ok, Current});
                {failed, _State2} ->
                    %% TODO: Should this be failed or unavailable?
                    gen_fsm:sync_send_event(Self, request_failed, infinity),
                    send_reply(From, failed)
            end
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

-spec is_current(maybe_obj(), state()) -> false | local_timeout | true.
is_current(timeout, _State) ->
    local_timeout;
is_current(notfound, _State) ->
    false;
is_current(Obj, State) ->
    Epoch = get_obj(epoch, Obj, State),
    Epoch =:= epoch(State).

-spec update_key(_,_,state()) -> {ok, obj(), state()} | {failed,state()}.
update_key(Key, Local, State) ->
    case get_latest_obj(Key, Local, State) of
        {ok, Latest, State2} ->
            case put_obj(Key, Latest, State2) of
                {ok, New, State3} ->
                    {ok, New, State3};
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
                {failed, State2} ->
                    {failed, State2}
            end;
        failed ->
            {precondition, State}
    end.

-spec get_latest_obj(_,_,state()) -> {ok, obj(), state()} | {failed, state()}.
get_latest_obj(Key, Local, State=#state{id=Id, members=Members}) ->
    Epoch = epoch(State),
    Peers = get_peers(Members, State),
    {Future, State2} = blocking_send_all({get, Key, Id, Epoch}, Peers, State),
    case wait_for_quorum(Future) of
        {quorum_met, Replies} ->
            Latest = latest_obj(Replies, Local, State),
            {ok, Latest, State2};
        {timeout, _Replies} ->
            {failed, State2}
    end.

put_obj(Key, Obj, State) ->
    Seq = obj_sequence(State),
    put_obj(Key, Obj, Seq, State).

-spec put_obj(_,obj(),state()) -> {ok, obj(), state()} | {failed,state()}.
put_obj(Key, Obj, Seq, State=#state{id=Id, members=Members, self=Self}) ->
    Epoch = epoch(State),
    Obj2 = increment_obj(Key, Obj, Seq, State),
    Peers = get_peers(Members, State),
    {Future, State2} = blocking_send_all({put, Key, Obj2, Id, Epoch}, Peers, State),
    %% TODO: local can be failed here, what to do?
    Local = local_put(Self, Key, Obj2, infinity),
    case wait_for_quorum(Future) of
        {quorum_met, _Replies} ->
            {ok, Local, State2};
        {timeout, _Replies} ->
            {failed, State2}
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
    ?OUT("~p: starting~n", [Id]),
    {A,B,C} = os:timestamp(),
    _ = random:seed(A + erlang:phash2(Id),
                    B + erlang:phash2(node()),
                    C),
    ETS = ets:new(x, [public, {read_concurrency, true}, {write_concurrency, true}]),
    State = #state{id=Id,
                   ensemble=Ensemble,
                   ets=ETS,
                   peers=[],
                   trust=false,
                   alive=?ALIVE,
                   mod=Mod},
    gen_fsm:send_event(self(), {init, Args}),
    riak_ensemble_peer_sup:register_peer(Ensemble, Id, self(), ETS),
    {ok, setup, State}.

setup({init, Args}, State0=#state{id=Id, ensemble=Ensemble, ets=ETS, mod=Mod}) ->
    NumWorkers = 1,
    Saved = reload_fact(Ensemble, Id),
    Workers = start_workers(NumWorkers, ETS),
    Members = compute_members(Saved#fact.views),
    State = State0#state{workers=list_to_tuple(Workers),
                         fact=Saved,
                         members=Members,
                         modstate=riak_ensemble_backend:start(Mod, Ensemble, Id, Args)},
    State2 = check_views(State),
    %% TODO: Why are we local commiting on startup?
    State3 = local_commit(State2#state.fact, State2),
    State4 = mod_trusted(State3),
    probe(init, State4).

-spec handle_event(_, atom(), state()) -> {next_state, atom(), state()}.
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
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% -spec handle_info(_, atom(), state()) -> next_state().
handle_info({'DOWN', _, _, Pid, _Reason}, StateName, State)
  when (Pid =:= State#state.trust_pid) ->
    State2 = mod_trusted(State),
    case State2#state.trust of
        true ->
            {next_state, StateName, State2};
        false when (StateName =:= leading) ->
            step_down(State2);
        false ->
            State3 = cancel_timer(State2),
            probe(init, State3)
    end;
handle_info({'DOWN', _, _, Pid, _Reason}, StateName, State) ->
    %% io:format("Pid down for: ~p~n", [Reason]),
    State2 = maybe_restart_worker(Pid, State),
    {next_state, StateName, State2};
handle_info(quorum_timeout, StateName, State) ->
    State2 = quorum_timeout(State),
    {next_state, StateName, State2};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

-spec terminate(_,_,_) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

-spec code_change(_, atom(), state(), _) -> {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec send_all(_,state()) -> state().
send_all(Msg, State) ->
    send_all(Msg, quorum, State).

send_all(Msg, Required, State=#state{members=Members}) ->
    send_peers(Msg, Members, Required, State).

send_peers(Msg, Members, State) ->
    Views = [Members],
    send_peers(Msg, Members, all, Views, State).

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
blocking_send_all(Msg, Peers, State=#state{id=Id}) ->
    Views = views(State),
    {Future, Awaiting} = riak_ensemble_msg:blocking_send_all(Msg, Id, Peers, Views),
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

%%%===================================================================
%%% Behaviour Interface
%%%===================================================================

mod_trusted(State=#state{mod=Mod, modstate=ModState}) ->
    case Mod:trusted(ModState) of
        {Trust, Pid} ->
            _ = monitor(process, Pid),
            State#state{trust=Trust, trust_pid=Pid};
        Trust ->
            State#state{trust=Trust}
    end.

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

mod_sync_request(From, State=#state{mod=Mod, modstate=ModState, id=Id}) ->
    ModState2 = Mod:sync_request({From, Id}, ModState),
    State#state{modstate=ModState2}.

-spec mod_sync([any()], state()) -> {ok, state()}       |
                                    {async, state()}    |
                                    {{error,_}, state()}.
mod_sync(Replies, State=#state{mod=Mod, modstate=ModState}) ->
    {Reply, ModState2} = Mod:sync(Replies, ModState),
    State2 = State#state{modstate=ModState2},
    {Reply, State2}.

-spec new_obj(_,_,_,_,state()) -> any().
new_obj(Epoch, Seq, Key, Value, #state{mod=Mod, modstate=_ModState}) ->
    Mod:new_obj(Epoch, Seq, Key, Value).

get_obj(X, Obj, Mod) when is_atom(Mod) ->
    riak_ensemble_backend:get_obj(Mod, X, Obj);
get_obj(X, Obj, #state{mod=Mod, modstate=_ModState}) ->
    riak_ensemble_backend:get_obj(Mod, X, Obj).

set_obj(X, Val, Obj, #state{mod=Mod, modstate=_ModState}) ->
    riak_ensemble_backend:set_obj(Mod, X, Val, Obj).

%%%===================================================================

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
