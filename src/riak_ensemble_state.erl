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
-module(riak_ensemble_state).

-include_lib("riak_ensemble_types.hrl").

-export([new/1, is_state/1]).
-export([add_member/3,
         del_member/3]).
-export([set_ensemble/3,
         update_ensemble/5,
         set_pending/4]).
-export([enable/1, enabled/1]).
-export([merge/2]).
-export([id/1, members/1, ensembles/1, pending/1]).

-type ensembles() :: orddict(ensemble_id(), ensemble_info()).
-type pending()   :: orddict(ensemble_id(), {vsn(), views()}).

-record(cluster_state, {id          :: any(),
                        enabled     :: boolean(),
                        members     :: {vsn(), ordsets(node())},
                        ensembles   :: ensembles(),
                        pending     :: pending()
                       }).

-type state() :: #cluster_state{}.
-export_type([state/0]).

-define(STATE, #cluster_state).

%%%===================================================================

-spec new(term()) -> state().
new(Id) ->
    ?STATE{id = Id,
           enabled = false,
           members = {vsn0(), ordsets:new()},
           ensembles = orddict:new(),
           pending = orddict:new()}.

-spec is_state(term()) -> boolean().
is_state(?STATE{}) ->
    true;
is_state(_) ->
    false.

-spec enable(state()) -> {ok, state()} | error.
enable(State=?STATE{enabled=false}) ->
    State2 = State?STATE{enabled=true},
    {ok, State2};
enable(?STATE{enabled=true}) ->
    error.

-spec enabled(state()) -> boolean().
enabled(?STATE{enabled=Enabled}) ->
    Enabled.

-spec id(state()) -> term().
id(?STATE{id=Id}) ->
    Id.

-spec members(state()) -> ordsets(node()).
members(?STATE{members={_Vsn, Nodes}}) ->
    Nodes.

-spec ensembles(state()) -> ensembles().
ensembles(?STATE{ensembles=Ensembles}) ->
    Ensembles.

-spec pending(state()) -> pending().
pending(?STATE{pending=Pending}) ->
    %% [{Id, Views} || {Id, {_Vsn, Views}} || <- Pending].
    Pending.

-spec add_member(vsn(), node(), state()) -> {ok, state()} | error.
add_member(Vsn, Node, State=?STATE{members={CurVsn, Nodes}}) ->
    case newer(CurVsn, Vsn) of
        true ->
            Nodes2 = ordsets:add_element(Node, Nodes),
            State2 = State?STATE{members={Vsn, Nodes2}},
            {ok, State2};
        false ->
            error
    end.

-spec del_member(vsn(), node(), state()) -> {ok, state()} | error.
del_member(Vsn, Node, State=?STATE{members={CurVsn, Nodes}}) ->
    case newer(CurVsn, Vsn) of
        true ->
            Nodes2 = ordsets:del_element(Node, Nodes),
            State2 = State?STATE{members={Vsn, Nodes2}},
            {ok, State2};
        false ->
            error
    end.

-spec set_ensemble(ensemble_id(), ensemble_info(), state()) -> {ok, state()} |
                                                               error.
set_ensemble(Ensemble, Info, State=?STATE{ensembles=Ensembles}) ->
    Vsn = Info#ensemble_info.vsn,
    CurVsn = case orddict:find(Ensemble, Ensembles) of
                 {ok, CurInfo} ->
                     CurInfo#ensemble_info.vsn;
                 error ->
                     undefined
             end,
    case newer(CurVsn, Vsn) of
        true ->
            Ensembles2 = orddict:store(Ensemble, Info, Ensembles),
            State2 = State?STATE{ensembles=Ensembles2},
            {ok, State2};
        false ->
            error
    end.

-spec update_ensemble(vsn(), ensemble_id(), peer_id(), views(), state())
                     -> {ok, state()} | error.
update_ensemble(Vsn, Ensemble, Leader, Views, State=?STATE{ensembles=Ensembles}) ->
    case orddict:find(Ensemble, Ensembles) of
        {ok, CurInfo} ->
            CurVsn = CurInfo#ensemble_info.vsn,
            case newer(CurVsn, Vsn) of
                true ->
                    NewInfo = CurInfo#ensemble_info{vsn=Vsn, leader=Leader, views=Views},
                    Ensembles2 = orddict:store(Ensemble, NewInfo, Ensembles),
                    State2 = State?STATE{ensembles=Ensembles2},
                    {ok, State2};
                false ->
                    error
            end;
        error ->
            error
    end.

-spec set_pending(vsn(), ensemble_id(), views(), state()) -> {ok, state()} |
                                                             error.
set_pending(Vsn, Ensemble, Views, State=?STATE{pending=Pending}) ->
    CurVsn = case orddict:find(Ensemble, Pending) of
                 {ok, {CV, _}} ->
                     CV;
                 error ->
                     undefined
             end,
    case newer(CurVsn, Vsn) of
        true ->
            Pending2 = orddict:store(Ensemble, {Vsn, Views}, Pending),
            State2 = State?STATE{pending=Pending2},
            {ok, State2};
        false ->
            error
    end.

-spec merge(state(), state()) -> state().
merge(A, B) when A?STATE.enabled and (A?STATE.id =/= B?STATE.id) ->
    _ = lager:warning("Ignoring cluster state with different id"),
    A;
merge(A=?STATE{members=MembersA, ensembles=EnsemblesA, pending=PendingA},
      _=?STATE{members=MembersB, ensembles=EnsemblesB, pending=PendingB}) ->
    A?STATE{members=merge_members(MembersA, MembersB),
            ensembles=merge_ensembles(EnsemblesA, EnsemblesB),
            pending=merge_pending(PendingA, PendingB)}.

%%%===================================================================

merge_members(A={VsnA, _}, B={VsnB, _}) ->
    case newer(VsnA, VsnB) of
        true ->
            B;
        false ->
            A
    end.

merge_ensembles(EnsemblesA, EnsemblesB) ->
    orddict:merge(fun merge_ensemble/3, EnsemblesA, EnsemblesB).

merge_ensemble(_, InfoA, InfoB) ->
    case newer(InfoA#ensemble_info.vsn, InfoB#ensemble_info.vsn) of
        true ->
            InfoB;
        false ->
            InfoA
    end.

merge_pending(PendingA, PendingB) ->
    orddict:merge(fun merge_pending_views/3, PendingA, PendingB).

merge_pending_views(_, A={VsnA, _}, B={VsnB, _}) ->
    case newer(VsnA, VsnB) of
        true ->
            B;
        false ->
            A
    end.

newer(VsnA, VsnB) ->
    ensure_vsn(VsnB) > ensure_vsn(VsnA).

ensure_vsn(undefined) ->
    vsn0();
ensure_vsn(Vsn={_,_}) ->
    Vsn.

vsn0() ->
    {-1,0}.
