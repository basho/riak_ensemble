-module(riak_ensemble_state).

-include_lib("riak_ensemble_types.hrl").

-export([new/1, is_state/1]).
-export([add_member/3,
         del_member/3]).
-export([set_ensemble/3,
         update_ensemble/5,
         set_pending/4,
         reset_pending/4]).
-export([enable/1, enabled/1]).
-export([merge/2]).
-export([id/1, members/1, ensembles/1, pending/1]).

-type ensembles() :: orddict:orddict(ensemble_id(), ensemble_info()).
-type pending()   :: orddict:orddict(ensemble_id(), {vsn(), views()}).

-record(cluster_state, {id          :: any(),
                        enabled     :: boolean(),
                        members     :: {vsn(), ordsets:ordsets(node())},
                        ensembles   :: ensembles(),
                        pending     :: pending()
                       }).

-define(STATE, #cluster_state).

new(Id) ->
    ?STATE{id = Id,
           enabled = false,
           members = {vsn0(), ordsets:new()},
           ensembles = orddict:new(),
           pending = orddict:new()}.

is_state(?STATE{}) ->
    true;
is_state(_) ->
    false.

enable(State=?STATE{enabled=false}) ->
    State2 = State?STATE{enabled=true},
    {ok, State2};
enable(?STATE{enabled=true}) ->
    error.

enabled(?STATE{enabled=Enabled}) ->
    Enabled.

id(?STATE{id=Id}) ->
    Id.

members(?STATE{members={_Vsn, Nodes}}) ->
    Nodes.

ensembles(?STATE{ensembles=Ensembles}) ->
    Ensembles.

pending(?STATE{pending=Pending}) ->
    %% [{Id, Views} || {Id, {_Vsn, Views}} || <- Pending].
    Pending.

add_member(Vsn, Node, State=?STATE{members={CurVsn, Nodes}}) ->
    case newer(CurVsn, Vsn) of
        true ->
            Nodes2 = ordsets:add_element(Node, Nodes),
            State2 = State?STATE{members={Vsn, Nodes2}},
            {ok, State2};
        false ->
            error
    end.

del_member(Vsn, Node, State=?STATE{members={CurVsn, Nodes}}) ->
    case newer(CurVsn, Vsn) of
        true ->
            Nodes2 = ordsets:del_element(Node, Nodes),
            State2 = State?STATE{members={Vsn, Nodes2}},
            {ok, State2};
        false ->
            error
    end.

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

reset_pending(Vsn, Ensemble, OldVsn, State=?STATE{pending=Pending}) ->
    CurVsn = case orddict:find(Ensemble, Pending) of
                 {ok, {CV, _}} ->
                     CV;
                 error ->
                     undefined
             end,
    case newer(CurVsn, Vsn) andalso (CurVsn =:= OldVsn) of
        true ->
            %% Current verison the same
            Pending2 = orddict:store(Ensemble, {CurVsn, []}, Pending),
            State2 = State?STATE{pending=Pending2},
            {ok, State2};
        false ->
            error
    end.

merge(A, B) when A?STATE.enabled and (A?STATE.id =/= B?STATE.id) ->
    _ = lager:warning("Ignoring cluster state with different id"),
    A;
merge(A=?STATE{members=MembersA, ensembles=EnsemblesA, pending=PendingA},
      _=?STATE{members=MembersB, ensembles=EnsemblesB, pending=PendingB}) ->
    A?STATE{members=merge_members(MembersA, MembersB),
            ensembles=merge_ensembles(EnsemblesA, EnsemblesB),
            pending=merge_pending(PendingA, PendingB)}.

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
