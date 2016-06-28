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

%% TODO: Before PR. Add module edoc + doc functions

-module(riak_ensemble_msg).

-export([send_all/4,
         send_all/5,
         blocking_send_all/4,
         blocking_send_all/5,
         blocking_send_all/6,
         wait_for_quorum/1,
         cast_all/3,
         handle_reply/4,
         quorum_timeout/1,
         reply/3]).

-include_lib("riak_ensemble_types.hrl").

%% -define(OUT(Fmt,Args), io:format(Fmt,Args)).
-define(OUT(Fmt,Args), ok).

%%%===================================================================

-type required()   :: quorum | other | all | all_or_quorum.

-record(msgstate, {awaiting = undefined :: 'undefined' | reqid(),
                   timer    = undefined :: 'undefined' | reference(),
                   required = quorum    :: required(),
                   id       :: peer_id(),
                   views    = [] :: views(),
                   replies  = [] :: [{peer_id(), any()}]}).

-opaque msg_state() :: #msgstate{}.
-export_type([msg_state/0]).

-type timer() :: reference().
-type reqid() :: reference().
-type msg() :: term().
-type peer_nack()  :: {peer_id(), nack}.
-type msg_from()   :: {riak_ensemble_msg,pid(),reference()}.
-type from()       :: {pid(),reference()}.
-type maybe_from() :: undefined | from().

-type future() :: undefined | pid().
-export_type([future/0, msg_from/0]).

-type extra_check() :: undefined | fun(([peer_reply()]) -> boolean()).
-export_type([extra_check/0]).

-record(collect, {replies  :: [peer_reply()], 
                  parent   :: maybe_from(),
                  id       :: peer_id(),
                  views    :: views(),
                  required :: required(),
                  extra    :: extra_check(),
                  reqid    :: reqid()
                 }).
-type collect() :: #collect{}.

%%%===================================================================

-spec send_all(msg(), peer_id(), peer_pids(), views()) -> msg_state().
send_all(Msg, Id, Peers, Views) ->
    send_all(Msg, Id, Peers, Views, quorum).

-spec send_all(msg(), peer_id(), peer_pids(), views(), required()) -> msg_state().
send_all(_Msg, Id, _Peers=[{Id,_}], _Views, _Required) ->
    ?OUT("~p: self-sending~n", [Id]),
    gen_fsm:send_event(self(), {quorum_met, []}),
    #msgstate{awaiting=undefined, timer=undefined, replies=[], id=Id};
send_all(Msg, Id, Peers, Views, Required) ->
    ?OUT("~p/~p: sending to ~p: ~p~n", [Id, self(), Peers, Msg]),
    {ReqId, Request} = make_request(Msg),
    _ = [maybe_send_request(Id, Peer, ReqId, Request) || Peer={PeerId,_} <- Peers,
                                                         PeerId =/= Id],
    Timer = send_after(?ENSEMBLE_TICK, self(), quorum_timeout),
    #msgstate{awaiting=ReqId, timer=Timer, replies=[], id=Id, views=Views,
              required=Required}.

%%%===================================================================

-spec cast_all(msg(), peer_id(), peer_pids()) -> ok.
cast_all(Msg, Id, Peers) ->
    ?OUT("~p/~p: casting to ~p: ~p~n", [Id, self(), Peers, Msg]),
    _ = [maybe_send_cast(Id, Peer, Msg) || Peer={PeerId,_} <- Peers,
                                           PeerId =/= Id],
    ok.

%%%===================================================================

-spec maybe_send_request(peer_id(), {peer_id(), maybe_pid()}, reqid(), msg()) -> ok.
-ifdef(TEST).

maybe_send_request(Id, {PeerId, PeerPid}, ReqId, Event) ->
    case riak_ensemble_test:maybe_drop(Id, PeerId) of
        true ->
            %% TODO: Consider nacking instead
            io:format("Dropping ~p -> ~p~n", [Id, PeerId]),
            ok;
        false ->
            send_request({PeerId, PeerPid}, ReqId, Event)
    end.

-else.

maybe_send_request(_Id, {PeerId, PeerPid}, ReqId, Event) ->
    send_request({PeerId, PeerPid}, ReqId, Event).

-endif.

%%%===================================================================

-spec send_request({peer_id(), maybe_pid()}, reqid(), msg()) -> ok.
send_request({PeerId, PeerPid}, ReqId, Event) ->
    case PeerPid of
        undefined ->
            ?OUT("~p: Sending offline nack for ~p~n", [self(), PeerId]),
            From = make_from(self(), ReqId),
            reply(From, PeerId, nack);
        _ ->
            ?OUT("~p: Sending to ~p: ~p~n", [self(), PeerId, Event]),
            gen_fsm:send_event(PeerPid, Event)
    end.

%%%===================================================================

-spec maybe_send_cast(peer_id(), {peer_id(), maybe_pid()}, msg()) -> ok.
-ifdef(TEST).

maybe_send_cast(Id, {PeerId, PeerPid}, Event) ->
    case riak_ensemble_test:maybe_drop(Id, PeerId) of
        true ->
            %% TODO: Consider nacking instead
            io:format("Dropping ~p -> ~p~n", [Id, PeerId]),
            ok;
        false ->
            send_cast({PeerId, PeerPid}, Event)
    end.

-else.

maybe_send_cast(_Id, {PeerId, PeerPid}, Event) ->
    send_cast({PeerId, PeerPid}, Event).

-endif.

%%%===================================================================

-spec send_cast({peer_id(), maybe_pid()}, msg()) -> ok.
send_cast({_PeerId, PeerPid}, Event) ->
    case PeerPid of
        undefined ->
            ok;
        _ ->
            ?OUT("~p: Sending to ~p: ~p~n", [self(), _PeerId, Event]),
            gen_fsm:send_event(PeerPid, Event)
    end.

%%%===================================================================

-spec reply(msg_from(), peer_id(), any()) -> ok.
reply({riak_ensemble_msg, Sender, ReqId}, Id, Reply) ->
    gen_fsm:send_all_state_event(Sender, {reply, ReqId, Id, Reply}).

%%%===================================================================

-spec blocking_send_all(msg(), peer_id(), peer_pids(), views())
                       -> {future(), msg_state()}.
blocking_send_all(Msg, Id, Peers, Views) ->
    blocking_send_all(Msg, Id, Peers, Views, quorum, undefined).

-spec blocking_send_all(msg(), peer_id(), peer_pids(), views(), required())
                       -> {future(), msg_state()}.
blocking_send_all(Msg, Id, Peers, Views, Required) when Required =/= undefined ->
    blocking_send_all(Msg, Id, Peers, Views, Required, undefined).

-spec blocking_send_all(msg(), peer_id(), peer_pids(), views(),
                        required(), extra_check()) -> {future(), msg_state()}.
blocking_send_all(Msg, Id, Peers, Views, Required, Extra) when Required =/= undefined ->
    ?OUT("~p: blocking_send_all to ~p: ~p~n", [Id, Peers, Msg]),
    MsgState = #msgstate{awaiting=undefined, timer=undefined, replies=[],
                         views=Views, id=Id, required=Required},
    Future = case Peers of
                 [{Id,_}] ->
                     undefined;
                 _ ->
                     spawn_link(fun() ->
                                        collector(Msg, Peers, Extra, MsgState)
                                end)
             end,
    {Future, MsgState}.

-spec collector(msg(), peer_pids(), extra_check(), msg_state()) -> ok.
collector(Msg, Peers, Extra, #msgstate{id=Id, views=Views, required=Required}) ->
    {ReqId, Request} = make_request(Msg),
    _ = [maybe_send_request(Id, Peer, ReqId, Request) || Peer={PeerId,_} <- Peers,
                                                         PeerId =/= Id],
    collect_replies(#collect{replies=[],
                             parent=undefined,
                             id=Id,
                             views=Views,
                             required=Required,
                             extra=Extra,
                             reqid=ReqId}).

-spec collect_replies(collect()) -> ok.
collect_replies(Collect=#collect{replies=Replies, reqid=ReqId}) ->
    receive
        {'$gen_all_state_event', Event} ->
            {reply, ReqId, Peer, Reply} = Event,
            Replies2 = [{Peer, Reply}|Replies],
            check_enough(Collect#collect{replies=Replies2});
        {waiting, From, Ref} when is_pid(From), is_reference(Ref) ->
            Parent = {From, Ref},
            check_enough(Collect#collect{parent=Parent})
    after ?ENSEMBLE_TICK ->
            maybe_timeout(Collect)
    end.

maybe_timeout(#collect{parent=undefined, replies=Replies, id=Id,
                       views=Views, required=Required, extra=Extra}) ->
    receive {waiting, From, Ref} ->
            case quorum_met(Replies, Id, Views, Required, Extra) of
                true ->
                    From ! {Ref, ok, Replies},
                    ok;
                _ ->
                    collect_timeout(Replies, {From, Ref})
            end
    end;
maybe_timeout(#collect{replies=Replies, parent=Parent}) ->
    collect_timeout(Replies, Parent).

-spec collect_timeout([peer_reply()], from()) -> ok.
collect_timeout(Replies, {From, Ref}) ->
    From ! {Ref, timeout, Replies},
    ok.

-spec check_enough(collect()) -> ok.
check_enough(Collect=#collect{parent=undefined}) ->
    collect_replies(Collect);
check_enough(Collect=#collect{id=Id,
                              replies=Replies,
                              parent={From,Ref}=Parent,
                              views=Views,
                              required=Required,
                              extra=Extra}) ->
    case quorum_met(Replies, Id, Views, Required, Extra) of
        true when Required =:= all_or_quorum ->
            %% If we've hit a quorum with all_or_quorum required, then
            %% we need to wait some additional length of time and see
            %% if we get replies from all.
            try_collect_all(Collect);
        true ->
            From ! {Ref, ok, Replies},
            ok;
        nack ->
            collect_timeout(Replies, Parent);
        false ->
            collect_replies(Collect)
    end.

-spec try_collect_all(#collect{}) -> _.
try_collect_all(Collect=#collect{reqid=ReqId}) ->
    Timeout = riak_ensemble_config:notfound_read_delay(),
    erlang:send_after(Timeout, self(), {try_collect_all_timeout, ReqId}),
    try_collect_all_impl(Collect).

try_collect_all_impl(Collect=#collect{id=Id,
                                      replies=Replies0,
                                      parent={From, Ref},
                                      reqid=ReqId,
                                      views=Views}) ->
    receive
        {'$gen_all_state_event', Event} ->
            {reply, ReqId, Peer, Reply} = Event,
            Replies = [{Peer, Reply}|Replies0],
            case quorum_met(Replies, Id, Views, all) of
                true ->
                    %% At this point we should be guaranteed to have already
                    %% gotten a parent that we can reply to:
                    ?OUT("Met quorum with Event ~p Replies ~p", [Event, Replies, Views]),
                    From ! {Ref, ok, Replies};
                false ->
                    ?OUT("Got additional message ~p but quorum still not met", [Event]),
                    try_collect_all(Collect#collect{replies=Replies});
                nack ->
                    %% Since we're waiting for all, we may see a nack from even
                    %% just a single negative response. But, we already know we
                    %% have a quorum of positive replies, so we can still send
                    %% back an 'ok' response with the replies we've gotten.
                    ?OUT("Got a nack! Returning replies so far: ~p", [Replies]),
                    From ! {Ref, ok, Replies}
            end;
        {try_collect_all_timeout, ReqId} ->
            ?OUT("Timed out waiting for try_collect_all", []),
            From ! {Ref, ok, Replies0}
    end.

-spec wait_for_quorum(future()) -> {quorum_met, [peer_reply()]} |
                                   {timeout, [peer_reply()]}.
wait_for_quorum(undefined) ->
    {quorum_met, []};
wait_for_quorum(Pid) ->
    Ref = make_ref(),
    Pid ! {waiting, self(), Ref},
    receive
        {Ref, ok, Replies} ->
            {Valid, _Nacks} = find_valid(Replies),
            {quorum_met, Valid};
        {Ref, timeout, Replies} ->
            {timeout, Replies}
    end.

%%%===================================================================

-spec handle_reply(any(), peer_id(), any(), msg_state()) -> msg_state().
handle_reply(ReqId, Peer, Reply, MsgState=#msgstate{awaiting=Awaiting}) ->
    case ReqId == Awaiting of
        true ->
            add_reply(Peer, Reply, MsgState);
        false ->
            MsgState
    end.

-spec add_reply(peer_id(), any(), msg_state()) -> msg_state().
add_reply(Peer, Reply, MsgState=#msgstate{timer=Timer}) ->
    Replies = [{Peer, Reply} | MsgState#msgstate.replies],
    case quorum_met(Replies, MsgState) of
        true ->
            cancel_timer(Timer),
            {Valid, _Nacks} = find_valid(Replies),
            gen_fsm:send_event(self(), {quorum_met, Valid}),
            MsgState#msgstate{replies=[], awaiting=undefined, timer=undefined};
        false ->
            MsgState#msgstate{replies=Replies};
        nack ->
            cancel_timer(Timer),
            quorum_timeout(MsgState#msgstate{replies=Replies})
    end.

-spec quorum_timeout(msg_state()) -> msg_state().
quorum_timeout(#msgstate{replies=Replies}) ->
    {Valid, _Nacks} = find_valid(Replies),
    gen_fsm:send_event(self(), {timeout, Valid}),
    #msgstate{awaiting=undefined, timer=undefined, replies=[]}.

%%%===================================================================

-spec quorum_met([peer_reply()], msg_state()) -> true | false | nack.
quorum_met(Replies, #msgstate{id=Id, views=Views, required=Required}) ->
    quorum_met(Replies, Id, Views, Required).

-spec quorum_met([peer_reply()], peer_id(), views(), required()) -> true | false | nack.
quorum_met(Replies, Id, Views, Required) ->
    quorum_met(Replies, Id, Views, Required, undefined).

-spec quorum_met([peer_reply()], peer_id(),
                 views(), required(), extra_check()) -> true | false | nack.
quorum_met(Replies, _Id, [], _Required, Extra) ->
    case Extra of
        undefined ->
            true;
        _ ->
            Extra(Replies)
    end;
quorum_met(Replies, Id, [Members|Views], Required, Extra) ->
    Filtered = [Reply || Reply={Peer,_} <- Replies,
                         lists:member(Peer, Members)],
    {Valid, Nacks} = find_valid(Filtered),
    Quorum = case Required of
                 quorum ->
                     length(Members) div 2 + 1;
                 all_or_quorum ->
                     length(Members) div 2 + 1;
                 other ->
                     length(Members) div 2 + 1;
                 all ->
                     length(Members)
             end,
    Heard = case (Required =/= other) andalso lists:member(Id, Members) of
                true ->
                    length(Valid) + 1;
                false ->
                    length(Valid)
            end,
    if Heard >= Quorum ->
            ?OUT("~p//~nM: ~p~nV: ~p~nN: ~p: view-met~n", [Id, Members, Valid, Nacks]),
            quorum_met(Replies, Id, Views, Required, Extra);
       length(Nacks) >= Quorum ->
            ?OUT("~p//~nM: ~p~nV: ~p~nN: ~p: nack~n", [Id, Members, Valid, Nacks]),
            nack;
       (Heard + length(Nacks)) =:= length(Members) ->
            ?OUT("~p//~nM: ~p~nV: ~p~nN: ~p: nack~n", [Id, Members, Valid, Nacks]),
            nack;
       true ->
            ?OUT("~p//~nM: ~p~nV: ~p~nN: ~p: false~n", [Id, Members, Valid, Nacks]),
            false
    end.

-spec find_valid([peer_reply()]) -> {[peer_reply()], [peer_nack()]}.
find_valid(Replies) ->
    {Valid, Nacks} = lists:partition(fun({_, nack}) ->
                                             false;
                                        (_) ->
                                             true
                                     end, Replies),
    {Valid, Nacks}.

%%%===================================================================

-spec make_request(msg()) -> {reqid(), tuple()}.
make_request(Msg) ->
    ReqId = make_ref(),
    From = make_from(self(), ReqId),
    Request = if is_tuple(Msg) ->
                      erlang:append_element(Msg, From);
                 true ->
                      {Msg, From}
              end,
    {ReqId, Request}.

-spec make_from(pid(), reqid()) -> msg_from().
make_from(Pid, ReqId) ->
    {riak_ensemble_msg, Pid, ReqId}.

%%%===================================================================

-spec send_after(timeout(), pid(), msg()) -> timer().
send_after(Time, Dest, Msg) ->
    erlang:send_after(Time, Dest, Msg).

-spec cancel_timer(timer()) -> ok.
cancel_timer(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                quorum_timeout -> ok
            after
                0 -> ok
            end;
        _ ->
            ok
    end.
