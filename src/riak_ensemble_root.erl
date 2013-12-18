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

-module(riak_ensemble_root).
-compile(export_all).
-include_lib("riak_ensemble_types.hrl").

%%%===================================================================
%%% API
%%%===================================================================

-spec update_ensemble(ensemble_id(), peer_id(), views(), vsn()) -> ok.
update_ensemble(Ensemble, Leader, Views, Vsn) ->
    ok = cast({update_ensemble, Ensemble, Leader, Views, Vsn}).

-spec set_ensemble(ensemble_id(), ensemble_info()) -> ok | {error, term()}.
set_ensemble(Ensemble, Info) ->
    case call({set_ensemble, Ensemble, Info}) of
        ok ->
            ok;
        Error ->
            {error, Error}
    end.

-spec join(node()) -> ok | {error, term()}.
join(Node) ->
    case call(Node, {join, node()}, 60000) of
        ok ->
            _ = lager:info("JOIN: success"),
            ok;
        Error ->
            {error, Error}
    end.

-spec gossip(vsn(), peer_id(), views()) -> ok.
gossip(Vsn, Leader, Views) ->
    ok = cast({gossip, Vsn, Leader, Views}).

%%%===================================================================

call(Cmd) ->
    call(node(), Cmd, 5000).

call(Node, Cmd, Timeout) ->
    Default = root_init(),
    Result = riak_ensemble_peer:kmodify(Node,
                                        root,
                                        cluster_state,
                                        {?MODULE, do_root_call, Cmd},
                                        Default,
                                        Timeout),
    case Result of
        {ok, _Obj} ->
            ok;
        Other ->
            Other
    end.

cast(Cmd) ->
    cast(node(), Cmd).

cast(Node, Cmd) ->
    Default = root_init(),
    spawn(fun() ->
                  riak_ensemble_peer:kmodify(Node,
                                             root,
                                             cluster_state,
                                             {?MODULE, do_root_cast, Cmd},
                                             Default,
                                             5000)
          end),
    ok.

do_root_call(Seq, State, Cmd) ->
    root_call(Cmd, Seq, State).

do_root_cast(Seq, State, Cmd) ->
    root_cast(Cmd, Seq, State).

%%%===================================================================

root_init() ->
    ets:lookup_element(em, cluster_state, 2).

%%%===================================================================

root_call({join, Node}, Vsn, State) ->
    _ = lager:info("join(Vsn): ~p :: ~p :: ~p", [Vsn, Node, riak_ensemble_state:members(State)]),
    case riak_ensemble_state:add_member(Vsn, Node, State) of
        {ok, State2} ->
            State2;
        error ->
            failed
    end;
root_call({set_pending, Vsn, Ensemble, Views}, _Vsn, State) ->
    case riak_ensemble_state:set_pending(Vsn, Ensemble, Views, State) of
        error ->
            failed;
        {ok, State2} ->
            State2
    end;
root_call({reset_pending, Vsn, Ensemble, OldVsn}, _Vsn, State) ->
    case riak_ensemble_state:reset_pending(Vsn, Ensemble, OldVsn, State) of
        error ->
            failed;
        {ok, State2} ->
            State2
    end;
root_call({set_ensemble, Ensemble, Info}, _Vsn, State) ->
    case riak_ensemble_state:set_ensemble(Ensemble, Info, State) of
        error ->
            failed;
        {ok, State2} ->
            State2
    end.

%%%===================================================================

root_cast({gossip, Vsn, Leader, Views}, _Vsn, State) ->
    Info = #ensemble_info{vsn=Vsn, leader=Leader, views=Views},
    case riak_ensemble_state:set_ensemble(root, Info, State) of
        {ok, State2} ->
            riak_ensemble_manager:gossip(State2),
            State2;
        error ->
            riak_ensemble_manager:gossip(State),
            failed
    end;
root_cast({update_ensemble, Ensemble, Leader, Views, Vsn}, _Vsn, State) ->
    case riak_ensemble_state:update_ensemble(Vsn, Ensemble, Leader, Views, State) of
        error ->
            failed;
        {ok, State2} ->
            State2
    end.
