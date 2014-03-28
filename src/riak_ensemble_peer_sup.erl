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
-module(riak_ensemble_peer_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_peer/4, stop_peer/2, peers/0]).
-export([get_peer_pid/2, register_peer/4]).

-include_lib("riak_ensemble_types.hrl").
-define(ETS, riak_ensemble_peers).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Owned by riak_ensemble_peer_sup to ensure table has lifetime that
    %% is greater than or equal to all riak_ensemble_peers.
    ?ETS = ets:new(?ETS, [named_table, public,
                          {read_concurrency, true},
                          {write_concurrency, true}]),
    {ok, {{one_for_one, 5, 10}, []}}.

-spec start_peer(module(), ensemble_id(), peer_id(), [any()]) -> pid().
start_peer(Mod, Ensemble, Id, Args) ->
    Ref = peer_ref(Mod, Ensemble, Id, Args),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child;
              {error, already_present} ->
                  ok = supervisor:delete_child(?MODULE, {Ensemble, Id}),
                  start_peer(Mod, Ensemble, Id, Args)
          end,
    Pid.

-spec stop_peer(ensemble_id(), peer_id()) -> ok.
stop_peer(Ensemble, Id) ->
    _ = supervisor:terminate_child(?MODULE, {Ensemble, Id}),
    ok = unregister_peer(Ensemble, Id),
    _ = supervisor:delete_child(?MODULE, {Ensemble, Id}),
    ok.

-spec peers() -> [{{ensemble_id(), peer_id()}, pid()}].
peers() ->
    Children = supervisor:which_children(?MODULE),
    [{Id,Pid} || {Id, Pid, worker, _} <- Children,
                 is_pid(Pid)].

-spec get_peer_pid(ensemble_id(), peer_id()) -> pid() | undefined.
get_peer_pid(Ensemble, Id) ->
    try
        ets:lookup_element(?ETS, {pid, {Ensemble, Id}}, 2)
    catch
        _:_ ->
            undefined
    end.

-spec register_peer(ensemble_id(), peer_id(), pid(), ets:tid()) -> ok.
register_peer(Ensemble, Id, Pid, ETS) ->
    true = ets:insert(?ETS, [{{pid, {Ensemble, Id}}, Pid},
                             {{ets, {Ensemble, Id}}, ETS}]),
    ok.

%% @private
unregister_peer(Ensemble, Id) ->
    true = ets:delete(?ETS, {pid, {Ensemble, Id}}),
    true = ets:delete(?ETS, {ets, {Ensemble, Id}}),
    ok.

%% @private
peer_ref(Mod, Ensemble, Id, Args) ->
    {{Ensemble, Id},
     {riak_ensemble_peer, start_link, [Mod, Ensemble, Id, Args]},
     permanent, 5000, worker, [riak_ensemble_peer]}.
