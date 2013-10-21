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
-export([start_peer/5, stop_peer/2, peers/0]).

-include("riak_ensemble_types.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

-spec start_peer(module(), ensemble_id(), peer_id(), views(), [any()]) -> pid().
start_peer(Mod, Ensemble, Id, Views, Args) ->
    %% TODO: Are these really views?
    Ref = peer_ref(Mod, Ensemble, Id, Views, Args),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child;
              {error, already_present} ->
                  ok = supervisor:delete_child(?MODULE, {Ensemble, Id}),
                  start_peer(Mod, Ensemble, Id, Views, Args)
          end,
    Pid.

-spec stop_peer(ensemble_id(), peer_id()) -> ok.
stop_peer(Ensemble, Id) ->
    _ = supervisor:terminate_child(?MODULE, {Ensemble, Id}),
    _ = supervisor:delete_child(?MODULE, {Ensemble, Id}),
    ok.

-spec peers() -> [{{ensemble_id(), peer_id()}, pid()}].
peers() ->
    Children = supervisor:which_children(?MODULE),
    [{Id,Pid} || {Id, Pid, worker, _} <- Children,
                 is_pid(Pid)].

%% @private
peer_ref(Mod, Ensemble, Id, Views, Args) ->
    {{Ensemble, Id},
     {riak_ensemble_peer, start_link, [Mod, Ensemble, Id, Views, Args]},
     permanent, 5000, worker, [riak_ensemble_peer]}.
