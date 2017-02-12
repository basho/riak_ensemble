%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2017 Basho Technologies, Inc.
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

%% TODO: This file should NOT be public!
%% KV riak_kv_ensemble_... use it, but should be broken of that habit.

-type ensemble_id() :: term().
-type peer_id() :: {term(), node()}.
-type leader_id() :: undefined | peer_id().
-type fixme() :: any().
-type views() :: [[peer_id()]].
-type peer_change() :: term(). %% FIXME
-type change_error() :: already_member | not_member.
-type std_reply() :: timeout | failed | unavailable | nack | {ok, term()}.
-type maybe_pid() :: pid() | undefined.
-type peer_pids() :: [{peer_id(), maybe_pid()}].
-type peer_reply() :: {peer_id(), term()}.
-type epoch() :: integer().
-type seq() :: integer().
-type vsn() :: {epoch(), seq()}.
-type peer_info() :: nodedown | undefined | {any(), boolean(), epoch()}.

-type orddict(Key,Val) :: [{Key, Val}].
-type ordsets(Val) :: [Val].

-record(ensemble_info, {
    vsn                                 :: vsn(),
    mod = riak_ensemble_basic_backend   :: module(),
    args    = []                        :: [any()],
    leader                              :: leader_id(),
    views                               :: [[peer_id()]],
    seq                                 :: undefined | {integer(), integer()}
}).
-type ensemble_info() :: #ensemble_info{}.

%% -type ensemble_info() :: {leader_id(), [peer_id()], {integer(), integer()}, module(), [any()]}.

-define(ENSEMBLE_TICK, riak_ensemble_config:tick()).
