%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ensemble_config).
-compile(export_all).
-include_lib("riak_ensemble_types.hrl").

%% @doc
%% The primary ensemble tick that determines the rate at which an elected
%% leader attempts to refresh its lease.
tick() ->
    get_env(ensemble_tick, 500).

%% @doc
%% The leader lease duration. Should be greater than the leader tick to give
%% the leader time to refresh before expiration, but lower than the follower
%% timeout.
lease() ->
    get_env(lease_duration, tick() * 3 div 2).

%% @doc
%% This setting determines if leader leases are trusted or not. Trusting the
%% lease allows a leader to reply to reads without contacting remote peers
%% as long as its lease has not yet expired.
trust_lease() ->
    get_env(trust_lease, true).

%% @doc
%% The follower timeout determines how long a follower waits to hear from
%% the leader before abandoning it.
follower_timeout() ->
    get_env(follower_timeout, lease() * 4).

%% @doc
%% The election timeout used for randomized election.
election_timeout() ->
    Timeout = follower_timeout(),
    Timeout + riak_ensemble_util:random_uniform(Timeout).

%% @doc
%% The prefollow timeout determines how long a peer waits to hear from the
%% preliminary leader before abandoning it.
prefollow_timeout() ->
    tick() * 2.

%% @doc
%% The pending timeout determines how long a pending peer waits in the pending
%% state to hear from an existing leader.
pending_timeout() ->
    tick() * 10.

%% @doc
%% The amount of time between probe attempts.
probe_delay() ->
    1000.

%% @doc The internal timeout used by peer worker FSMs when performing gets.
local_get_timeout() ->
    get_env(peer_get_timeout, 60000).

%% @doc The internal timeout used by peer worker FSMs when performing puts.
local_put_timeout() ->
    get_env(peer_put_timeout, 60000).

%% @doc
%% The number of leader ticks that can go by without hearing from the ensemble
%% backend.
alive_ticks() ->
    get_env(alive_tokens, 2).

%% @doc The number of peer workers/FSM processes used by the leader.
peer_workers() ->
    get_env(peer_workers, 1).

%% @doc
%% The operation delay used by {@link riak_ensemble_storage} to coalesce
%% multiple local operations into a single disk oepration.
storage_delay() ->
    get_env(storage_delay, 50).

%% @doc
%% The periodic tick at which {@link riak_ensemble_storage} flushes operations
%% to disk even if there are no explicit sync requests.
storage_tick() ->
    get_env(storage_tick, 5000).

%% @doc
%% When true, synctrees are not trusted after a peer restart, requiring an
%% exchange with a trusted majority to become trusted. This provides the
%% strongest guarantees against byzantine faults.
tree_validation() ->
    get_env(tree_validation, true).

%% @doc
%% Determines if remote synctree updates are performed synchronously.
%% When true, tree updates are performed before replying to the user.
synchronous_tree_updates() ->
    get_env(synchronous_tree_updates, false).

%% @doc
%% Determines how long to wait for additional responses to come in on
%% certain reads that may return notfound. If we receive responses from
%% every peer in the ensemble, we do not need to write a tombstone for
%% the notfound key. If set to zero, no additional time will be waited,
%% but it is still possible we may be able to skip writing the tombstone
%% if all the responses arrive within a very close window of time.
%% The default of 1ms should be enough to avoid most tombstones that
%% would otherwise be created, but a higher value may be specified in
%% cases where unpredictable latencies necessitate it.
notfound_read_delay() ->
    get_env(notfound_read_delay, 1).

get_env(Key, Default) ->
    application:get_env(riak_ensemble, Key, Default).
