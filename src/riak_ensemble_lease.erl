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

%% @doc
%% This module is used by {@link riak_ensemble_peer} to keep track of
%% an established leader lease. The leader is responsible for periodically
%% refreshing its lease, otherwise the lease will timeout.
%%
%% Using a time-based lease in a distributed system is not without issue.
%% This module does its best to address these concerns as follows:
%%
%% 1. This module uses Erlang based timeouts to trigger lease expiration.
%%    Erlang uses time correction to attempt to occur for clock issues,
%%    as discussed here:
%%      http://www.erlang.org/doc/apps/erts/time_correction.html
%%
%% 2. In addition to Erlang time, this module also double checks the
%%    lease against the OS monotonic clock. The monotonic clock is
%%    not affected by the user/NTP changing the system clock, and
%%    is designed to always move forward (although, virtualization
%%    sometimes affects this guarantee).
%%
%% Likewise, riak_ensemble is designed such that the lease and leader refresh
%% are much smaller than the follower timeout. All of these factors, along
%% with riak_ensemble being designed to maintain strong leadership (unlike
%% other systems such as Raft) make the use of leader leases safe in practice.
%% As a reminder, Google is also known to use leader leases it its paxos
%% implementation as discussed in their "Paxos Made Live" paper.
%%
%% Of course, users that do not trust leader leases can always set the
%% trust_lease application variable to false, causing riak_ensemble to ignore
%% leader leases and always perform full quorum operations.
%%

-module(riak_ensemble_lease).

-export([start_link/0,
         check_lease/1,
         lease/2,
         unlease/1]).

%% internal exports
-export([init/2, loop/2]).

-type lease_ref() :: {pid(), ets:tid()}.
-export_type([lease_ref/0]).

%%%===================================================================

-spec start_link() -> {ok, lease_ref()}.
start_link() ->
    Ref = make_ref(),
    spawn_link(?MODULE, init, [self(), Ref]),
    receive
        {Ref, Reply} ->
            Reply
    end.

-spec check_lease(lease_ref()) -> boolean().
check_lease({_, T}) ->
    case ets:lookup_element(T, lease, 2) of
        undefined ->
            false;
        Until ->
            case riak_ensemble_clock:monotonic_time_ms() of
                {ok, Time} when Time < Until ->
                    true;
                _ ->
                    false
            end
    end.

-spec lease(lease_ref(), timeout()) -> ok.
lease({Pid,_}, Duration) ->
    ok = call(Pid, {lease, Duration}).

-spec unlease(lease_ref()) -> ok.
unlease({Pid,_}) ->
    ok = call(Pid, unlease).

%%%===================================================================

init(Parent, Ref) ->
    T = ets:new(?MODULE, [protected, set, {read_concurrency, true}]),
    ets:insert(T, {lease, undefined}),
    Reply = {ok, {self(), T}},
    Parent ! {Ref, Reply},
    loop(T, infinity).

%%%===================================================================

loop(T, Timeout) ->
    receive
        {{lease, Duration}, From} ->
            case riak_ensemble_clock:monotonic_time_ms() of
                {ok, Time} ->
                    ets:insert(T, {lease, Time + Duration});
                error ->
                    ets:insert(T, {lease, undefined})
            end,
            reply(From, ok),
            ?MODULE:loop(T, Duration);
        {unlease, From} ->
            ets:insert(T, {lease, undefined}),
            reply(From, ok),
            ?MODULE:loop(T, infinity)
    after Timeout ->
            ets:insert(T, {lease, undefined}),
            ?MODULE:loop(T, infinity)
    end.

%%%===================================================================

call(Pid, Msg) ->
    MRef = monitor(process, Pid),
    From = {self(), MRef},
    Pid ! {Msg, From},
    receive
        {MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    end.

reply({Pid, Ref}, Reply) ->
    Pid ! {Ref, Reply},
    ok.
