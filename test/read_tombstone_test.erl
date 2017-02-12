%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2017 Basho Technologies, Inc.
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

%% This test specifically targets an optimization in riak_ensemble that is
%% intended to avoid writing tombstones when possible on reads that return
%% notfound. Normally reads that return notfound require a tombstone to be
%% written in case there's a partial write somewhere in the ensemble that
%% hasn't been read yet. If we wait an extra, say, 1ms though, then it's
%% very likely we will see replies from every peer in the ensemble, in
%% which case we can definitively see whether any partial writes exist or
%% not, and potentially avoid the need to write a tombstone.
-module(read_tombstone_test).

-include_lib("eunit/include/eunit.hrl").

-define(DBG(Msg),   ok).
% -define(DBG(Msg),   io:format(user, "~b ", [?LINE])).
% -define(DBG(Msg),   ?debugMsg(Msg)).

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),
    application:set_env(riak_ensemble, notfound_read_delay, 3000),

    Peers = riak_ensemble_manager:get_members(root),
    Leader = riak_ensemble_manager:get_leader(root),
    [Follow1, Follow2] = Peers -- [Leader],

    ?DBG("Running kget on a nonexistent key"),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),

    ?DBG("Testing that no peers have tombstones"),
    ?assert(is_notfound(Leader, <<"test">>)),
    ?assert(is_notfound(Follow1, <<"test">>)),
    ?assert(is_notfound(Follow2, <<"test">>)),

    ?DBG("Running kget with one member suspended"),
    application:set_env(riak_ensemble, notfound_read_delay, 0),
    Peer2Pid = riak_ensemble_manager:get_peer_pid(root, Follow2),
    erlang:suspend_process(Peer2Pid),
    ?assertMatch({ok, _}, ens_test:kget(<<"test2">>)),
    erlang:resume_process(Peer2Pid),

    ?DBG("Testing that active peers have tombstones"),
    ?assertNot(is_notfound(Leader, <<"test2">>)),
    ?assertNot(is_notfound(Follow1, <<"test2">>)),

    ok.

%% This can be used to check for tombstones, because if a key
%% has a tombstone, we'll see an object returned that wraps
%% the 'notfound' value rather than just the atom 'notfound' alone.
is_notfound(Member, Key) ->
    Pid = riak_ensemble_manager:get_peer_pid(root, Member),
    Res = riak_ensemble_peer:debug_local_get(Pid, Key),
    Res =:= notfound.
