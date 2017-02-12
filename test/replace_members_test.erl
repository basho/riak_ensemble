%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014-2017 Basho Technologies, Inc.
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

-module(replace_members_test).

-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 60). %% seconds

-define(DBG(Msg),   ok).
% -define(DBG(Msg),   io:format(user, "~b ", [?LINE])).
% -define(DBG(Msg),   ?debugMsg(Msg)).

run_test_() ->
    ens_test:run(fun scenario/0, ?TIMEOUT).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),
    VRet = ens_test:kput(<<"test">>, <<"test">>),
    ?assertMatch({ok, _}, VRet),
    ?assertEqual(VRet, ens_test:kget(<<"test">>)),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    Adds = [{I, node()} || I <- [4,5,6]],
    Dels = [{I, node()} || I <- [root, 2, 3]],
    Changes = [{add, Member} || Member <- Adds] ++
              [{del, Member} || Member <- Dels],
    ?DBG("Replacing Root Members: root/2/3 -> 4/5/6"),
    ?assertEqual(ok, riak_ensemble_peer:update_members(Pid, Changes, 10000)),

    ?DBG("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Adds),
    ens_test:wait_stable(root),
    ?DBG("Done waiting for root ensemble to stabilize"),

    %% This fails because we sync metadata trees, but not the data itself.
    %% Riak KV hands off data before the transition so this shouldn't happen in
    %% practice.
    ?DBG("Performing failing get"),
    ?assertEqual({error, failed}, ens_test:kget(<<"test">>)),

    %% The failure above causes the leader to step down.  We need to wait for a
    %% new election or the call to update_members/3 below will fail.
    ?DBG("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Adds),
    ens_test:wait_stable(root),
    ?DBG("Done waiting for root ensemble to stabilize"),

    ?DBG("Replacing root members with original members"),
    Changes2 = [{add, M} || M <- Dels] ++ [{del, M} || M <- Adds],
    Leader = riak_ensemble_manager:get_leader_pid(root),
    ?assertEqual(ok, riak_ensemble_peer:update_members(Leader, Changes2, 10000)),

    ?DBG("Waiting for root ensemble to stabilize"),
    ens_test:wait_members(root, Dels),
    ens_test:wait_stable(root),
    ?DBG("Done waiting for root ensemble to stabilize"),

    %% The following get succeeds because the data is still stored on root/2/3
    ?DBG("Performing successful get"),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)).
