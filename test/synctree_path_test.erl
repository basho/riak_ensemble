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

-module(synctree_path_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOCK_M,     riak_ensemble_basic_backend).
-define(MOCK_F,     synctree_path).
-define(MOCK_A,     2).
-define(MOCK_MFA,   {?MOCK_M, ?MOCK_F, ?MOCK_A}).

run_test_() ->
    {setup,
        fun() -> ens_test:setup_fun(?MOCK_M) end,
        fun ens_test:cleanup_fun/1,
        {timeout, 60, fun scenario/0}
    }.

scenario() ->
    meck:expect(?MOCK_M, ?MOCK_F, ens_hooks:synctree_path_shared_fun(?MOCK_MFA)),
    ens_test:start(3),
    ens_test:wait_stable(root),
    ?assertMatch({ok, _}, ens_test:kput(<<"test">>, <<"test">>)),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    ens_test:kget(cluster_state),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    erlang:resume_process(Pid),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)).
