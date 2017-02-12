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

-module(drop_write_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOCK_M,     riak_ensemble_basic_backend).
-define(MOCK_F,     put).
-define(MOCK_A,     4).
-define(MOCK_MFA,   {?MOCK_M, ?MOCK_F, ?MOCK_A}).

run_test_() ->
    {setup,
        fun() -> ens_test:setup_fun(?MOCK_M) end,
        fun ens_test:cleanup_fun/1,
        {timeout, 60, fun scenario/0}
    }.

scenario() ->
    meck:expect(?MOCK_M, ?MOCK_F, ens_hooks:drop_put_fun(?MOCK_MFA)),
    ens_test:start(5),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    ?assertMatch({ok, _}, ens_test:kput(<<"drop">>, <<"test">>)),
    ?assertMatch({ok, _}, ens_test:kget(<<"drop">>)),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),
    erlang:resume_process(Pid),
    ens_test:read_until(<<"drop">>),
    ok.
