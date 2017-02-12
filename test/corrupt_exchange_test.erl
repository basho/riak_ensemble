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

-module(corrupt_exchange_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOCK_M,     synctree).
-define(MOCK_F,     m_store).
-define(MOCK_A,     2).
-define(MOCK_MFA,   {?MOCK_M, ?MOCK_F, ?MOCK_A}).

run_test_() ->
    {setup,
        fun() -> ens_test:setup_fun(?MOCK_M) end,
        fun ens_test:cleanup_fun/1,
        {timeout, 90, fun scenario/0}
    }.

%% detect corruption
%% wait for all to be trusted again / corruption heals
%% wait for successful read
%% fail if we ever get notfound / etc

scenario() ->
    ens_test:start(3),
    meck:expect(?MOCK_M, ?MOCK_F, ens_hooks:corrupt_segment_all_fun(?MOCK_MFA)),
    % ?debugFmt("Leader: ~p", [riak_ensemble_manager:get_leader(root)]),
    ?assertMatch({ok, _}, ens_test:kput(<<"corrupt">>, <<"test">>)),
    % result depends on timing, so don't try to match it
    _ = ens_test:kget(<<"corrupt">>),
    % ?debugVal(ens_test:kget(<<"corrupt">>)),

    meck:expect(?MOCK_M, ?MOCK_F, ens_hooks:pass_fun(?MOCK_MFA)),
    ens_test:read_until(<<"corrupt">>),
    ok.
