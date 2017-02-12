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

-module(lease_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOCK_M,     riak_ensemble_peer).
-define(MOCK_F,     check_epoch).
-define(MOCK_A,     3).
-define(MOCK_MFA,   {?MOCK_M, ?MOCK_F, ?MOCK_A}).

run_test_() ->
    {setup,
        fun() -> ens_test:setup_fun(?MOCK_M) end,
        fun ens_test:cleanup_fun/1,
        {timeout, 60, fun scenario/0}
    }.

scenario() ->
    ens_test:start(3),
    ?assertMatch({ok, _}, ens_test:kput(<<"test">>, <<"test">>)),

    %% Test with lease trusted
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),

    %% Test with lease not trusted
    ?assertEqual(ok, application:set_env(riak_ensemble, trust_lease, false)),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),

    %% Test with lease not trusted and followers intercepted to
    %% nack epoch check.
    meck:expect(?MOCK_M, ?MOCK_F, ?MOCK_A, false),
    ?assertEqual({error, timeout}, ens_test:kget(<<"test">>)),

    %% Test with lease trusted again
    ?assertEqual(ok, application:set_env(riak_ensemble, trust_lease, true)),
    %% Because of error above, leader may have changed. Wait until stable.
    ens_test:wait_stable(root),
    %% Read twice because leader change forces first read to rewrite, which
    %% ignores the lease entirely.
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),

    %% Test with simulated expired lease
    ?assertEqual(ok, application:set_env(riak_ensemble, follower_timeout, 1000)),
    ?assertEqual(ok, application:set_env(riak_ensemble, lease_duration, 0)),
    timer:sleep(1000),
    ?assertMatch({error, _}, ens_test:kget(<<"test">>)),

    %% Remove intercept and test that all is well
    meck:expect(?MOCK_M, ?MOCK_F, ens_hooks:pass_fun(?MOCK_MFA)),
    ens_test:wait_stable(root),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),

    ok.
