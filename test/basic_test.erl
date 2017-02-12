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

-module(basic_test).

-include_lib("eunit/include/eunit.hrl").

-define(DBG(Msg),   ok).
% -define(DBG(Msg),   io:format(user, "~b ", [?LINE])).
% -define(DBG(Msg),   ?debugMsg(Msg)).

run_test_() ->
    ens_test:run(fun scenario/0).

scenario() ->
    ens_test:start(3),
    ens_test:wait_stable(root),
    ?assertMatch({ok, _}, ens_test:kput(<<"test">>, <<"test">>)),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    ens_test:kget(cluster_state),
    Pid = riak_ensemble_manager:get_leader_pid(root),
    ?DBG("Suspending leader"),
    erlang:suspend_process(Pid),
    ens_test:wait_stable(root),
    ?DBG("Performing get"),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)),
    ?DBG("Resuming leader"),
    erlang:resume_process(Pid),
    ?DBG("Performing get"),
    ?assertMatch({ok, _}, ens_test:kget(<<"test">>)).
