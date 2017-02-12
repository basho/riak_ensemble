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

%% Various pure tests
-module(ensemble_tests_pure).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {timeout, 60, fun test_monotonic_time/0}.

test_monotonic_time() ->
    MsFun = riak_ensemble_clock:monotonic_ms_fun(),
    Ms1 = MsFun(),
    timer:sleep(1000),
    Ms2 = MsFun(),
    % monotonic time values can be negative
    ?assert(Ms2 > Ms1),
    ?assert(erlang:abs(Ms2 - Ms1) >= 1000).
