%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2017 Basho Technologies, Inc.
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

-module(riak_ensemble_test).

-export([
    maybe_drop/2,
    setup/0
]).

-ifdef(TEST).

-define(ETS_TEST, riak_ensemble_test).

setup() ->
    case ets:info(?ETS_TEST, named_table) of
        undefined ->
            _ = ets:new(?ETS_TEST, [
                public, named_table,
                {read_concurrency, true},
                {write_concurrency, true}
            ]),
            ok;
        _ ->
            ok
    end.

maybe_drop(Id, PeerId) ->
    case catch ets:member(?ETS_TEST, {drop, {Id, PeerId}}) of
        true ->
            true;
        _ ->
            false
    end.

-else.  % not TEST

setup() ->
    ok.

maybe_drop(_, _) ->
    false.

-endif. % TEST
