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
-module(riak_ensemble_clock).
-on_load(init/0).
-export([monotonic_time/0, monotonic_time_ms/0]).

monotonic_time() ->
    erlang:nif_error({error, not_loaded}).

monotonic_time_ms() ->
    erlang:nif_error({error, not_loaded}).

init() ->
    case code:priv_dir(riak_ensemble) of
        {error, bad_name} ->
            case code:which(?MODULE) of
                Filename when is_list(Filename) ->
                    SoName = filename:join([filename:dirname(Filename),"../priv", "riak_ensemble"]);
                _ ->
                    SoName = filename:join("../priv", "riak_ensemble")
            end;
        Dir ->
            SoName = filename:join(Dir, "riak_ensemble")
    end,
    erlang:load_nif(SoName, 0).
