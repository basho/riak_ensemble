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

%% @doc Interface to the monotonic time NIF.
%%
%% Prefer `erlang:monotonic_time(milli_seconds)' on OTP-18+.
%%
%% If the current Erlang runtime supports monotonic time, the NIF will NOT
%% be loaded, and calls to {@link monotonic_time_ms/0} will raise errors,
%% so it should not be called directly.
%% Instead, {@link monotonic_ms_fun/0} should be used to obtain the
%% appropriate function regardless of version.
%%
-module(riak_ensemble_clock).

%% Public API
-export([monotonic_ms_fun/0]).

%% This shouldn't be called directly, but has to be exported to connect
%% the NIF wiring.
-export([monotonic_time_ms/0]).

-ifdef(BASHO_CHECK).
%% Dialyzer and XRef won't recognize 'on_load' as using the function and
%% will complain about it.
-export([init_nif_lib/0]).
-endif.
-on_load(init_nif_lib/0).

-define(APPLICATION, riak_ensemble).

%% ===================================================================
%% Public API
%% ===================================================================

-spec monotonic_ms_fun() -> fun(() -> integer()).
%% @doc Returns a function that returns monotonic time in milliseconds.
monotonic_ms_fun() ->
    ErlMod = erlang,    % keep xref and dialyzer happy
    case erlang:is_builtin(ErlMod, monotonic_time, 0) of
        true ->
            fun() -> ErlMod:monotonic_time(milli_seconds) end;
        _ ->
            fun ?MODULE:monotonic_time_ms/0
    end.

%% ===================================================================
%% Private API
%% ===================================================================

-spec monotonic_time_ms() -> integer().
%% @doc It is not advisable to call this function directly.
%% @see monotonic_ms_fun/0
monotonic_time_ms() ->
    erlang:nif_error({error, not_loaded}).

%% ===================================================================
%% NIF initialization
%% ===================================================================

init_nif_lib() ->
    case erlang:is_builtin(erlang, monotonic_time, 0) of
        true ->
            ok; % Don't need the NIF library.
        _ ->
            SoDir = case code:priv_dir(?APPLICATION) of
                {error, bad_name} ->
                    ADir =  case code:which(?MODULE) of
                        Beam when is_list(Beam) ->
                            filename:dirname(filename:dirname(Beam));
                        _ ->
                            {ok, CWD} = file:get_cwd(),
                            % This is almost certainly wrong, but it'll end
                            % up equivalent to "../priv".
                            filename:dirname(CWD)
                    end,
                    filename:join(ADir, "priv");
                PDir ->
                    PDir
            end,
            AppEnv = application:get_all_env(?APPLICATION),
            erlang:load_nif(filename:join(SoDir, ?APPLICATION), AppEnv)
    end.
