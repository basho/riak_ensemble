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

-module(ens_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_TEST_TIMEOUT,   45).    % seconds
-define(DEFAULT_WAIT_TRIES,     100).
-define(DEFAULT_WAIT_DELAY,     200).   % milliseconds

-record(setup_data, {
    test_dir    :: string(),
    started     :: [atom()]
}).

run(Test) ->
    run(Test, ?DEFAULT_TEST_TIMEOUT).

run(Test, Timeout) ->
    {setup,
        fun setup_fun/0,
        fun cleanup_fun/1,
        {timeout, Timeout, Test}}.

setup_fun() ->
    TestDir = create_test_dir(),
    LogDir  = test_log_dir(),
    ConLog  = filename:join(LogDir, "console.log"),
    ErrLog  = filename:join(LogDir, "error.log"),
    CrashLog = filename:join(LogDir, "crash.log"),
    % Setting more logging stuff than is probably needed so we don't have
    % to revisit as the started applications change.
    error_logger:tty(false),
    application:load(sasl),
    application:set_env(sasl, errlog_type, error),
    application:load(lager),
    application:set_env(lager, crash_log, CrashLog),
    application:set_env(lager, handlers, [
        {lager_console_backend, error},
        {lager_file_backend, [{file, ErrLog}, {level, error}]},
        {lager_file_backend, [{file, ConLog}, {level, debug}]}
    ]),
    application:load(riak_ensemble),
    application:set_env(riak_ensemble, data_root, TestDir),
    {ok, Started} = application:ensure_all_started(riak_ensemble),
    #setup_data{test_dir = TestDir, started = Started}.

setup_fun(Mocked) ->
    meck:new(Mocked, [passthrough]),
    {Mocked, setup_fun()}.

cleanup_fun(#setup_data{test_dir = TestDir, started = Started}) ->
    lists:foreach(fun application:stop/1, lists:reverse(Started)),
    delete_test_dir(TestDir);
cleanup_fun({Mocked, SetupData}) ->
    cleanup_fun(SetupData),
    meck:unload(Mocked).

start() ->
    Node = node(),
    riak_ensemble_manager:enable(),
    [{root, Node}] = riak_ensemble_manager:get_members(root),
    ens_test:wait_stable(root),
    ok.

start(N) ->
    start(),
    expand(N).

expand(N) ->
    NewMembers = [{X, node()} || X <- lists:seq(2, N)],
    Changes = [{add, Member} || Member <- NewMembers],
    Pid = riak_ensemble_manager:get_leader_pid(root),
    riak_ensemble_peer:update_members(Pid, Changes, 5000),
    ens_test:wait_stable(root),

    Members = [{root, node()} | NewMembers],
    ens_test:wait_members(root, Members),
    ens_test:wait_stable(root),
    ok.

wait_stable(Ensemble) ->
    case check_stable(Ensemble) of
        true ->
            ok;
        false ->
            wait_stable(Ensemble)
    end.

check_stable(Ensemble) ->
    riak_ensemble_manager:check_quorum(Ensemble, 1000) andalso
        riak_ensemble_peer:stable_views(Ensemble, 1000) == {ok, true}.

wait_members(Ensemble, Expected) ->
    Members = riak_ensemble_manager:get_members(Ensemble),
    case lists:all(fun(Expect) -> lists:member(Expect, Members) end, Expected) of
        true ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_members(Ensemble, Expected)
    end.

kput(Key, Val) ->
    riak_ensemble_client:kover(root, Key, Val, 5000).

kget(Key) ->
    riak_ensemble_client:kget(root, Key, 5000).

kget(Key, Opts) ->
    riak_ensemble_client:kget(node(), root, Key, 5000, Opts).

read_until(Key) ->
    case ens_test:kget(Key) of
        {ok, Obj} ->
            Value = riak_ensemble_basic_backend:obj_value(Obj),
            ?assertNotEqual(notfound, Value);
        {error, _} ->
            timer:sleep(?DEFAULT_WAIT_DELAY),
            read_until(Key)
    end.

%% Utility function used to construct test predicates.
%%
%% Retries the function `Fun' until it returns `true', or until the maximum
%% number of retries is reached.

wait_until(Fun) when is_function(Fun) ->
    wait_until(Fun, ?DEFAULT_WAIT_TRIES, ?DEFAULT_WAIT_DELAY).

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    case Fun() of
        true ->
            ok;
        Ret when Retry == 1 ->
            {fail, Ret};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry - 1, Delay)
    end.

-spec create_test_dir() -> string() | no_return().
%% Creates a new, empty, uniquely-named directory for testing.
create_test_dir() ->
    string:strip(?cmd("mktemp -d /tmp/" ?MODULE_STRING ".XXXXXXX"), both, $\n).

-spec delete_test_dir(Dir :: string()) -> ok | no_return().
%% Deletes a test directory fully, whether or not it exists.
delete_test_dir(Dir) ->
    ?assertCmd("rm -rf " ++ Dir).

-spec test_log_dir() -> string() | no_return().
%% Get the path to a suitable log directory for testing.
test_log_dir() ->
    % This module's obviously loaded, so this should be fast and
    % reliable even if cover-compiled.
    {_Mod, _Bin, Beam} = code:get_object_code(?MODULE),
    filename:join(filename:dirname(filename:dirname(Beam)), "log").
