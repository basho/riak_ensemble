%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% Long-lived worker process used by {@link riak_ensemble_peer} to handle
%% asynchronous work such as K/V gets and puts. The peer maintains a pool
%% of workers and routes requests to workers based on object key, ensuring
%% that operations that affect the same key are serialized on the same worker.
%% Thus, workers also enable partitioned queuing. These workers can also be
%% paused and resumed. This is used by the peer when necessary to enforce
%% necessary barriers. For example, a leader will pause all workers before
%% attempting to change ensemble membership, then resume workers after. This
%% prevents workers from issuing requests during a changing ownership set,
%% since those requests will likely be rejected.
%%
%% Note: pausing/resuming is best-effort. A worker currently involved
%% in a request will not pause until after completing/failing the request.
%% Thus, pause/resume is not designed to provide guarantees for correctness,
%% but rather as a tool for optimization (eg. to prevent issues requests
%% that will likely fail because some other correctness mechanism rejects them).

-module(riak_ensemble_peer_worker).
-export([start/1, pause_workers/2, unpause_workers/2]).
-export([loop/1, maybe_pause/1]). %% For internal use

%%===================================================================

-spec start(ets:tid()) -> {ok, pid()}.
start(ETS) ->
    Parent = self(),
    Pid = spawn(fun() ->
                        init(Parent, ETS)
                end),
    {ok, Pid}.

-spec pause_workers([pid()], ets:tid()) -> ok.
pause_workers(_Workers, ETS) ->
    ets:insert(ETS, {paused, true}),
    ok.

-spec unpause_workers([pid()], ets:tid()) -> ok.
unpause_workers(Workers, ETS) ->
    ets:delete(ETS, paused),
    _ = [Pid ! unpause || Pid <- Workers],
    ok.

%%===================================================================

init(Parent, ETS) ->
    monitor(process, Parent),
    loop(ETS).

loop(ETS) ->
    receive
        {async, Fun} ->
            maybe_pause(ETS),
            Fun();
        {'DOWN', _, _, _, _} ->
            exit(normal);
        _ ->
            ok
    end,
    ?MODULE:loop(ETS).

maybe_pause(ETS) ->
    case ets:lookup(ETS, paused) of
        [{paused, true}] ->
            pause(ETS);
        [] ->
            ok
    end.

pause(ETS) ->
    receive
        unpause ->
            ok;
        {'DOWN', _, _, _, _} ->
            exit(normal)
    after 5000 ->
            ok
    end,
    ?MODULE:maybe_pause(ETS).
