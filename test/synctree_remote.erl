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

-module(synctree_remote).

-include_lib("eunit/include/eunit.hrl").

-define(DBG(Fmt, Args), ok).
% -define(DBG(Fmt, Args), io:format(user, "~b ", [?LINE])).
% -define(DBG(Fmt, Args), ?debugFmt(Fmt, Args)).

run_test_() ->
    Timeout = 60,
    {timeout, Timeout, fun test_remote/0}.

test_remote() ->
    Num = 10,
    Diff = 4,

    %% Spawn new process for remote tree
    Other = spawn(
        fun() ->
            B1 = synctree_pure:build(Num - Diff),
            message_loop(B1, 0, 0)
        end),

    %% Build local tree
    A1 = synctree_pure:build(Num),
    Local = synctree:direct_exchange(A1),

    %% Compare with remote tree through message passing
    Remote = fun
        (exchange_get, {L, B}) ->
            receive {get_bucket, B, X} ->
                X
            after 0 ->
                Other ! {get_bucket, self(), L, B},
                receive
                    {get_bucket, B, X} ->
                        X
                end
            end;
        (start_exchange_level, {Level, Buckets}) ->
            Other ! {start_exchange_level, self(), Level, Buckets},
            receive
                {start_exchange_level, X} ->
                    X
            end
    end,

    KeyDiff = synctree:compare(synctree:height(A1), Local, Remote),
    Expected = synctree_pure:expected_diff(Num, Diff),
    ?assertEqual(Expected, KeyDiff),

    %% Signal spawned process to print stats and exit
    Other ! done,
    ok.

message_loop(Tree, Msgs, Bytes) ->
    receive
        {get_bucket, From, L, B} ->
            ?DBG("Not streamed: ~p/~p", [L, B]),
            Size = send_bucket(From, L, B, Tree),
            message_loop(Tree, Msgs + 1, Bytes + Size);
        {start_exchange_level, From, Level, Buckets} ->
            ?DBG("Start streaming for ~p/~p", [Level, Buckets]),
            From ! {start_exchange_level, ok},
            _ = [send_bucket(From, Level, B, Tree) || B <- Buckets],
            message_loop(Tree, Msgs, Bytes);
        done ->
            ?DBG("Exchanged ~b messages, ~b bytes", [Msgs, Bytes])
    end.

send_bucket(From, L, B, Tree) ->
    Reply = synctree:exchange_get(L, B, Tree),
    From ! {get_bucket, B, Reply},
    Size = byte_size(term_to_binary(Reply)),
    Size.
