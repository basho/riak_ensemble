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
-module(riak_ensemble_util).
-export([replace_file/2,
         read_file/1,
         sha/1,
         md5/1,
         orddict_delta/2,
         shuffle/1,
         cast_unreliable/2]).

%%===================================================================

-type delta() :: {any(), any()} | {'$none', any()} | {any(), '$none'}.
-type orddict() :: orddict:orddict().

%%===================================================================

-spec replace_file(file:filename(), iodata()) -> ok | {error, term()}.
replace_file(FN, Data) ->
    TmpFN = FN ++ ".tmp",
    {ok, FH} = file:open(TmpFN, [write, raw]),
    try
        ok = file:write(FH, Data),
        ok = file:sync(FH),
        ok = file:close(FH),
        ok = file:rename(TmpFN, FN),
        {ok, Contents} = read_file(FN),
        true = (Contents == iolist_to_binary(Data)),
        ok
    catch _:Err ->
            {error, Err}
    end.

%%===================================================================

%% @doc Similar to {@link file:read_file/1} but uses raw file I/O
-spec read_file(file:filename()) -> {ok, binary()} | {error, _}.
read_file(FName) ->
    case file:open(FName, [read, raw, binary]) of
        {ok, FD} ->
            Result = read_file(FD, []),
            ok = file:close(FD),
            case Result of
                {ok, IOList} ->
                    {ok, iolist_to_binary(IOList)};
                {error, _}=Err ->
                    Err
            end;
        {error,_}=Err ->
            Err
    end.

-spec read_file(file:fd(), [binary()]) -> {ok, [binary()]} | {error,_}.
read_file(FD, Acc) ->
    case file:read(FD, 4096) of
        {ok, Data} ->
            read_file(FD, [Data|Acc]);
        eof ->
            {ok, lists:reverse(Acc)};
        {error, _}=Err ->
            Err
    end.

%%===================================================================

-ifndef(old_hash).

-spec sha(iolist() | binary()) -> binary().
sha(Bin) ->
    crypto:hash(sha, Bin).

-spec md5(iolist() | binary()) -> binary().
md5(Bin) ->
    crypto:hash(md5, Bin).

-else.

-spec sha(iolist() | binary()) -> binary().
sha(Bin) ->
    crypto:sha(Bin).

-spec md5(iolist() | binary()) -> binary().
md5(Bin) ->
    crypto:md5(Bin).

-endif.

%%===================================================================

%% @doc
%% Compare two orddicts, returning a list of differences between
%% them. Differences come in three forms:
%%   {Val, '$none'} :: key is in `D1' but not in `D2'
%%   {'$none', Val} :: key is in `D2' but not in `D1'
%%   {Val1, Val2}   :: key is in both orddicts but values differ
%%
-spec orddict_delta(orddict(), orddict()) -> [{any(), delta()}].
orddict_delta(D1, D2) ->
    orddict_delta(D1, D2, []).

-spec orddict_delta(orddict(), orddict(), [{any(), delta()}]) -> [{any(), delta()}].
orddict_delta([{K1,V1}|D1], [{K2,_}=E2|D2], Acc) when K1 < K2 ->
    Acc2 = [{K1,{V1,'$none'}} | Acc],
    orddict_delta(D1, [E2|D2], Acc2);
orddict_delta([{K1,_}=E1|D1], [{K2,V2}|D2], Acc) when K1 > K2 ->
    Acc2 = [{K2,{'$none',V2}} | Acc],
    orddict_delta([E1|D1], D2, Acc2);
orddict_delta([{K1,V1}|D1], [{_K2,V2}|D2], Acc) -> %K1 == K2
    case V1 of
        V2 ->
            orddict_delta(D1, D2, Acc);
        _ ->
            Acc2 = [{K1,{V1,V2}} | Acc],
            orddict_delta(D1, D2, Acc2)
    end;
orddict_delta([], [{K2,V2}|D2], Acc) ->
    Acc2 = [{K2,{'$none',V2}} | Acc],
    orddict_delta([], D2, Acc2);
orddict_delta([{K1,V1}|D1], [], Acc) ->
    Acc2 = [{K1,{V1,'$none'}} | Acc],
    orddict_delta(D1, [], Acc2);
orddict_delta([], [], Acc) ->
    lists:reverse(Acc).


-spec shuffle([T]) -> [T].
shuffle([]) ->
    [];
shuffle(L=[_]) ->
    L;
shuffle(L) ->
    Range = length(L),
    L2 = [{rand:uniform(Range), E} || E <- L],
    [E || {_, E} <- lists:sort(L2)].

%% Copied from riak_core_send_msg.erl
cast_unreliable(Dest, Request) ->
    bang_unreliable(Dest, {'$gen_cast', Request}).

bang_unreliable(Dest, Msg) ->
    catch erlang:send(Dest, Msg, [noconnect, nosuspend]),
    Msg.
