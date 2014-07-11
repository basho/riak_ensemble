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
-module(synctree_ets).

-export([new/1,
         fetch/3,
         exists/2,
         store/3,
         store/2]).

-record(?MODULE, {ets :: ets:tid()}).
-define(STATE, #?MODULE).
-type state() :: ?STATE{}.

-spec new(_) -> state().
new(_) ->
    T = ets:new(?MODULE, []),
    ?STATE{ets=T}.

-spec fetch(_, _, state()) -> {ok, _}.
fetch(Key, Default, ?STATE{ets=T}) ->
    case ets:lookup(T, Key) of
        [] ->
            {ok, Default};
        [{_, Value}] ->
            {ok, Value}
    end.

-spec exists(_, state()) -> boolean().
exists(Key, ?STATE{ets=T}) ->
    ets:member(T, Key).

-spec store(_, _, state()) -> state().
store(Key, Val, State=?STATE{ets=T}) ->
    _ = ets:insert(T, {Key, Val}),
    State.

-spec store([{_,_}], state()) -> state().
store(Updates, State=?STATE{ets=T}) ->
    %% _ = ets:insert(T, Updates),
    Inserts = [case Update of
                   {put, Key, Val} ->
                       {Key, Val};
                   {delete, Key} ->
                       {Key, deleted}
               end || Update <- Updates],
    _ = ets:insert(T, Inserts),
    _ = [ets:delete_object(T, {Key, deleted}) || {delete, Key} <- Updates],
    State.
