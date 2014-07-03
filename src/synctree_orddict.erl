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
-module(synctree_orddict).

-export([new/1,
         fetch/3,
         exists/2,
         store/3,
         store/2]).

-record(?MODULE, {data :: [{_,_}]}).
-define(STATE, #?MODULE).
-type state() :: ?STATE{}.

-spec new(_) -> state().
new(_) ->
    L = orddict:new(),
    ?STATE{data=L}.

-spec fetch(_, _, state()) -> {ok,_}.
fetch(Key, Default, ?STATE{data=L}) ->
    case orddict:find(Key, L) of
        error ->
            {ok, Default};
        {ok, Value} ->
            {ok, Value}
    end.

-spec exists(_, state()) -> boolean().
exists(Key, ?STATE{data=L}) ->
    lists:keymember(Key, 1, L).

-spec store(_, _, state()) -> state().
store(Key, Val, State=?STATE{data=L}) ->
    L2 = orddict:store(Key, Val, L),
    State?STATE{data=L2}.

-spec store([{_,_}], state()) -> state().
store(Updates, State=?STATE{data=L}) ->
    Inserts = [case Update of
                   {put, Key, Val} ->
                       {Key, Val};
                   {delete, Key} ->
                       {Key, deleted}
               end || Update <- Updates],
    L2 = lists:ukeymerge(1, lists:sort(Inserts), L),
    L3 = [X || X={_, Val} <- L2,
               Val =/= deleted],
    State?STATE{data=L3}.
