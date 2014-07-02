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
-module(synctree_leveldb).

-export([new/1,
         reopen/1,
         fetch/3,
         exists/2,
         store/3,
         store/2]).

-record(?MODULE, {db   :: any(),
                  path :: term()}).

-define(STATE, #?MODULE).
-type state() :: ?STATE{}.

-define(RETRIES, 10).

-spec new(_) -> state().
new(Opts) ->
    Path = get_path(Opts),
    ok = filelib:ensure_dir(Path),
    {ok, DB} = safe_open(?RETRIES, Path, leveldb_opts()),
    ?STATE{path=Path, db=DB}.

get_path(Opts) ->
    case proplists:get_value(path, Opts) of
        undefined ->
            Base = "/tmp/ST",
            Name = integer_to_list(timestamp(erlang:now())),
            filename:join(Base, Name);
        Path ->
            Path
    end.

-spec fetch(_, _, state()) -> {ok, _}.
fetch(Key, Default, ?STATE{db=DB}) ->
    DBKey = term_to_binary(Key),
    case eleveldb:get(DB, DBKey, []) of
        {ok, Bin} ->
            try
                {ok, binary_to_term(Bin)}
            catch
                _:_ -> {ok, Default}
            end;
        _ ->
            {ok, Default}
    end.

exists(Key, ?STATE{db=DB}) ->
    DBKey = term_to_binary(Key),
    case eleveldb:get(DB, DBKey, []) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

-spec store(_, _, state()) -> state().
store(Key, Val, State=?STATE{db=DB}) ->
    DBKey = term_to_binary(Key),
    %% Intentionally ignore errors (TODO: Should we?)
    _ = eleveldb:put(DB, DBKey, term_to_binary(Val), []),
    State.

-spec store([{_,_}], state()) -> state().
store(Updates, State=?STATE{db=DB}) ->
    %% TODO: Should we sort first? Doesn't LevelDB do that automatically in memtable?
    DBUpdates = [case Update of
                     {put, Key, Val} ->
                         {put, term_to_binary(Key), term_to_binary(Val)};
                     {delete, Key} ->
                         {delete, term_to_binary(Key)}
                 end || Update <- Updates],
    %% Intentionally ignore errors (TODO: Should we?)
    _ = eleveldb:write(DB, DBUpdates, []),
    State.

reopen(State=?STATE{db=DB, path=Path}) ->
    _ = eleveldb:close(DB),
    ok = filelib:ensure_dir(Path),
    {ok, NewDB} = safe_open(?RETRIES, Path, leveldb_opts()),
    State?STATE{db=NewDB}.

safe_open(Retries, Path, Opts) ->
    case eleveldb:open(Path, Opts) of
        {ok, DB} ->
            {ok, DB};
        _ when (Retries > 0) ->
            timer:sleep(100),
            safe_open(Retries-1, Path, Opts)
    end.

timestamp({Mega, Secs, Micro}) ->
    Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

leveldb_opts() ->
    [{is_internal_db, true},
     {write_buffer_size, 4 * 1024 * 1024},
     {use_bloomfilter, true},
     {create_if_missing, true}].

