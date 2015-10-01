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

-export([init_ets/0,
         new/1,
         fetch/3,
         exists/2,
         store/3,
         store/2]).

-record(?MODULE, {id   :: binary(),
                  db   :: any(),
                  path :: term()}).

-define(STATE, #?MODULE).
-type state() :: ?STATE{}.

-define(RETRIES, 10).

%% -------------------------------------------------------------------

%% Prefix bytes used to tag keys stored in LevelDB to allow for easy
%% evolution of the storage format.

%% Key representing {Bucket, Level} data
-define(K_BUCKET, 0).

%% -------------------------------------------------------------------

%% @doc
%% Called by {@link riak_ensemble_sup} to create the public ETS table used
%% to keep track of shared LevelDB references. Having riak_ensemble_sup
%% own the ETS table ensures it survives as long as riak_ensemble is up.
-spec init_ets() -> ok.
init_ets() ->
    _ = ets:new(?MODULE, [named_table, set, public,
                          {read_concurrency, true},
                          {write_concurrency, true}]),
    ok.

-spec new(_) -> state().
new(Opts) ->
    Path = get_path(Opts),
    {ok, DB} = maybe_open_leveldb(Path, ?RETRIES),
    Id = get_tree_id(Opts),
    ?STATE{id=Id, path=Path, db=DB}.

maybe_open_leveldb(Path, Retries) ->
    %% Check if we have already opened this LevelDB instance, which can
    %% occur when peers are sharing the same on-disk instance.
    case ets:lookup(?MODULE, Path) of
        [{_, DB}] ->
            {ok, DB};
        _ ->
	    ok = filelib:ensure_dir(Path),
	    case eleveldb:open(Path, leveldb_opts()) of
		{ok, DB} ->
		    %% If eleveldb:open succeeded, we should have the only ref
		    true = ets:insert_new(?MODULE, {Path, DB}),
		    {ok, DB};
		_ when Retries > 0 ->
		    timer:sleep(100),
		    maybe_open_leveldb(Path, Retries - 1)
	    end
    end.


get_path(Opts) ->
    case proplists:get_value(path, Opts) of
        undefined ->
            Base = "/tmp/ST",
            Name = integer_to_list(timestamp(erlang:timestamp())),
            filename:join(Base, Name);
        Path ->
            Path
    end.

get_tree_id(Opts) ->
    case proplists:get_value(tree_id, Opts) of
        undefined ->
            <<>>;
        Id when is_binary(Id) ->
            Id
    end.

db_key(Id, {Level, Bucket}) ->
    db_key(Id, Level, Bucket).

db_key(Id, Level, Bucket)  when is_integer(Level), is_integer(Bucket) ->
    BucketBin = binary:encode_unsigned(Bucket),
    <<?K_BUCKET:8/integer, Id/binary,  Level:8/integer, BucketBin/binary>>.

-spec fetch(_, _, state()) -> {ok, _}.
fetch({Level, Bucket}, Default, ?STATE{id=Id, db=DB}) ->
    DBKey = db_key(Id, Level, Bucket),
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

exists({Level, Bucket}, ?STATE{id=Id, db=DB}) ->
    DBKey = db_key(Id, Level, Bucket),
    case eleveldb:get(DB, DBKey, []) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

-spec store(_, _, state()) -> state().
store({Level, Bucket}, Val, State=?STATE{id=Id, db=DB}) ->
    DBKey = db_key(Id, Level, Bucket),
    %% Intentionally ignore errors (TODO: Should we?)
    _ = eleveldb:put(DB, DBKey, term_to_binary(Val), []),
    State.

-spec store([{_,_}], state()) -> state().
store(Updates, State=?STATE{id=Id, db=DB}) ->
    %% TODO: Should we sort first? Doesn't LevelDB do that automatically in memtable?
    DBUpdates = [case Update of
                     {put, Key, Val} ->
                         {put, db_key(Id, Key), term_to_binary(Val)};
                     {delete, Key} ->
                         {delete, db_key(Id, Key)}
                 end || Update <- Updates],
    %% Intentionally ignore errors (TODO: Should we?)
    _ = eleveldb:write(DB, DBUpdates, []),
    State.

timestamp({Mega, Secs, Micro}) ->
    Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

leveldb_opts() ->
    [{is_internal_db, true},
     {write_buffer_size, 4 * 1024 * 1024},
     {use_bloomfilter, true},
     {create_if_missing, true}].

