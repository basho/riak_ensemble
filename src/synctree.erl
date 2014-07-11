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

%% @doc
%% This module implements a self-validating hashtree that is used within
%% riak_ensemble as the primary data integrity mechanism. This tree is
%% designed similar to the hashtree used in file systems such as ZFS, with
%% parent nodes containing the hash of their children. Thus, during traversal
%% a tree path can be completely verified from the root node to the endpoint.
%%
%% This tree is designed to be replicated on multiple nodes and provides
%% built-in exchange logic that efficiently determines the differences between
%% two trees -- thus trees can exchange with peers and heal missing/corrupted
%% data.
%%
%% To perform an exchange, trees must be of the same shape regardless of the
%% data inserted. Thus, the tree implemented by this module has a fixed
%% structure and is therefore more akin to a hash trie. To enable this fixed
%% structure, data inserted into the tree is uniformly mapped to one of a fixed
%% number of segments. These segments are sorted key/value lists. Hashes are
%% computed over each segment, with each hash being stored as a leaf in the
%% actual hash tree. Since there are a fixed number of segments, there is a
%% fixed number of leaf hashes. The remaining levels in the hash tree are then
%% generated on top of these leaf hashes as normal.
%%
%% This design is therefore similar to hashtree.erl from riak_core.
%%
%% The main high-levels differences are as follows:
%%   1. The synctree is built entirely on pure key/value (get/put) operations,
%%      there is no concept of iteration nor any need for a sorted backend.
%%
%%   2. The synctree is always up-to-date. An insert into the tree immediately
%%      updates the appropriate segment and relevant tree path. There is no
%%      concept of a delayed, bulk update as used by hashtree.erl
%%
%%   3. All operations on the tree are self-validating. Every traversal through
%%      the tree whether for reads, inserts, or exchanges verifies the hashes
%%      down all encountered tree paths.
%%
%%   4. The synctree supports pluggable backends. It was originally designed
%%      and tested against both orddict and ETS backends, and then later
%%      extended with a LevelDB backend for persistent storage as used in
%%      riak_ensemble.
%%
%% Most of the differences from hashtree.erl are not strictly better, but
%% rather are designed to address differences between Riak AAE (which uses
%% hashtree) and riak_ensemble integrity checking (which uses synctree).
%%
%% Specifically, AAE is designed to be a fast, mostly background process
%% with limited impact on normal Riak operations. While the integrity logic
%% requires an always up-to-date tree that is used to verify every get/put
%% operation as they occur, ensuring 100% validity. In other terms, for AAE
%% the hashtree is not expected to be the truth but rather a best-effort
%% projection of the truth (the K/V backend is the truth). Whereas for the
%% integrity logic, the synctree is the truth -- if the backend differs, it's
%% wrong and we consider the backend data corrupted.

-module(synctree).

-export([new/0, new/1, new/3, new/4, new/5]).
-export([newdb/1, newdb/2]).
-export([height/1, top_hash/1]).
-export([insert/3, get/2, exchange_get/3, corrupt/2]).
-export([compare/3, compare/4, compare/5, local_compare/2, direct_exchange/1]).
-export([rehash_upper/1, rehash/1]).
-export([verify_upper/1, verify/1]).

%% TODO: Should we eeally exporting these directly?
-export([m_batch/2, m_flush/1]).

-define(WIDTH, 16).
-define(SEGMENTS, 1024*1024).

-type action() :: {put, _, _} |
                  {delete, _}.

-type hash()   :: binary().
-type key()    :: term().
-type value()  :: binary().
-type level()  :: non_neg_integer().
-type bucket() :: non_neg_integer().
-type hashes() :: [{_, hash()}].

-type corrupted() :: {corrupted, level(), bucket()}.

-record(tree, {id        :: term(),
               width     :: pos_integer(),
               segments  :: pos_integer(),
               height    :: pos_integer(),
               shift     :: pos_integer(),
               shift_max :: pos_integer(),
               top_hash  :: hash(),
               buffer    :: [action()],
               buffered  :: non_neg_integer(),
               mod       :: module(),
               modstate  :: any()
              }).

-type tree() :: #tree{}.
-type maybe_integer() :: pos_integer() | default.
-type options() :: proplists:proplist().

%% Supported hash methods
-define(H_MD5, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec newdb(term()) -> tree().
newdb(Id) ->
    newdb(Id, []).

-spec newdb(term(), options()) -> tree().
newdb(Id, Opts) ->
    new(Id, default, default, synctree_leveldb, Opts).

-spec new() -> tree().
new() ->
    new(undefined).

-spec new(term()) -> tree().
new(Id) ->
    new(Id, ?WIDTH, ?SEGMENTS).

-spec new(term(), maybe_integer(), maybe_integer()) -> tree().
new(Id, Width, Segments) ->
    new(Id, Width, Segments, synctree_ets).

-spec new(term(), maybe_integer(), maybe_integer(), module()) -> tree().
new(Id, Width, Segments, Mod) ->
    new(Id, Width, Segments, Mod, []).

-spec new(term(), maybe_integer(), maybe_integer(), module(), options()) -> tree().
new(Id, default, Segments, Mod, Opts) ->
    new(Id, ?WIDTH, Segments, Mod, Opts);
new(Id, Width, default, Mod, Opts) ->
    new(Id, Width, ?SEGMENTS, Mod, Opts);
new(Id, Width, Segments, Mod, Opts) ->
    Height = compute_height(Segments, Width),
    Shift = compute_shift(Width),
    ShiftMax = Shift * Height,
    Tree = #tree{id=Id,
                 width=Width,
                 segments=Segments,
                 height=Height,
                 shift=Shift,
                 shift_max=ShiftMax,
                 buffer=[],
                 buffered=0,
                 mod=Mod,
                 modstate=Mod:new(Opts)},
    reload_top_hash(Tree).

-spec reload_top_hash(tree()) -> tree().
reload_top_hash(Tree) ->
    {ok, TopHash} = m_fetch({0,0}, undefined, Tree),
    Tree#tree{top_hash=TopHash}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec height(tree()) -> pos_integer().
height(#tree{height=Height}) ->
    Height.

-spec top_hash(tree()) -> hash() | undefined.
top_hash(#tree{top_hash=TopHash}) ->
    TopHash.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert(key(), value(), tree()) -> tree() | corrupted().
insert(Key, Value, Tree) when is_binary(Value) ->
    Segment = get_segment(Key, Tree),
    case get_path(Segment, Tree) of
        {corrupted,_,_}=Error ->
            Error;
        Path ->
            {TopHash, Updates} = update_path(Path, Key, Value, []),
            Tree2 = m_store(Updates, Tree),
            Tree2#tree{top_hash=TopHash}
    end.

update_path([], _, ChildHash, Acc) ->
    %% Make sure to also save top hash
    Acc2 = [{put, {0,0}, ChildHash}|Acc],
    {ChildHash, Acc2};
update_path([{{Level,Bucket}, Hashes}|Path], Child, ChildHash, Acc) ->
    Hashes2 = orddict:store(Child, ChildHash, Hashes),
    NewHash = hash(Hashes2),
    Acc2 = [{put, {Level,Bucket}, Hashes2}|Acc],
    update_path(Path, Bucket, NewHash, Acc2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get(key(), tree()) -> value() | notfound | corrupted().
get(Key, Tree) ->
    TopHash = top_hash(Tree),
    case TopHash of
        undefined ->
            notfound;
        _ ->
            Segment = get_segment(Key, Tree),
            case get_path(Segment, Tree) of
                {corrupted,_,_}=Error ->
                    Error;
                [{_, Hashes}|_] ->
                    orddict_find(Key, notfound, Hashes)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec exchange_get(bucket(), level(), tree()) -> hashes() | corrupted().
exchange_get(0, 0, Tree) ->
    TopHash = top_hash(Tree),
    [{0,TopHash}];
exchange_get(Level, Bucket, Tree) ->
    Hashes = verified_hashes(Level, Bucket, Tree),
    Hashes.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec corrupt(key(), tree()) -> tree().
corrupt(Key, Tree=#tree{height=Height}) ->
    Segment = get_segment(Key, Tree),
    Bucket = {Height + 1, Segment},
    {ok, Hashes} = m_fetch(Bucket, [], Tree),
    Hashes2 = orddict:erase(Key, Hashes),
    m_store(Bucket, Hashes2, Tree).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_segment(Key, #tree{segments=Segments}) ->
    <<HashKey:128/integer>> = crypto:hash(md5, ensure_binary(Key)),
    HashKey rem Segments.

-spec hash([{_, binary()}]) -> hash().
hash(Term) ->
    L = [H || {_,H} <- Term],
    HashBin = crypto:hash(md5, L),
    <<?H_MD5, HashBin/binary>>.

ensure_binary(Key) when is_integer(Key) ->
    <<Key:64/integer>>;
ensure_binary(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
ensure_binary(Key) when is_binary(Key) ->
    Key;
ensure_binary(Key) ->
    term_to_binary(Key).

compute_height(Segments, Width) ->
    %% By design, we require segments to be a power of width.
    Height = erlang:trunc(math:log(Segments) / math:log(Width)),
    case erlang:trunc(math:pow(Width, Height)) =:= Segments of
        true ->
            Height
    end.

compute_shift(Width) ->
    %% By design, we require width to be a power of 2.
    Shift = erlang:trunc(math:log(Width) / math:log(2)),
    case erlang:trunc(math:pow(2, Shift)) =:= Width of
        true ->
            Shift
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec verified_hashes(level(), bucket(), tree()) -> hashes() | corrupted().
verified_hashes(Level, Bucket, Tree=#tree{shift=Shift}) ->
    N = (Level - 1) * Shift,
    TopHash = top_hash(Tree),
    case get_path(N, 1, Shift, Bucket, [{0, TopHash}], Tree, []) of
        {corrupted,_,_}=Error ->
            %% io:format("Tree corrupted (verified_hashes)~n"),
            Error;
        [{_, Hashes}|_] ->
            Hashes
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_path(Segment, Tree=#tree{shift=Shift, shift_max=N}) ->
    TopHash = top_hash(Tree),
    get_path(N, 1, Shift, Segment, [{0, TopHash}], Tree, []).

get_path(N, Level, Shift, Segment, UpHashes, Tree, Acc) ->
    Bucket = Segment bsr N,
    Expected = orddict_find(Bucket, undefined, UpHashes),
    {ok, Hashes} = m_fetch({Level, Bucket}, [], Tree),
    Acc2 = [{{Level, Bucket}, Hashes}|Acc],
    Verify = verify_hash(Expected, Hashes),
    case {Verify, N} of
        {false, _} ->
            lager:warning("Corrupted at ~p/~p~n", [Level, Bucket]),
            {corrupted, Level, Bucket};
        {_, 0} ->
            Acc2;
        _ ->
            get_path(N-Shift, Level+1, Shift, Segment, Hashes, Tree, Acc2)
    end.

verify_hash(undefined, []) ->
    true;
verify_hash(undefined, _Actual) ->
    %% io:format("Expected (undef): []~n"),
    %% io:format("Actual:   ~p~n", [_Actual]),
    false;
verify_hash(Expected, Hashes) ->
    %% Note: when we add support for multiple hash functions, update this
    %%       function to compute Actual using the same function that was
    %%       previously used to compute Expected.
    Actual = hash(Hashes),
    case Expected of
        Actual ->
            true;
        _ ->
            %% io:format("Expected: ~p~n", [Expected]),
            %% io:format("Actual:   ~p~n", [Actual]),
            false
    end.

orddict_find(Key, Default, L) ->
    case lists:keyfind(Key, 1, L) of
        false ->
            Default;
        {_, Value} ->
            Value
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% exchange logic
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

direct_exchange(Tree=#tree{}) ->
    fun(exchange_get, {Level, Bucket}) ->
            exchange_get(Level, Bucket, Tree);
       (start_exchange_level, {_Level, _Buckets}) -> 
           ok
    end.

local_compare(T1, T2) ->
    Local = direct_exchange(T1),
    Remote = fun(exchange_get, {Level, Bucket}) ->
                     exchange_get(Level, Bucket, T2);
                (start_exchange_level, {_Level, _Buckets}) ->
                     ok
             end,
    compare(height(T1), Local, Remote).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

compare(Height, Local, Remote) ->
    compare(Height, Local, Remote, fun(Keys, KeyAcc) ->
                                           Keys ++ KeyAcc
                                   end).

compare(Height, Local, Remote, AccFun) ->
    compare(Height, Local, Remote, AccFun, []).

compare(Height, Local, Remote, AccFun, Opts) ->
    Final = Height + 1,
    exchange(0, [0], Final, Local, Remote, AccFun, [], Opts).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

exchange(_Level, [], _Final, _Local, _Remote, _AccFun, Acc, _Opts) ->
    Acc;
exchange(Level, Diff, Final, Local, Remote, AccFun, Acc, Opts) ->
    %% io:format("~p :: ~w~n", [Level, Diff]),
    if Level =:= Final ->
            exchange_final(Level, Diff, Local, Remote, AccFun, Acc, Opts);
       true ->
            Diff2 = exchange_level(Level, Diff, Local, Remote, Opts),
            exchange(Level+1, Diff2, Final, Local, Remote, AccFun, Acc, Opts)
    end.

exchange_level(Level, Buckets, Local, Remote, Opts) ->
    Remote(start_exchange_level, {Level, Buckets}),
    FilterType = filter_type(Opts),
    lists:flatmap(fun(Bucket) ->
                          A = Local(exchange_get, {Level, Bucket}),
                          B = Remote(exchange_get, {Level, Bucket}),
                          Delta = riak_ensemble_util:orddict_delta(A, B),
                          Diffs = filter(FilterType, Delta),
                          [BK || {BK, _} <- Diffs]
                  end, Buckets).

exchange_final(Level, Buckets, Local, Remote, AccFun, Acc0, Opts) ->
    Remote(start_exchange_level, {Level, Buckets}),
    FilterType = filter_type(Opts),
    lists:foldl(fun(Bucket, Acc) ->
                        A = Local(exchange_get, {Level, Bucket}),
                        B = Remote(exchange_get, {Level, Bucket}),
                        Delta = riak_ensemble_util:orddict_delta(A, B),
                        Diffs = filter(FilterType, Delta),
                        AccFun(Diffs, Acc)
                end, Acc0, Buckets).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

filter_type(Opts) ->
    LocalOnly = lists:member(local_only, Opts),
    RemoteOnly = lists:member(remote_only, Opts),
    %% Intentionally fail if both local and remote only are provided
    case {LocalOnly, RemoteOnly} of
        {true, false} ->
            local_only;
        {false, true} ->
            remote_only;
        {false, false} ->
            all
    end.

filter(all, Delta) ->
    Delta;
filter(local_only, Delta) ->
    %% filter out remote_missing differences
    lists:filter(fun({_, {_, '$none'}}) ->
                         false;
                    (_) ->
                         true
                 end, Delta);
filter(remote_only, Delta) ->
    %% filter out local_missing differences
    lists:filter(fun({_, {'$none', _}}) ->
                         false;
                    (_) ->
                         true
                 end, Delta).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

m_fetch(Key, Default, #tree{mod=Mod, modstate=ModState}) ->
    Mod:fetch(Key, Default, ModState).

m_store(Key, Val, Tree=#tree{mod=Mod, modstate=ModState}) ->
    ModState2 = Mod:store(Key, Val, ModState),
    Tree#tree{modstate=ModState2}.

-spec m_store([action()], tree()) -> tree().
m_store(Updates, Tree=#tree{mod=Mod, modstate=ModState}) ->
    ModState2 = Mod:store(Updates, ModState),
    Tree#tree{modstate=ModState2}.

m_exists(Key, #tree{mod=Mod, modstate=ModState}) ->
    Mod:exists(Key, ModState).

m_batch(Update, Tree=#tree{buffer=Buffer, buffered=Buffered}) ->
    Tree2 = Tree#tree{buffer=[Update|Buffer],
                      buffered=Buffered + 1},
    maybe_flush_buffer(Tree2).

maybe_flush_buffer(Tree=#tree{buffered=Buffered}) ->
    Threshold = 200,
    case Buffered > Threshold of
        true ->
            m_flush(Tree);
        false ->
            Tree
    end.

m_flush(Tree=#tree{buffer=Buffer}) ->
    Updates = lists:reverse(Buffer),
    Tree2 = m_store(Updates, Tree),
    Tree2#tree{buffer=[], buffered=0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec rehash_upper(tree()) -> tree().
rehash_upper(Tree=#tree{height=Height}) ->
    rehash(Height, Tree).

-spec rehash(tree()) -> tree().
rehash(Tree=#tree{height=Height}) ->
    rehash(Height + 1, Tree).

rehash(MaxDepth, Tree) ->
    {Tree2, Hashes} = rehash(1, MaxDepth, 0, Tree),
    {TopHash, Tree3} = case Hashes of
                           [] ->
                               NewTree = delete_existing_batch({0,0}, Tree2),
                               {undefined, NewTree};
                           _ ->
                               NewHash = hash(Hashes),
                               NewTree = m_batch({put, {0,0}, NewHash}, Tree2),
                               {NewHash, NewTree}
                       end,
    Tree4 = m_flush(Tree3),
    Tree4#tree{top_hash=TopHash}.

rehash(Level, MaxDepth, Bucket, Tree) when Level =:= MaxDepth ->
    %% final level, just return the stored value
    {ok, Hashes} = m_fetch({Level, Bucket}, [], Tree),
    {Tree, Hashes};
rehash(Level, MaxDepth, Bucket, Tree=#tree{width=Width}) ->
    X0 = Bucket * Width,
    Children = [X || X <- lists:seq(X0, X0+Width-1)],
    {Tree2, CH} = lists:foldl(fun(X, {TreeAcc, Acc}) ->
                                      case rehash(Level+1, MaxDepth, X, TreeAcc) of
                                          {Tree2, []} ->
                                              {Tree2, Acc};
                                          {Tree2, Hashes} ->
                                              NewHash = hash(Hashes),
                                              Acc2 = [{X, NewHash}|Acc],
                                              {Tree2, Acc2}
                                      end
                              end, {Tree, []}, Children),
    CH2 = lists:reverse(CH),
    Tree3 = case CH2 of
                [] ->
                    delete_existing_batch({Level,Bucket}, Tree2);
                _ ->
                    m_batch({put, {Level,Bucket}, CH2}, Tree2)
            end,
    {Tree3, CH2}.

delete_existing_batch(Key, Tree) ->
    case m_exists(Key, Tree) of
        true ->
            m_batch({delete, Key}, Tree);
        false ->
            Tree
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% verify using top-down BFS traversal

-spec verify_upper(tree()) -> boolean().
verify_upper(Tree=#tree{height=Height}) ->
    verify(Height, Tree).

-spec verify(tree()) -> boolean().
verify(Tree=#tree{height=Height}) ->
    verify(Height + 1, Tree).

verify(MaxDepth, Tree) ->
    verify(1, MaxDepth, 0, top_hash(Tree), Tree).

verify(Level, MaxDepth, Bucket, UpHash, Tree) ->
    {ok, Hashes} = m_fetch({Level, Bucket}, [], Tree),
    case verify_hash(UpHash, Hashes) of
        false ->
            false;
        true when Level == MaxDepth ->
            true;
        true ->
            lists:all(fun({Child, ChildHash}) ->
                              verify(Level + 1, MaxDepth, Child, ChildHash, Tree)
                      end, Hashes)
    end.
