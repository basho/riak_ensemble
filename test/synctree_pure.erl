%% Simple synctree tests that are stateless.
-module(synctree_pure).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TEST(X), {timeout, 60, {test, ?MODULE, X}}).

run_test_() ->
    %% Violating "pure" principle a bit here, alas
    synctree_leveldb:init_ets(),
    Timeout = 60,
    Tests = [?TEST(test_basic_orddict),
             ?TEST(test_basic_ets),
             ?TEST(test_basic_leveldb),
             %% Kinda slow with orddict, disabling for now
             %% ?TEST(test_corrupt_orddict),
             ?TEST(test_corrupt_ets),
             ?TEST(test_corrupt_leveldb),
             ?TEST(test_exchange_orddict),
             ?TEST(test_exchange_ets),
             ?TEST(test_exchange_leveldb)],
    {timeout, Timeout, Tests}.

test_basic_orddict() -> test_basic(synctree_orddict).
test_basic_ets()     -> test_basic(synctree_ets).
test_basic_leveldb() -> test_basic(synctree_leveldb).

test_basic(Mod) ->
    T = build(100, Mod),
    Result = synctree:get(42, T),
    Expect = <<420:64/integer>>,
    ?assertEqual(Expect, Result),
    T2 = synctree:insert(42, <<42:64/integer>>, T),
    Result2 = synctree:get(42, T2),
    Expect2 = <<42:64/integer>>,
    ?assertEqual(Expect2, Result2),
    ok.

test_corrupt_orddict() -> test_corrupt(synctree_orddict).
test_corrupt_ets()     -> test_corrupt(synctree_ets).
test_corrupt_leveldb() -> test_corrupt(synctree_leveldb).

test_corrupt(Mod) ->
    T = build(10, Mod),
    Result = synctree:get(4, T),
    Expect = <<40:64/integer>>,
    ?assertEqual(Expect, Result),
    T2 = synctree:corrupt(4, T),
    Result2 = synctree:get(4, T2),
    ?assertMatch({corrupted, _, _}, Result2),
    T3 = synctree:rehash(T2),
    Result3 = synctree:get(4, T3),
    ?assertEqual(notfound, Result3),
    ok.

test_exchange_orddict() -> test_exchange(synctree_orddict).
test_exchange_ets()     -> test_exchange(synctree_ets).
test_exchange_leveldb() -> test_exchange(synctree_leveldb).

test_exchange(Mod) ->
    Num = 50,
    Diff = 10,
    T1 = build(Num, Mod),
    T2 = build(Num-Diff, Mod),
    Result = synctree:local_compare(T1, T2),
    Expect = expected_diff(Num, Diff),
    ?assertEqual(Expect, lists:sort(Result)),
    ok.

build(N) ->
    build(N, synctree_ets).

build(N, Mod) ->
    do_build(N, synctree:new(undefined, default, default, Mod)).

do_build(0, T) ->
    T;
do_build(N, T) ->
    T2 = synctree:insert(N, <<(N*10):64/integer>>, T),
    do_build(N-1, T2).

expected_diff(Num, Diff) ->
    [{N, {<<(N*10):64/integer>>, '$none'}} 
     || N <- lists:seq(Num - Diff + 1, Num)].
