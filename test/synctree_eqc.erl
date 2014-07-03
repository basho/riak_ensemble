%% Port of EQC test from riak_core/hashtree.erl

-module(synctree_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

eqc_test_() ->
    {spawn,
     {timeout, 120,
      fun() ->
              ?assert(eqc:quickcheck(eqc:testing_time(4, prop_correct())))
      end
     }}.

bin(X) ->
    list_to_binary(integer_to_list(X)).

objects() ->
    ?SIZED(Size, objects(Size+3)).

objects(N) ->
    ?LET(Keys, shuffle(lists:seq(1,N)),
         [{bin(K), binary(8)} || K <- Keys]
        ).

lengths(N) ->
    ?LET(MissingN1,  choose(0,N),
         ?LET(MissingN2,  choose(0,N-MissingN1),
              ?LET(DifferentN, choose(0,N-MissingN1-MissingN2),
                   {MissingN1, MissingN2, DifferentN}))).

mutate(Binary) ->
    L1 = binary_to_list(Binary),
    [X|Xs] = L1,
    X2 = (X+1) rem 256,
    L2 = [X2|Xs],
    list_to_binary(L2).

prop_correct() ->
    ?FORALL(Objects, objects(),
            ?FORALL({MissingN1, MissingN2, DifferentN}, lengths(length(Objects)),
                    begin
                        {RemoteOnly, Objects2} = lists:split(MissingN1, Objects),
                        {LocalOnly,  Objects3} = lists:split(MissingN2, Objects2),
                        {Different,  Same}     = lists:split(DifferentN, Objects3),

                        Different2 = [{Key, mutate(Hash)} || {Key, Hash} <- Different],

                        Insert = fun(Tree, Vals) ->
                                         lists:foldl(fun({Key, Hash}, Acc) ->
                                                             synctree:insert(Key, Hash, Acc)
                                                     end, Tree, Vals)
                                 end,

                        [begin
                             A1 = synctree:new({0,Id}),
                             B1 = synctree:new({0,Id}),

                             A2 = Insert(A1, Same),
                             A3 = Insert(A2, LocalOnly),
                             A4 = Insert(A3, Different),

                             B2 = Insert(B1, Same),
                             B3 = Insert(B2, RemoteOnly),
                             B4 = Insert(B3, Different2),

                             A5 = A4,
                             B5 = B4,

                             Expected =
                                 [{missing, Key}        || {Key, _} <- RemoteOnly] ++
                                 [{remote_missing, Key} || {Key, _} <- LocalOnly] ++
                                 [{different, Key}      || {Key, _} <- Different],

                             KeyDiff = compare(A5, B5),

                             ?assertEqual(lists:usort(Expected),
                                          lists:usort(KeyDiff)),

                             %% Reconcile trees
                             A6 = Insert(A5, RemoteOnly),
                             B6 = Insert(B5, LocalOnly),
                             B7 = Insert(B6, Different),
                             A7 = A6,
                             B8 = B7,
                             ?assertEqual([], compare(A7, B8)),
                             true
                         end || Id <- lists:seq(0, 10)],
                        %% close(A0),
                        %% close(B0),
                        %% destroy(A0),
                        %% destroy(B0),
                        true
                    end)).

compare(T1, T2) ->
    KeyDiff = synctree:local_compare(T1, T2),
    [case Delta of
         {Key, {'$none', _}} ->
             {missing, Key};
         {Key, {_, '$none'}} ->
             {remote_missing, Key};
         {Key, {_, _}} ->
             {different, Key}
     end || Delta <- KeyDiff].

-endif.
