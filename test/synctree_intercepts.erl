-module(synctree_intercepts).
-compile(export_all).
-define(M, synctree_orig).

%% copied from synctree.erl
-record(tree, {id        :: term(),
               width     :: pos_integer(),
               segments  :: pos_integer(),
               height    :: pos_integer(),
               shift     :: pos_integer(),
               shift_max :: pos_integer(),
               top_hash  :: any(),
               buffer    :: [_],
               buffered  :: non_neg_integer(),
               mod       :: module(),
               modstate  :: any()
              }).

corrupt_upper(Updates, Tree=#tree{id=Id}) ->
    %% io:format("~p: corrupt_upper: ~p~n", [Id, Updates]),
    case should_corrupt(Id, Updates) of
        true ->
            %% io:format(user, "~p: corrupt: ~p~n", [Id, Updates]),
            %% io:format(user, "CORRUPT!!!!!!!!!!!!!!!~n", []),
            %% Corrupt two levels above the segment
            [A,B,C|Rest] = lists:reverse(Updates),
            Updates2 = [A, B, corrupt_level(C) | Rest],
            ?M:m_store_orig(Updates2, Tree);
        false ->
            ?M:m_store_orig(Updates, Tree)
    end.

corrupt_segment(Updates, Tree=#tree{id=Id}) ->
    case should_corrupt(Id, Updates) of
        true ->
            %% io:format(user, "~p: corrupt: ~p~n", [Id, Updates]),
            %% io:format(user, "CORRUPT!!!!!!!!!!!!!!!~n", []),
            [A|Rest] = lists:reverse(Updates),
            Updates2 = [corrupt_level(A)|Rest],
            ?M:m_store_orig(Updates2, Tree);
        false ->
            ?M:m_store_orig(Updates, Tree)
    end.

corrupt_segment_all(Updates, Tree=#tree{id=_Id}) ->
    case should_corrupt(all, Updates) of
        true ->
            %% io:format(user, "~p: corrupt: ~p~n", [_Id, Updates]),
            %% io:format(user, "CORRUPT!!!!!!!!!!!!!!!~n", []),
            [A|Rest] = lists:reverse(Updates),
            Updates2 = [corrupt_level(A)|Rest],
            ?M:m_store_orig(Updates2, Tree);
        false ->
            ?M:m_store_orig(Updates, Tree)
    end.

corrupt_segment_follower(Updates, Tree=#tree{id=Id}) ->
    case should_corrupt({follower, Id}, Updates) of
        true ->
            %% io:format(user, "~p: corrupt: ~p~n", [Id, Updates]),
            %% io:format(user, "CORRUPT!!!!!!!!!!!!!!!~n", []),
            [A|Rest] = lists:reverse(Updates),
            Updates2 = [corrupt_level(A)|Rest],
            ?M:m_store_orig(Updates2, Tree);
        false ->
            ?M:m_store_orig(Updates, Tree)
    end.

m_store_normal(Updates, Tree) ->
    ?M:m_store_orig(Updates, Tree).

should_corrupt(Id, Updates) ->
    Leader = riak_ensemble_manager:get_leader(root),
    %% io:format(user, "CC: ~p / ~p: ~p~n", [Leader, Id, Updates]),
    case Id of
        all ->
            should_corrupt_updates(Updates);
        {root, {root, _}} ->
        %% {root, Leader} ->
            should_corrupt_updates(Updates);
        {follower, PeerId} when (PeerId =/= {root, Leader}) ->
            should_corrupt_updates(Updates);
        _ ->
            false
    end.

should_corrupt_updates(Updates) ->
    Segment = hd(lists:reverse(Updates)),
    case Segment of
        {put, _, Data} ->
            is_list(Data) andalso lists:keymember(<<"corrupt">>, 1, Data);
        _ ->
            false
    end.

corrupt_level({put, Key, Data}) ->
    [{VictimKey, VictimBin}|T] = Data,
    CorruptBin = corrupt_binary(VictimBin),
    %% io:format(user, "VB: ~p :: ~p~n", [VictimBin, CorruptBin]),
    {put, Key, [{VictimKey, CorruptBin}|T]}.

corrupt_binary(<<X:8/integer, Bin/binary>>) ->
    Y = (X + 1) rem 256,
    <<Y:8/integer, Bin/binary>>.
