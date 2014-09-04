-module(riak_ensemble_basic_backend_intercepts).
-compile(export_all).
-include("riak_ensemble_types.hrl").

-define(M, riak_ensemble_basic_backend_orig).

%% copied from riak_ensemble_basic_backend.erl
-record(state, {savefile :: file:filename(),
                id       :: peer_id(),
                data     :: orddict:orddict()}).

drop_put(Key, Obj, From, State=#state{id=Id}) ->
    case Key of
        <<"drop",_/binary>> ->
            case Id of
                {root, _} ->
                    ?M:put_orig(Key, Obj, From, State);
                _ ->
                    riak_ensemble_backend:reply(From, Obj),
                    State
            end;
        _ ->
            ?M:put_orig(Key, Obj, From, State)
    end.

synctree_path_shared(root, Id) ->
    TreeId = term_to_binary(Id),
    Path = "root",
    {TreeId, Path};
synctree_path_shared(_, _) ->
    default.
