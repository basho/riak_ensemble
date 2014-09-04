-module(riak_ensemble_peer_intercepts).
-compile(export_all).
-include("riak_ensemble_types.hrl").

-define(M, riak_ensemble_peer_orig).

check_epoch_false(_Peer, _Epoch, _State) ->
    false.

check_epoch(Peer, Epoch, State) ->
    ?M:check_epoch_orig(Peer, Epoch, State).
