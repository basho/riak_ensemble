%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ensemble_exchange).
-compile(export_all).

start_exchange(Ensemble, Peer, Id, Tree, Peers, Views, Trusted) ->
    spawn(fun() ->
                  try
                      perform_exchange(Ensemble, Peer, Id, Tree, Peers, Views, Trusted)
                  catch Class:Reason ->
                          io:format("CAUGHT: ~p/~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
                          gen_fsm:send_event(Peer, exchange_failed)
                  end
          end).

perform_exchange(Ensemble, Peer, Id, Tree, Peers, Views, Trusted) ->
    Required = case Trusted of
                   true  -> quorum;
                   false -> other
               end,
    RemotePeers =
        case trust_majority(Id, Peers, Views, Required) of
            {ok, Quorum} ->
                Quorum;
            failed ->
                case all_trust_majority(Id, Peers, Views) of
                    {ok, All} ->
                        All;
                    failed ->
                        failed
                end
        end,
    case RemotePeers of
        failed ->
            gen_fsm:send_event(Peer, exchange_failed),
            ok;
        _ ->
            perform_exchange2(Ensemble, Peer, Id, Tree, RemotePeers)
    end.

perform_exchange2(Ensemble, Peer, Id, Tree, RemotePeers) ->
    case riak_ensemble_peer_tree:verify_upper(Tree) of
        true ->
            exchange(Ensemble, Peer, Id, Tree, RemotePeers);
        false ->
            %% io:format(user, "~p: tree_corrupted (perform_exchange2)~n", [Id]),
            gen_fsm:sync_send_event(Peer, tree_corrupted, infinity)
    end.

exchange(_Ensemble, Peer, _Id, _Tree, []) ->
    gen_fsm:send_event(Peer, exchange_complete);
exchange(Ensemble, Peer, Id, Tree, [RemotePeer|RemotePeers]) ->
    RemotePid = riak_ensemble_manager:get_peer_pid(Ensemble, RemotePeer),
    RemoteTree = gen_fsm:sync_send_event(RemotePid, tree_pid, infinity),
    Local = fun(exchange_get, {L,B}) ->
                    exchange_get(L, B, Peer, Tree);
               (start_exchange_level, _) ->
                    ok
            end,
    Remote = fun(exchange_get, {L,B}) ->
                     exchange_get(L, B, RemotePid, RemoteTree);
                (start_exchange_level, _) ->
                     ok
             end,
    Height = riak_ensemble_peer_tree:height(Tree),
    Result = synctree:compare(Height, Local, Remote),
    %% io:format("~p: Result: ~p~n", [Id, Result]),
    _ = [case Diff of
             {Key, {'$none', B}} ->
                 riak_ensemble_peer_tree:insert(Key, B, Tree);
             {_Key, {_, '$none'}}  ->
                 ok;
             {Key, {A,B}} ->
                 case riak_ensemble_peer:valid_obj_hash(B, A) of
                     true ->
                         riak_ensemble_peer_tree:insert(Key, B, Tree);
                     false ->
                         ok
                 end
         end || Diff <- Result],
    exchange(Ensemble, Peer, Id, Tree, RemotePeers).

exchange_get(L, B, PeerPid, Tree) ->
    case riak_ensemble_peer_tree:exchange_get(L, B, Tree) of
        corrupted ->
            gen_fsm:sync_send_event(PeerPid, tree_corrupted, infinity),
            throw(corrupted);
        Hashes ->
            Hashes
    end.

trust_majority(Id, Peers, Views, Required) ->
    X = riak_ensemble_msg:blocking_send_all(exchange, Id, Peers,
                                            Views, Required),
    {Future, _} = X,
    Parent = self(),
    spawn_link(fun() ->
                       Result = case riak_ensemble_msg:wait_for_quorum(Future) of
                                    {quorum_met, Replies} ->
                                        {ok, [Peer || {Peer,_} <- Replies]};
                                    {timeout, _Replies} ->
                                        failed
                                end,
                       %% io:format(user, "trust majority: ~p~n", [Result]),
                       Parent ! {trust, Result}
               end),
    receive {trust, Trusted} ->
            Trusted
    end.

all_trust_majority(Id, Peers, Views) ->
    X = riak_ensemble_msg:blocking_send_all(all_exchange, Id, Peers,
                                            Views, all),
    {Future, _} = X,
    Parent = self(),
    spawn_link(fun() ->
                       Result = case riak_ensemble_msg:wait_for_quorum(Future) of
                                    {quorum_met, Replies} ->
                                        {ok, [Peer || {Peer,_} <- Replies]};
                                    {timeout, _Replies} ->
                                        failed
                                end,
                       %% io:format(user, "all_trust majority: ~p~n", [Result]),
                       Parent ! {trust, Result}
               end),
    receive {trust, Trusted} ->
            Trusted
    end.
