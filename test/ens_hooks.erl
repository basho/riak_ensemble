%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014-2017 Basho Technologies, Inc.
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

%% These are meck versions of what were originally rt_intercept hooks.
%% When TEST is defined:
%%  - Hooked functions MUST be exported and called with the ?MODULE qualifier.
%%  - Where state fields have to be accessed, the mocked module must export
%%    state_fields/0 as a wrapper around the record_info/2 pseudo-function.
%% In all cases, a fun() is returned, even if dynamic configuration isn't
%% required, in part to ensure that the right M/F/A is being hooked.
-module(ens_hooks).

-include_lib("eunit/include/eunit.hrl").

% Hooks
-export([
    corrupt_segment_all_fun/1,
    corrupt_segment_follower_fun/1,
    corrupt_segment_leader_fun/1,
    corrupt_upper_fun/1,
    drop_put_fun/1,
    synctree_path_shared_fun/1
]).

% Helpers
-export([
    pass_fun/1
]).

%% ===================================================================
%% Private API
%% ===================================================================

%% Returns a replacement for synctree:m_store/2 that corrupts some data
%% for any peer.
corrupt_segment_all_fun({synctree, m_store, 2}) ->
    fun(OrigUpdates, Tree) ->
        UseUpdates = case should_corrupt_updates(OrigUpdates) of
            true ->
                [Tgt | Rest] = lists:reverse(OrigUpdates),
                [corrupt_level(Tgt) | Rest];
            _ ->
                OrigUpdates
        end,
        meck:passthrough([UseUpdates, Tree])
    end.

%% Returns a replacement for synctree:m_store/2 that corrupts some data
%% for followers.
corrupt_segment_follower_fun({synctree = Mod, m_store, 2}) ->
    {_, IdIdx} = lists:keyfind(id, 1, Mod:state_fields()),
    fun(OrigUpdates, Tree) ->
        Leader = riak_ensemble_manager:get_leader(root),
        UseUpdates = case erlang:element(IdIdx, Tree) of
            {root, Leader} ->
                OrigUpdates;
            _Follower ->
                case should_corrupt_updates(OrigUpdates) of
                    true ->
                        [Tgt | Rest] = lists:reverse(OrigUpdates),
                        [corrupt_level(Tgt) | Rest];
                    _ ->
                        OrigUpdates
                end
        end,
        meck:passthrough([UseUpdates, Tree])
    end.

%% Returns a replacement for synctree:m_store/2 that corrupts some data
%% for the leader.
corrupt_segment_leader_fun({synctree = Mod, m_store, 2}) ->
    {_, IdIndex} = lists:keyfind(id, 1, Mod:state_fields()),
    fun(OrigUpdates, Tree) ->
        Leader = riak_ensemble_manager:get_leader(root),
        UseUpdates = case erlang:element(IdIndex, Tree) of
            {root, Leader} ->
                case should_corrupt_updates(OrigUpdates) of
                    true ->
                        [Tgt | Rest] = lists:reverse(OrigUpdates),
                        [corrupt_level(Tgt) | Rest];
                    _ ->
                        OrigUpdates
                end;
            _Follower ->
                OrigUpdates
        end,
        meck:passthrough([UseUpdates, Tree])
    end.

%% Returns a replacement for synctree:m_store/2 that corrupts some data
%% for the leader two levels above the segment.
corrupt_upper_fun({synctree = Mod, m_store, 2}) ->
    {_, IdIndex} = lists:keyfind(id, 1, Mod:state_fields()),
    fun(OrigUpdates, Tree) ->
        Leader = riak_ensemble_manager:get_leader(root),
        UseUpdates = case erlang:element(IdIndex, Tree) of
            {root, Leader} ->
                case should_corrupt_updates(OrigUpdates) of
                    true ->
                        [A, B, Tgt | Rest] = lists:reverse(OrigUpdates),
                        [A, B, corrupt_level(Tgt) | Rest];
                    _ ->
                        OrigUpdates
                end;
            _Follower ->
                OrigUpdates
        end,
        meck:passthrough([UseUpdates, Tree])
    end.

%% Returns a replacement for riak_ensemble_basic_backend:put/4 that
%% drops some operations.
drop_put_fun({riak_ensemble_basic_backend = Mod, put, 4}) ->
    {_, IdIndex} = lists:keyfind(id, 1, Mod:state_fields()),
    fun
        (<<"drop", _/binary>> = Key, Obj, From, State) ->
            case erlang:element(IdIndex, State) of
                {root, _} ->
                    meck:passthrough([Key, Obj, From, State]);
                _ ->
                    riak_ensemble_backend:reply(From, Obj),
                    State
            end;
        (Key, Obj, From, State) ->
            meck:passthrough([Key, Obj, From, State])
    end.

%% Returns a replacement for riak_ensemble_basic_backend:synctree_path/2 that
%% doesn't just return the default.
synctree_path_shared_fun({riak_ensemble_basic_backend, synctree_path, 2}) ->
    fun
        (root, Id) ->
            {erlang:term_to_binary(Id), "root"};
        (Ensemble, Id) ->
            meck:passthrough([Ensemble, Id])
    end.

%% Returns a function calling meck:passthrough/1 with A args.
%% For anything defined above, must also match the MFA pattern here.
pass_fun({_M, _F, 2}) ->
    fun(A1, A2) -> meck:passthrough([A1, A2]) end;
pass_fun({_M, _F, 3}) ->
    fun(A1, A2, A3) -> meck:passthrough([A1, A2, A3]) end;
pass_fun({_M, _F, 4}) ->
    fun(A1, A2, A3, A4) -> meck:passthrough([A1, A2, A3, A4]) end.

%% ===================================================================
%% Internal
%% ===================================================================

should_corrupt_updates(Updates) ->
    case lists:last(Updates) of
        {put, _Key, Data} when erlang:is_list(Data) ->
            lists:keymember(<<"corrupt">>, 1, Data);
        _ ->
            false
    end.

corrupt_level({put, Key, [{TgtKey, <<X:8/integer, Bin/binary>>} | DTail]}) ->
    Y = (X + 1) rem 256,
    Corrupt = <<Y:8/integer, Bin/binary>>,
    {put, Key, [{TgtKey, Corrupt} | DTail]}.
