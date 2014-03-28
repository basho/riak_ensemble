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

%% @doc
%% Implementation of the {@link riak_ensemble_backend} behavior that
%% that stores simple key/value objects in an in-process orddict that
%% is synchronously written to disk on each put.
%%
%% Note: this is used as the peer type for the built-in root ensemble
%% that stores system-wide metadata, bootstraps additional ensembles,
%% etc.

-module(riak_ensemble_basic_backend).
-behaviour(riak_ensemble_backend).

-export([init/3, new_obj/4]).
-export([obj_epoch/1, obj_seq/1, obj_key/1, obj_value/1]).
-export([set_obj_epoch/2, set_obj_seq/2, set_obj_value/2]).
-export([get/3, put/4, tick/5, ping/2]).
-export([sync_request/2, sync/2]).

-include_lib("riak_ensemble_types.hrl").

-record(obj, {epoch :: epoch(),
              seq   :: seq(),
              key   :: term(),
              value :: term()}).

-record(state, {savefile :: file:filename(),
                data     :: orddict:orddict()}).

-type obj()   :: #obj{}.
-type state() :: #state{}.
-type key()   :: any().
-type value() :: any().

%%===================================================================

-spec init(ensemble_id(), peer_id(), []) -> state().
init(Ensemble, Id, []) ->
    %% TODO: Any concerns about using hash here?
    %% TODO: For root ensemble, should we use different naming scheme?
    <<Hash:160/integer>> = riak_ensemble_util:sha(term_to_binary({Ensemble, Id})),
    Name = integer_to_list(Hash),
    {ok, Root} = application:get_env(riak_ensemble, data_root),
    File = filename:join([Root, "ensembles", Name ++ "_kv"]),
    Data = reload_data(File),
    #state{savefile=File, data=Data}.

%%===================================================================

-spec new_obj(epoch(), seq(), key(), value()) -> obj().
new_obj(Epoch, Seq, Key, Value) ->
    #obj{epoch=Epoch, seq=Seq, key=Key, value=Value}.

%%===================================================================

-spec obj_epoch(obj()) -> epoch().
obj_epoch(Obj) ->
    Obj#obj.epoch.

-spec obj_seq(obj()) -> seq().
obj_seq(Obj) ->
    Obj#obj.seq.

-spec obj_key(obj()) -> key().
obj_key(Obj) ->
    Obj#obj.key.

-spec obj_value(obj()) -> value().
obj_value(Obj) ->
    Obj#obj.value.

%%===================================================================

-spec set_obj_epoch(epoch(), obj()) -> obj().
set_obj_epoch(Epoch, Obj) ->
    Obj#obj{epoch=Epoch}.

-spec set_obj_seq(seq(), obj()) -> obj().
set_obj_seq(Seq, Obj) ->
    Obj#obj{seq=Seq}.

-spec set_obj_value(value(), obj()) -> obj().
set_obj_value(Value, Obj) ->
    Obj#obj{value=Value}.

%%===================================================================

-spec get(key(), riak_ensemble_backend:from(), state()) -> state().
get(Key, From, State=#state{data=Data}) ->
    Reply = case orddict:find(Key, Data) of
                {ok, Value} ->
                    Value;
                error ->
                    notfound
            end,
    riak_ensemble_backend:reply(From, Reply),
    State.

-spec put(key(), obj(), riak_ensemble_backend:from(), state()) -> state().
put(Key, Obj, From, State=#state{savefile=File, data=Data}) ->
    Data2 = orddict:store(Key, Obj, Data),
    save_data(File, Data2),
    riak_ensemble_backend:reply(From, Obj),
    State#state{data=Data2}.

%%===================================================================

-spec sync_request(riak_ensemble_backend:from(), state()) -> state().
sync_request(From, State=#state{data=Data}) ->
    riak_ensemble_backend:reply(From, Data),
    State.

-spec sync([{peer_id(), orddict:orddict()}], state()) -> {ok, state()}       |
                                                         {async, state()}    |
                                                         {{error,_}, state()}.
sync(OtherData, State=#state{savefile=File, data=Data}) ->
    LatestFn = fun(_, Obj1, Obj2) ->
                       riak_ensemble_backend:latest_obj(?MODULE, Obj1, Obj2)
               end,
    Data2 = lists:foldl(fun({_PeerId, PeerData}, Acc) ->
                                orddict:merge(LatestFn, Acc, PeerData)
                        end, Data, OtherData),
    save_data(File, Data2),
    State2 = State#state{data=Data2},
    {ok, State2}.

%%===================================================================

-spec tick(epoch(), seq(), peer_id(), views(), state()) -> state().
tick(_Epoch, _Seq, _Leader, _Views, State) ->
    State.

-spec ping(pid(), state()) -> {ok, state()}.
ping(_From, State) ->
    {ok, State}.

%%===================================================================

-spec reload_data(file:filename()) -> orddict:orddict().
reload_data(File) ->
    case load_saved_data(File) of
        {ok, Data} ->
            Data;
        not_found ->
            []
    end.

-spec load_saved_data(file:filename()) -> not_found | {ok, orddict:orddict()}.
load_saved_data(File) ->
    case riak_ensemble_util:read_file(File) of
        {ok, <<CRC:32/integer, Binary/binary>>} ->
            case erlang:crc32(Binary) of
                CRC ->
                    try
                        {ok, binary_to_term(Binary)}
                    catch
                        _:_ ->
                            lager:warning("Corrupted state detected. "
                                          "Reverting to empty state."),
                            not_found
                    end;
                _ ->
                    not_found
            end;
        {error, _} ->
            not_found
    end.

-spec save_data(file:filename(), orddict:orddict()) -> ok.
save_data(File, Data) ->
    Binary = term_to_binary(Data),
    CRC = erlang:crc32(Binary),
    ok = filelib:ensure_dir(File),
    ok = riak_ensemble_util:replace_file(File, [<<CRC:32/integer>>, Binary]),
    ok.
