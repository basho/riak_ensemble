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
%% This module defines riak_ensemble_backend behavior used to implement
%% custom peer behavior.

-module(riak_ensemble_backend).

-include_lib("riak_ensemble_types.hrl").

%% API
-export([start/4,
         get_obj/3,
         set_obj/4,
         latest_obj/3,
         reply/2,
         pong/1]).

%%===================================================================

-type state() :: any().
-type obj()   :: any().
-type key()   :: any().
-type value() :: any().
-type from()  :: {{_,_}, peer_id()} |
                 {riak_ensemble_msg:msg_from(), peer_id()}.

-export_type([from/0]).

%%===================================================================

%% Initialization callback that returns initial module state.
-callback init(ensemble_id(), peer_id(), [any()]) -> state().

%% Create a new opaque key/value object using whatever representation
%% the defining module desires.
-callback new_obj(epoch(), seq(), key(), value()) -> obj().

%% Accessors to retrieve epoch/seq/key/value from an opaque object.
-callback obj_epoch(obj()) -> epoch().
-callback obj_seq  (obj()) -> seq().
-callback obj_key  (obj()) -> term().
-callback obj_value(obj()) -> term().

%% Setters for epoch/seq/value for opaque objects.
-callback set_obj_epoch(epoch(), obj()) -> obj().
-callback set_obj_seq  (seq(),   obj()) -> obj().
-callback set_obj_value(term(),  obj()) -> obj().

%% Callback for get operations. Responsible for sending a reply to the
%% waiting `from' process using {@link reply/2}.
-callback get(key(), from(), state()) -> state().

%% Callback for put operations. Responsible for sending a reply to
%% the waiting `from' process using {@link reply/2}.
-callback put(key(), obj(), from(), state()) -> state().

%% Callback for periodic leader tick. This function is called periodically
%% by an elected leader. Can be used to implement custom housekeeping.
-callback tick(epoch(), seq(), peer_id(), views(), state()) -> state().

%% Callback used to ensure that the backend is still healthy. If `async'
%% is returned, backend should eventually call {@link pong/1}.
-callback ping(pid(), state()) -> {ok|async|failed, state()}.

%% Callback to handle `'DOWN'` messages from monitored, backend related
%% processes. Returns `false` to indicate that this is a reference not
%% related to the backend. Returns `{ok, state()}` to indicate that the
%% backend handled the message and that the peer can continue executing as
%% before. Returns `{reset, state()}` to indicate that in flight requests are
%% likely to fail and that any thing the peer needs to do to reset itself, such
%% as restarting workers, should occur.
-callback handle_down(reference(), pid(), term(), state()) -> false |
                                                              {ok, state()} |
                                                              {reset, state()}.
%% Callback used to determine if peers using this backend can be started.
-callback ready_to_start() -> boolean().

%% Callback that allows a backend to override where a peer's synctree
%% is stored. By default, each peer has an entirely independent on-disk
%% synctree. Using this callback, a backend could do a M:1 or M:N
%% style mapping where multiple peers share an on-disk tree.
%%
%% If this function does not return `default`, it must return a tuple
%% where the first element is an unique tree-id (as a binary) and the
%% second element is the filename for the synctree. Multiple trees
%% stored in the same on-disk synctree will be internally partitioned
%% using the provided tree-id.
-callback synctree_path(ensemble_id(), peer_id()) -> default |
                                                     {binary(), string()}.

%%===================================================================

start(Mod, Ensemble, Id, Args) ->
    Mod:init(Ensemble, Id, Args).

get_obj(Mod, X, Obj) ->
    case X of
        epoch ->
            Mod:obj_epoch(Obj);
        seq ->
            Mod:obj_seq(Obj);
        key ->
            Mod:obj_key(Obj);
        value ->
            Mod:obj_value(Obj)
    end.

set_obj(Mod, X, Val, Obj) ->
    case X of
        epoch ->
            Mod:set_obj_epoch(Val, Obj);
        seq ->
            Mod:set_obj_seq(Val, Obj);
        value ->
            Mod:set_obj_value(Val, Obj)
    end.

latest_obj(Mod, ObjA, ObjB) ->
    A = {Mod:obj_epoch(ObjA), Mod:obj_seq(ObjA)},
    B = {Mod:obj_epoch(ObjB), Mod:obj_seq(ObjB)},
    case B > A of
        true  -> ObjB;
        false -> ObjA
    end.

-spec reply(from(), any()) -> ok.
reply({{To, Tag}, _Id}, Reply) ->
    catch To ! {Tag, Reply},
    ok;
reply({From, Id}, Reply) ->
    riak_ensemble_msg:reply(From, Id, Reply),
    ok.

-spec pong(pid()) -> ok.
pong(From) ->
    riak_ensemble_peer:backend_pong(From).
