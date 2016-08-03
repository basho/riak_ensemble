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
-module(riak_ensemble_peer_tree).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([get/2,
         insert/3,
         rehash_upper/1,
         rehash/1,
         verify_upper/1,
         verify/1,
         exchange_get/3,
         top_hash/1,
         height/1,
         repair/1]).
-export([async_rehash_upper/1,
         async_rehash/1,
         async_verify_upper/1,
         async_verify/1,
         async_repair/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tree :: any(),
                corrupted :: {integer(), integer()}}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% TODO:
%% For most of these APIs, really should return {ok, any()} and
%% corrupted or {error, corrupted}. As is, any() | corrupted reduces
%% to any() which gives us zero dialyzer benefits.

start_link(Id, TreeId, Path) ->
    gen_server:start_link(?MODULE, [Id, TreeId, Path], []).

-spec get(_,pid()) -> any().
get(Key, Pid) ->
    gen_server:call(Pid, {get, Key}, infinity).

-spec insert(_,_,pid()) -> ok | corrupted.
insert(Key, ObjHash, Pid) ->
    gen_server:call(Pid, {insert, Key, ObjHash}, infinity).

-spec rehash_upper(pid()) -> ok.
rehash_upper(Pid) ->
    gen_server:call(Pid, rehash_upper, infinity).

-spec rehash(pid()) -> ok.
rehash(Pid) ->
    gen_server:call(Pid, rehash, infinity).

-spec top_hash(pid()) -> any().
top_hash(Pid) ->
    gen_server:call(Pid, top_hash, infinity).

-spec exchange_get(_,_,pid()) -> any().
exchange_get(Level, Bucket, Pid) ->
    gen_server:call(Pid, {exchange_get, Level, Bucket}, infinity).

-spec height(pid()) -> pos_integer().
height(Pid) ->
    gen_server:call(Pid, height, infinity).

-spec repair(pid()) -> ok.
repair(Pid) ->
    gen_server:call(Pid, repair, infinity).

-spec verify_upper(pid()) -> boolean().
verify_upper(Pid) ->
    gen_server:call(Pid, verify_upper, infinity).

-spec verify(pid()) -> boolean().
verify(Pid) ->
    gen_server:call(Pid, verify, infinity).

%%%===================================================================

%% These async operations must only be called by a process that is
%% monitoring or linked to the tree process (eg. riak_ensemble_peer).

%% Asynchronously sends rehash_complete to caller
-spec async_rehash_upper(pid()) -> ok.
async_rehash_upper(Pid) ->
    gen_server:cast(Pid, {async_rehash_upper, self()}).

%% Asynchronously sends rehash_complete to caller
-spec async_rehash(pid()) -> ok.
async_rehash(Pid) ->
    gen_server:cast(Pid, {async_rehash, self()}).

%% Asynchronously sends {verify_complete, boolean()}
-spec async_verify_upper(pid()) -> ok.
async_verify_upper(Pid) ->
    gen_server:cast(Pid, {async_verify_upper, self()}).

%% Asynchronously sends {verify_complete, boolean()}
-spec async_verify(pid()) -> ok.
async_verify(Pid) ->
    gen_server:cast(Pid, {async_verify, self()}).

%% Asynchronously sends repair_complete
-spec async_repair(pid()) -> ok.
async_repair(Pid) ->
    gen_server:cast(Pid, {async_repair, self()}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Id, TreeId, Path]) ->
    TreeType = application:get_env(riak_ensemble, synctree_backend, synctree_ets),
    Tree = synctree:new(Id, default, default, TreeType, [{path, Path},
                                                         {tree_id, TreeId}]),
    State = #state{tree=Tree},
    {ok, State}.

handle_call({get, Key}, _From, State) ->
    {Reply, State2} = do_get(Key, State),
    {reply, Reply, State2};
handle_call({insert, Key, ObjHash}, _From, State) ->
    {Reply, State2} = do_insert(Key, ObjHash, State),
    {reply, Reply, State2};
handle_call(rehash_upper, _From, State) ->
    State2 = do_rehash_upper(State),
    {reply, ok, State2};
handle_call(rehash, _From, State) ->
    State2 = do_rehash(State),
    {reply, ok, State2};
handle_call(top_hash, _From, State) ->
    Reply = do_top_hash(State),
    {reply, Reply, State};
handle_call({exchange_get, Level, Bucket}, _From, State) ->
    {Reply, State2} = do_exchange_get(Level, Bucket, State),
    {reply, Reply, State2};
handle_call(height, _From, State) ->
    Reply = do_height(State),
    {reply, Reply, State};
handle_call(repair, _From, State) ->
    State2 = do_repair(State),
    {reply, ok, State2};
handle_call(verify_upper, _From, State) ->
    Reply = do_verify_upper(State),
    {reply, Reply, State};
handle_call(verify, _From, State) ->
    Reply = do_verify(State),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({async_rehash_upper, From}, State) ->
    State2 = do_rehash_upper(State),
    async_reply(From, rehash_complete),
    {noreply, State2};
handle_cast({async_rehash, From}, State) ->
    State2 = do_rehash(State),
    async_reply(From, rehash_complete),
    {noreply, State2};
handle_cast({async_verify_upper, From}, State) ->
    Reply = do_verify_upper(State),
    async_reply(From, {verify_complete, Reply}),
    {noreply, State};
handle_cast({async_verify, From}, State) ->
    Reply = do_verify(State),
    async_reply(From, {verify_complete, Reply}),
    {noreply, State};
handle_cast({async_repair, From}, State) ->
    State2 = do_repair(State),
    async_reply(From, repair_complete),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Hardcoded to send FSM event as expected by riak_ensemble_peer
async_reply(From, Reply) when is_pid(From) ->
    gen_fsm:send_event(From, Reply).

-spec do_get(_,state()) -> {any(), state()}.
do_get(Key, State=#state{tree=Tree}) ->
    case synctree:get(Key, Tree) of
        {corrupted, Level, Bucket} ->
            State2 = State#state{corrupted={Level, Bucket}},
            {corrupted, State2};
        Other ->
            {Other, State}
    end.

-spec do_insert(_,_,state()) -> {ok, state()} | {corrupted, state()}.
do_insert(Key, ObjHash, State=#state{tree=Tree}) ->
    case synctree:insert(Key, ObjHash, Tree) of
        {corrupted, Level, Bucket} ->
            State2 = State#state{corrupted={Level, Bucket}},
            {corrupted, State2};
        NewTree ->
            %% io:format("Hash updated: ~p :: ~p~n", [NewTree, synctree:top_hash(NewTree)]),
            State2 = State#state{tree=NewTree},
            {ok, State2}
    end.

-spec do_rehash_upper(state()) -> state().
do_rehash_upper(State=#state{tree=Tree}) ->
    NewTree = synctree:rehash_upper(Tree),
    State#state{tree=NewTree}.

-spec do_rehash(state()) -> state().
do_rehash(State=#state{tree=Tree}) ->
    NewTree = synctree:rehash(Tree),
    State#state{tree=NewTree}.

-spec do_top_hash(state()) -> any().
do_top_hash(#state{tree=Tree}) ->
    synctree:top_hash(Tree).

-spec do_exchange_get(_,_,state()) -> any().
do_exchange_get(Level, Bucket, State=#state{tree=Tree}) ->
    case synctree:exchange_get(Level, Bucket, Tree) of
        {corrupted, Level, Bucket} ->
            State2 = State#state{corrupted={Level, Bucket}},
            {corrupted, State2};
        Hashes ->
            {Hashes, State}
    end.

-spec do_height(state()) -> pos_integer().
do_height(#state{tree=Tree}) ->
    synctree:height(Tree).

do_repair(State=#state{corrupted=undefined}) ->
    State;
do_repair(State=#state{corrupted=Corrupted, tree=Tree}) ->
    Final = synctree:height(Tree) + 1,
    case Corrupted of
        {Final, _Bucket} ->
            %% io:format("REPAIR SEGMENT: ~p~n", [Corrupted]),
            Tree2 = synctree:m_flush(synctree:m_batch({delete, Corrupted}, Tree)),
            Tree3 = synctree:rehash(Tree2),
            State#state{tree=Tree3, corrupted=undefined};
        _ ->
            %% io:format("REPAIR INNER: ~p~n", [Corrupted]),
            State#state{corrupted=undefined}
    end.

-spec do_verify_upper(state()) -> boolean().
do_verify_upper(#state{tree=Tree}) ->
    synctree:verify_upper(Tree).

-spec do_verify(state()) -> boolean().
do_verify(#state{tree=Tree}) ->
    synctree:verify(Tree).
