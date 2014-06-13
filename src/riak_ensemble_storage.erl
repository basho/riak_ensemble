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
%% This module implements a central storage manager for riak_ensemble.
%% Previously, individual ensembles as well as the ensemble manager would
%% independently save their own state to disk. However, such an approach
%% scaled poorly as the number of independent ensembles increased. It was
%% not uncommon to see thousands of synchronous writes issued to disk per
%% second, overwhelming the I/O subsystem. To solve this issue, this storage
%% manager was created.
%%
%% Rather than storing data independently, the storage manager combines the
%% state from multiple ensembles as well as the ensemble manager into a
%% single entity that is stored together in a single file. Since this file
%% is now a critical single point of failure, the storage manager uses the
%% new {@link riak_ensemble_save} logic to save this data to disk such that
%% there are four redundant copies to recover from.
%%
%% This manager is also responsible for coalescing multiple writes together
%% to reduce disk traffic. Individual writes are staged in an ETS table and
%% then flushed to disk after a delay (eg. 50ms).
%%
%% There are two ways to save data to disk that are used by other components
%% in riak_ensemble: synchronous and asynchronous.
%%
%% For synchronous writes components use the sequence:
%%   riak_ensemble_storage:put(Key, Data),
%%   riak_ensemble_storage:sync().
%% The sync() call than blocks until the data has successfully been written,
%% to disk.
%%
%% For asynchronous writes, components simply use put() without sync(). The
%% data will then be written to disk either when another component calls sync,
%% or after next storage manager tick (eg. every 5 seconds).
%%

-module(riak_ensemble_storage).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get/1, put/2, sync/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ETS, ets_riak_ensemble_storage).
-define(SYNC_DELAY, riak_ensemble_config:storage_delay()).
-define(TICK, riak_ensemble_config:storage_tick()).

-type gen_server_from() :: any().

-record(state, {savefile :: file:filename(),
                waiting  :: [gen_server_from()],
                previous :: binary(),
                timer    :: reference()}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec sync() -> ok.
sync() ->
    gen_server:call(?MODULE, sync, infinity).

-spec put(term(), term()) -> true.
put(Key, Value) ->
    ets:insert(?ETS, {Key, Value}).

-spec get(term()) -> {ok, term()} | not_found.
get(Key) ->
    try
        Value = ets:lookup_element(?ETS, Key, 2),
        {ok, Value}
    catch
        _:_ ->
            %% Retry through the server in case data is being loaded
            gen_server:call(?MODULE, {get, Key}, infinity)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Root} = application:get_env(riak_ensemble, data_root),
    File = filename:join([Root, "ensembles", "ensemble_facts"]),
    _ = ets:new(?ETS, [named_table, public, {read_concurrency, true},
                       {write_concurrency, true}]),
    case riak_ensemble_save:read(File) of
        {ok, Bin} ->
            Existing = binary_to_term(Bin),
            true = ets:insert(?ETS, Existing);
        _ ->
            ok
    end,
    schedule_tick(),
    {ok, #state{savefile=File, waiting=[], timer=undefined}}.

handle_call({get, Key}, _From, State) ->
    Reply = case ets:lookup(?ETS, Key) of
                [{_, Value}] ->
                    {ok, Value};
                _ ->
                    not_found
            end,
    {reply, Reply, State};

handle_call(sync, From, State=#state{waiting=Waiting}) ->
    Waiting2 = [From|Waiting],
    State2 = maybe_schedule_sync(State),
    State3 = State2#state{waiting=Waiting2},
    {noreply, State3};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};

handle_info(do_sync, State) ->
    {noreply, do_sync(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec schedule_tick() -> ok.
schedule_tick() ->
    _ = erlang:send_after(?TICK, self(), tick),
    ok.

-spec tick(state()) -> state().
tick(State) ->
    State2 = maybe_schedule_sync(State),
    State2.

-spec maybe_schedule_sync(state()) -> state().
maybe_schedule_sync(State=#state{timer=undefined}) ->
    Timer = erlang:send_after(?SYNC_DELAY, self(), do_sync),
    State#state{timer=Timer};
maybe_schedule_sync(State) ->
    State.

-spec do_sync(state()) -> state().
do_sync(State=#state{savefile=File, waiting=Waiting, previous=PrevData}) ->
    Data = term_to_binary(ets:tab2list(?ETS)),
    case Data of
        PrevData ->
            ok;
        _ ->
            ok = riak_ensemble_save:write(File, Data)
    end,
    _ = [gen_server:reply(From, ok) || From <- Waiting],
    State#state{waiting=[], timer=undefined, previous=Data}.
