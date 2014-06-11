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
%% The primary purpose of this module is to route requests to ensemble
%% leaders given ensemble names, even if the requests are originating
%% from nodes that are not part of the ensemble system: eg. a remote
%% Erlang node using {@link riak_ensemble_client}. This router also
%% addresses the issue that ensemble and peer names are arbitrary terms
%% and not registered names, and therefore Erlang's built-in messaging
%% cannot directly address ensemble peers.
%%
%% This routing layer is handled by multiple instances of this module
%% that run on each node in the ensemble cluster. A request is sent to
%% a random router on a given node, which then looks up the ensemble
%% leader using its local `riak_ensemble_manager' state, routing the
%% request directly to a local pid (if the leader is local) or forwarding
%% on to a router on the respective leading node.
%%
%% The reason for running multiple router instances per node is to enable
%% additional concurrency and not have a single router bottleneck traffic.
%%
%% A secondary purpose of this module is to provide an isolated version
%% of `gen_fsm:send_sync_event' that converts timeouts into error tuples
%% rather than exit conditions, as well as discarding late/delayed messages.
%% This isolation is provided by spawning an intermediary proxy process.

-module(riak_ensemble_router).
-compile(export_all).
-behaviour(gen_server).

-include_lib("riak_ensemble_types.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-type target() :: pid() | ensemble_id().
-type msg() :: term().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(atom()) -> ignore | {error, _} | {ok, pid()}.
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec sync_send_event(target(), msg(), timeout()) -> timeout | term().
sync_send_event(Target, Event, Timeout) ->
    sync_send_event(node(), Target, Event, Timeout).

-spec sync_send_event(node(), target(), msg(), timeout()) -> timeout | term().
sync_send_event(_Node, Target, _Event, infinity) when not is_pid(Target) ->
    %% TODO: Consider handling this case
    throw("infinity timeout not currently safe for non-pid target");
sync_send_event(Node, Target, Event, Timeout) ->
    Ref = make_ref(),
    spawn_link(?MODULE, sync_proxy, [self(), Ref, Node, Target, Event, Timeout]),
    receive
        {Ref, nack} ->
            timeout;
        {Ref, Result} ->
            Result
    end.

-spec sync_proxy(pid(), reference(), node(), target(), msg(), timeout()) -> ok.
sync_proxy(From, Ref, _Node, Target, Event, Timeout) when is_pid(Target) ->
    sync_proxy_direct(From, Ref, Target, Event, Timeout);
sync_proxy(From, Ref, Node, Target, Event, Timeout) ->
    sync_proxy_router(From, Ref, Node, Target, Event, Timeout).

-spec sync_proxy_direct(pid(), reference(), pid(), msg(), timeout()) -> ok.
sync_proxy_direct(From, Ref, Pid, Event, Timeout) ->
    try
        Result = gen_fsm:sync_send_event(Pid, Event, Timeout),
        From ! {Ref, Result},
        ok
    catch
        _:_ ->
            From ! {Ref, timeout},
            ok
    end.

-spec sync_proxy_router(pid(), reference(), node(), ensemble_id(), msg(), timeout()) -> ok.
sync_proxy_router(From, Ref, Node, Target, Event, Timeout) ->
    case riak_ensemble_router:cast(Node, Target, {sync_send_event, self(), Ref, Event, Timeout}) of
        ok ->
            receive
                {Ref, _}=Reply ->
                    From ! Reply,
                    ok
            after Timeout ->
                    From ! {Ref, timeout},
                    ok
            end;
        error ->
            From ! {Ref, timeout},
            ok
    end.

-spec cast(ensemble_id(), msg()) -> error | ok.
cast(Ensemble, Msg) ->
    ensemble_cast(Ensemble, Msg).

-spec cast(node(), ensemble_id(), msg()) -> error | ok.
cast(Node, Ensemble, Msg) when Node =:= node() ->
    cast(Ensemble, Msg);
cast(Node, Ensemble, Msg) ->
    NumRouters = tuple_size(routers()),
    Pick = random(NumRouters),
    Router = element(Pick + 1, routers()),
    %% gen_server:cast({Router, Node}, {ensemble_cast, Ensemble, Msg}).
    case noconnect_cast({Router, Node}, {ensemble_cast, Ensemble, Msg}) of
        nodedown ->
            _ = fail_cast(Msg),
            ok;
        ok ->
            ok
    end.

noconnect_cast(Dest, Msg) ->
    case catch erlang:send(Dest, {'$gen_cast', Msg}, [noconnect]) of
	noconnect ->
            spawn(fun() ->
                          case Dest of
                              {_, Node} ->
                                  net_adm:ping(Node);
                              Pid when is_pid(Pid) ->
                                  net_adm:ping(node(Pid));
                              _ ->
                                  ok
                          end
                  end),
            nodedown;
        _ ->
            ok
    end.

%% TODO: Switch to using sidejob_config or copy thereof
routers() ->
    {riak_ensemble_router_1,
     riak_ensemble_router_2,
     riak_ensemble_router_3,
     riak_ensemble_router_4,
     riak_ensemble_router_5,
     riak_ensemble_router_6,
     riak_ensemble_router_7}.

%% @doc Generate "random" number X, such that `0 <= X < N'.
-spec random(pos_integer()) -> pos_integer().
random(N) ->
    %% Note: hashing over I/O statistics seems to be fastest option when
    %%       benchmarking with lots of concurrent processes. Inside BEAM,
    %%       querying I/O statistics corresponds to two atomic reads.
    %%
    %% random:uniform_s(os:timestamp()),
    %% crypto:rand_uniform(0, NumRouters),
    %% element(3, os:timestamp()) rem N.
    %% erlang:phash2(make_ref(), N).
    %% erlang:phash2(erlang:statistics(context_switches), N).
    %% erlang:phash2(erlang:statistics(garbage_collection), NumRouters),
    erlang:phash2(erlang:statistics(io), N).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ensemble_cast, Ensemble, Msg}, State) ->
    ensemble_cast(Ensemble, Msg),
    {noreply, State};
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

-spec ensemble_cast(ensemble_id(), msg()) -> error | ok.
ensemble_cast(Ensemble, Msg) ->
    case riak_ensemble_manager:get_leader(Ensemble) of
        {_, Node}=Leader ->
            %% io:format("L: ~p~n", [Leader]),
            if Node =:= node() ->
                    Pid = riak_ensemble_manager:get_peer_pid(Ensemble, Leader),
                    %% io:format("Sending to ~p~n", [Pid]),
                    handle_ensemble_cast(Msg, Pid),
                    ok;
               true ->
                    riak_ensemble_router:cast(Node, Ensemble, Msg),
                    ok
            end;
        undefined ->
            error
    end.

-spec handle_ensemble_cast(_,_) -> ok.
handle_ensemble_cast({sync_send_event, From, Ref, Event, Timeout}, Pid) ->
    spawn(fun() ->
                  try
                      Result = gen_fsm:sync_send_event(Pid, Event, Timeout),
                      From ! {Ref, Result}
                  catch
                      _:_ ->
                          From ! {Ref, timeout}
                  end
          end),
    ok;
handle_ensemble_cast(_, _Pid) ->
    ok.

fail_cast({sync_send_event, From, Ref, _Event, _Timeout}) ->
    From ! {Ref, timeout},
    ok.
