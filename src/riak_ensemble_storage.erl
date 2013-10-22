-module(riak_ensemble_storage).
-compile(export_all).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ETS, ets_riak_ensemble_storage).
-define(SYNC_DELAY, 100).

-type gen_server_from() :: any().

-record(state, {savefile :: file:filename(),
                waiting  :: [gen_server_from()],
                timer    :: reference()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

sync() ->
    %% gen_server:call(?MODULE, sync, infinity).
    ok.

put(Key, Value) ->
    ets:insert(?ETS, {Key, Value}).

get(Key) ->
    try
        Value = ets:lookup_element(?ETS, Key, 2),
        Value
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
    {ok, #state{savefile=File, waiting=[], timer=undefined}}.

handle_call({get, Key}, _From, State) ->
    Reply = case ets:lookup(?ETS, Key) of
                [{_, Value}] ->
                    Value;
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

maybe_schedule_sync(State=#state{timer=undefined}) ->
    Timer = erlang:send_after(?SYNC_DELAY, self(), do_sync),
    State#state{timer=Timer};
maybe_schedule_sync(State) ->
    State.

do_sync(State=#state{savefile=File, waiting=Waiting}) ->
    Data = term_to_binary(ets:tab2list(?ETS)),
    ok = riak_ensemble_save:write(File, Data),
    [gen_server:reply(From, ok) || From <- Waiting],
    State#state{waiting=[], timer=undefined}.
