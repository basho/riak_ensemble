-module(erl_port).
-behaviour(gen_server).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% API
-export([start/1,
         get_output/1]).

-record(state, {
        node :: atom(),
        output = "" :: string()}).

start(Node) ->
    gen_server:start({local, Node}, ?MODULE, Node, []).

get_output(Pid) ->
    gen_server:call(Pid, get_output).

init(Node) ->
    gen_server:cast(self(), open_port),
    {ok, #state{node=Node}}.

handle_call(get_output, _, State) ->
    {reply, State#state.output, State}.

handle_cast(open_port, State=#state{node=Node}) ->
    Str =
        "erl -shutdown_time 10000 -pa ../.eunit -pa ../ebin -pa ../deps/*/ebin -setcookie riak_ensemble_test -name " ++ atom_to_list(Node),
    io:format(user, "Str = ~p~n", [Str]),
    open_port({spawn, Str}, []),
    {noreply, State}.

handle_info({_Port, {data, Data}}, State=#state{output=Output}) ->
    lager:info("Received Port(~p) Data ~p", [_Port, Data]),
    NewState = State#state{output=Output ++ Data},
    {noreply, NewState};

handle_info({'EXIT', Port, Reason}, State) ->
    lager:info("Port ~p has exited with Reason ~p", [Port, Reason]),
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_OldVsn, State, _) ->
    {ok, State}.


