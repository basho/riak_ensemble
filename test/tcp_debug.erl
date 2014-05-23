-module(tcp_debug).

-define(TIMEOUT, 1000).

%% Client API
-export([connect/1, call/2, call/3, close/1]).

%% Server API
-export([remote_start/1, start/0, stop/1]).

%% ===============
%% Client 
%% ===============
connect(Port) ->
    gen_tcp:connect("localhost", Port, [binary, {packet, 2}, {nodelay, true}]).

call(Sock, Data) ->
    call(Sock, Data, ?TIMEOUT).

call(Sock, {_M, _F, _A}=Data, Timeout) ->
    inet:setopts(Sock, [{active, once}]),
    gen_tcp:send(Sock, term_to_binary(Data)),
    receive
        {tcp, Sock, Response} -> 
            binary_to_term(Response);
        {tcp_closed, Sock}=E1 ->
            E1;
        {tcp_error, Sock, _Reason}=E2 ->
            E2
    after Timeout ->
        {error, timeout}
    end.

close(Sock) ->
    gen_tcp:close(Sock).

%% ===============
%% Server
%% ===============

remote_start(Node) ->
    rpc:call(Node, ?MODULE, start, [], ?TIMEOUT).

start() ->
    Pid = spawn(fun() ->
        {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false},
            {linger, {false, 0}}, {packet, 2}, {nodelay, true}]),
        {ok, Port} = inet:port(ListenSock),
        receive
            {get_port, From} ->
                From ! Port
        end,
        loop(ListenSock)
    end),
    Pid ! {get_port, self()},
    receive
        Port -> Port
    end,
    {ok, Port, Pid}.

%% Kill the server and all it's connections
stop(Pid) ->
     exit(Pid, kill).
    
loop(ListenSock) ->
    {ok, Sock} = gen_tcp:accept(ListenSock),
    Pid = spawn_link(fun() -> debug_session(Sock) end),
    ok = gen_tcp:controlling_process(Sock, Pid),
    Pid ! ready,
    loop(ListenSock).

debug_session(Sock) ->
    receive
        ready -> ok
    end,
    inet:setopts(Sock, [{active, true}]),
    receive_loop(Sock).

receive_loop(Sock) ->
    receive
        {tcp, Sock, Data} ->
            handle_msg(Sock, Data),
            receive_loop(Sock);
        {tcp_closed, Sock} ->
            io:format("Socket ~p closed ~n", [Sock]),
            ok;
        _ ->
            ok
    end.

handle_msg(Sock, Data) when is_binary(Data) ->
    {M, F, A} = binary_to_term(Data),
    Result = apply(M, F, A),
    gen_tcp:send(Sock, term_to_binary(Result)).
