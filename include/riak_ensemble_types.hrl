-type ensemble_id() :: term().
-type peer_id() :: {term(), node()}.
-type leader_id() :: undefined | peer_id().
-type fixme() :: any().
-type views() :: [[peer_id()]].
-type peer_change() :: term(). %% FIXME
-type change_error() :: already_member | not_member.
-type std_reply() :: timeout | failed | unavailable | nack | {ok, term()}.
-type maybe_pid() :: pid() | undefined.
-type peer_pids() :: [{peer_id(), maybe_pid()}].
-type peer_reply() :: {peer_id(), term()}.
-type epoch() :: integer().
-type seq() :: integer().
-type vsn() :: {epoch(), seq()}.
-type peer_info() :: nodedown | undefined | {any(), boolean(), epoch()}.

-type orddict(Key,Val) :: [{Key, Val}].
-type ordsets(Val) :: [Val].

-record(ensemble_info, {vsn                                   :: vsn(),
                        mod     = riak_ensemble_basic_backend :: module(),
                        args    = []                          :: [any()],
                        leader                                :: leader_id(),
                        views                                 :: [[peer_id()]],
                        seq                                   :: {integer(), integer()}
                       }).
-type ensemble_info() :: #ensemble_info{}.

%% -type ensemble_info() :: {leader_id(), [peer_id()], {integer(), integer()}, module(), [any()]}.

-define(ENSEMBLE_TICK, riak_ensemble_config:tick()).
