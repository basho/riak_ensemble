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

-module(riak_ensemble_client).
-export([kget/3, kupdate/5, kput_once/4, kover/4, kdelete/3, ksafe_delete/4]).
-export([kget/4, kupdate/6, kput_once/5, kover/5, kdelete/4, ksafe_delete/5]).
-export([kget/5]).

-include_lib("riak_ensemble_types.hrl").

-type obj() :: any().
-type client_reply() ::  {ok, obj()} |
                         {error, failed | timeout | unavailable}.

%%%===================================================================

-spec kget(_,_,timeout()) -> client_reply().
kget(Ensemble, Key, Timeout) ->
    kget(node(), Ensemble, Key, Timeout).

-spec kget(node(),_,_,timeout()) -> client_reply().
kget(Node, Ensemble, Key, Timeout) ->
    kget(Node, Ensemble, Key, Timeout, []).

-spec kget(node(),_,_,timeout(),_) -> client_reply().
kget(Node, Ensemble, Key, Timeout, Opts) ->
    maybe(Node,
          fun() ->
              translate(riak_ensemble_peer:kget(Node, Ensemble, Key, Timeout, Opts))
          end).

%%%===================================================================

-spec kupdate(_,_,_,_,timeout()) -> client_reply().
kupdate(Ensemble, Key, Obj, NewObj, Timeout) ->
    kupdate(node(), Ensemble, Key, Obj, NewObj, Timeout).

-spec kupdate(node(),_,_,_,_,timeout()) -> client_reply().
kupdate(Node, Ensemble, Key, Obj, NewObj, Timeout) ->
    maybe(Node,
          fun() -> translate(riak_ensemble_peer:kupdate(Node, Ensemble, Key,
                                                       Obj, NewObj, Timeout))
          end).

%%%===================================================================

-spec kput_once(_,_,_,timeout()) -> client_reply().
kput_once(Ensemble, Key, NewObj, Timeout) ->
    kput_once(node(), Ensemble, Key, NewObj, Timeout).

-spec kput_once(node(),_,_,_,timeout()) -> client_reply().
kput_once(Node, Ensemble, Key, NewObj, Timeout) ->
    maybe(Node,
          fun() -> translate(riak_ensemble_peer:kput_once(Node, Ensemble, Key,
                                                          NewObj, Timeout))
          end).

%%%===================================================================

-spec kover(_,_,_,timeout()) -> client_reply().
kover(Ensemble, Key, NewObj, Timeout) ->
    kover(node(), Ensemble, Key, NewObj, Timeout).

-spec kover(node(),_,_,_,timeout()) -> client_reply().
kover(Node, Ensemble, Key, NewObj, Timeout) ->
    maybe(Node,
          fun() ->
              translate(riak_ensemble_peer:kover(Node, Ensemble, Key, NewObj,
                                                 Timeout))
          end).

%%%===================================================================

-spec kdelete(_,_,timeout()) -> client_reply().
kdelete(Ensemble, Key, Timeout) ->
    kdelete(node(), Ensemble, Key, Timeout).

-spec kdelete(node(),_,_,timeout()) -> client_reply().
kdelete(Node, Ensemble, Key, Timeout) ->
    maybe(Node,
          fun() ->
              translate(riak_ensemble_peer:kdelete(Node, Ensemble, Key, Timeout))
          end).

%%%===================================================================

-spec ksafe_delete(_,_,_,timeout()) -> client_reply().
ksafe_delete(Ensemble, Key, Obj, Timeout) ->
    ksafe_delete(node(), Ensemble, Key, Obj, Timeout).

-spec ksafe_delete(node(),_,_,_,timeout()) -> client_reply().
ksafe_delete(Node, Ensemble, Key, Obj, Timeout) ->
    maybe(Node,
          fun() ->
              translate(riak_ensemble_peer:ksafe_delete(Node, Ensemble, Key,
                                                        Obj, Timeout))
          end).

%%%===================================================================

%% TODO: Change riak_ensemble_peer to use {error, X} and remove translation
-spec translate(failed | timeout | unavailable | {ok, obj()}) -> client_reply().
translate(Result) ->
    case Result of
        unavailable ->
            {error, unavailable};
        timeout ->
            {error, timeout};
        failed ->
            {error, failed};
        {ok, _Obj} ->
            %% TODO: This may be "notfound" object and we check in riak_client.
            %%       Perhaps build this logic into peer to return {error, notfound}?
            Result
    end.

-spec maybe(node(), fun()) -> client_reply().
maybe(Node, Fun) when Node =:= node() ->
    case riak_ensemble_manager:enabled() of
        true ->
            Fun();
        _ ->
            {error, unavailable}
    end;
maybe(_Node, Fun) ->
    Fun().
