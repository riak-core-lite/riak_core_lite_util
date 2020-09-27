-module(riak_core_quorum_statem_SUITE).
-export([all/0]).
-export([all_reply/1, missing_reply/1]).

all() -> [all_reply, missing_reply].

flush() -> flush([]).

flush(Accum) ->
    receive V -> flush([V | Accum])
    after 500 ->
              lists:reverse(Accum)
    end.

all_reply(_Config) ->
    Fn = fun(#{pid := Pid, ref := Ref}) ->
                 Pid ! {Ref, 1},
                 Pid ! {Ref, 2},
                 Pid ! {Ref, 3}
         end,
    Ref = make_ref(),
    Opts = #{fn => Fn, w => 3, wait_timeout_ms => 100, ref => Ref, from => self()},
    {ok, _Pid} = riak_core_quorum_statem:run(Opts),
    [{Ref, {ok, #{reason := finished, result := Result}}}] = flush(),
    [1, 2, 3] = lists:sort(Result).

missing_reply(_Config) ->
    % W = 4 and 3 results sent
    Fn = fun(#{pid := Pid, ref := Ref}) ->
                 Pid ! {Ref, 1},
                 Pid ! {Ref, 2},
                 Pid ! {Ref, 3}
         end,
    Ref = make_ref(),
    Opts = #{fn => Fn, w => 4, wait_timeout_ms => 100, ref => Ref, from => self()},
    {ok, _Pid} = riak_core_quorum_statem:run(Opts),
    [{Ref, {error, #{reason := timeout, result := Result}}}] = flush(),
    [1, 2, 3] = lists:sort(Result).
