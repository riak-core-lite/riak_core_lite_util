-module(riak_core_quorum_statem).

-behaviour(gen_statem).

-export([start_link/1, run/1, quorum_request/6]).
-export([init/1, callback_mode/0, terminate/3]).
-export([waiting/3]).

start_link(Opts) ->
    gen_statem:start_link(?MODULE, Opts, []).

run(Opts) ->
    {ok, Pid} = start_link(Opts),
    gen_statem:cast(Pid, start),
    {ok, Pid}.

quorum_request(Key, Command, N, ServiceMod, VNodeMasterMod, Opts) ->
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, N, ServiceMod),
    Fn =
        fun (#{ref := Ref, pid := Pid}) ->
                riak_core_vnode_master:command(Preflist, Command, {raw, Ref, Pid}, VNodeMasterMod)
        end,
    run(Opts#{fn => Fn}).

%

init(#{fn := Fn, w := W, wait_timeout_ms := WaitTimeoutMs, ref := Ref, from := From}) ->
    Data =
        #{fn => Fn,
          w => W,
          cur_w => W,
          wait_timeout_ms => WaitTimeoutMs,
          ref => Ref,
          from => From,
          result => []},
    {ok, waiting, Data}.

callback_mode() ->
    state_functions.

%

waiting(cast, start, Data) ->
    #{fn := Fn, wait_timeout_ms := WaitTimeoutMs, ref := Ref} = Data,
    Fn(#{pid => self(), ref => Ref}),
    {keep_state, Data, [{state_timeout, WaitTimeoutMs, waiting}]};
waiting(info, {Ref, ResVal}, Data) ->
    #{ref := Ref, cur_w := CurW, from := From, result := Result} = Data,
    NewW = CurW - 1,
    NewResult = [ResVal | Result],
    NewData = Data#{cur_w := NewW, result := NewResult},
    case NewW of
        0 ->
            Res = {Ref, {ok, #{reason => finished, result => NewResult}}},
            From ! Res,
            {stop, normal, NewData};
        _ ->
            {keep_state, NewData}
    end;
waiting(state_timeout, waiting, Data) ->
    #{ref := Ref, result := Result, from := From} = Data,
    Res = {Ref, {error, #{reason => timeout, result => Result}}},
    From ! Res,
    {stop, normal, Data}.

terminate(_Reason, _State, _Data) ->
    ok.
