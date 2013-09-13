-module(pivot_event_db_riak).

-export([get/3]).
-export([set/4]).
-export([list/2]).

-define(BUCKET(Env), <<"pivot-event-", Env/binary>>).
-define(KEY(App, Event), <<App/binary, ":", Event/binary>>).

-include_lib("riakc/include/riakc.hrl").

get(Env, App, Event) ->
  case riakou:do(get, [?BUCKET(Env), ?KEY(App, Event)]) of
    {ok, Obj} ->
      case riakc_obj:value_count(Obj) of
        1 ->
          {ok, binary_to_float(riakc_obj:get_value(Obj))};
        _ ->
          Values = riakc_obj:get_values(Obj),
          Value = pick_highest(Values, 0),
          %% do read repair
          spawn(?MODULE, set, [Env, App, Event, Value]),
          {ok, Value}
      end;
    Error ->
      Error
  end.

set(Env, App, Event, Value) ->
  Obj = riakc_obj:new(?BUCKET(Env), ?KEY(App, Event), float_to_binary(Value, [compact, {decimals, 10}])),
  MD1 = riakc_obj:get_update_metadata(Obj),
  MD2 = riakc_obj:set_secondary_index(MD1, [
    {{binary_index, "app"}, [App]}
  ]),
  Obj2 = riakc_obj:update_metadata(Obj, MD2),
  riakou:do(put, [Obj2]).

list(Env, App) ->
  case riakou:do(get_index, [?BUCKET(Env), {binary_index, "app"}, App]) of
    {ok, {keys, Keys}} ->
      get_values(Env, App, Keys);
    {ok, Rec} ->
      get_values(Env, App, Rec?INDEX_RESULTS.keys);
    Error ->
      Error
  end.

pick_highest([], Highest) ->
  Highest;
pick_highest([BinValue, Values], Highest) ->
  Value = binary_to_float(BinValue),
  case Value > Highest of
    true ->
      pick_highest(Values, Value);
    _ ->
      pick_highest(Values, Highest)
  end;
pick_highest([_|Values], Highest) ->
  pick_highest(Values, Highest).

get_values(Env, App, Keys) ->
  Pairs = [begin
    AppLength = byte_size(App),
    <<App:AppLength/binary, ":", StrippedKey/binary>> = Key,
    {ok, Value} = ?MODULE:get(Env, App, StrippedKey),
    {StrippedKey, Value}
  end || Key <- Keys],
  {ok, Pairs}.
