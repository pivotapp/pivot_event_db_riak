-module(pivot_mab_arms_db_riak_test).

-include_lib ("eunit/include/eunit.hrl").

basic_test() ->
  ok = riakou:start(),
  riakou:start_link(<<"riak://localhost">>),

  % We have to wait for us to connect to riak... :/
  timer:sleep(200),

  Env = <<"test">>,
  App = <<"app">>,
  Events = [{<<"add to cart">>, 0.7}, {<<"leave site">>, 0.0}, {<<"purchase">>, 1.0}],

  [ok, ok, ok] = [pivot_event_db_riak:set(Env, App, Event, Score) || {Event, Score} <- Events],

  [{ok, Score} = pivot_event_db_riak:get(Env, App, Event) || {Event, Score} <- Events],

  {ok, ReturnedEvents} = pivot_event_db_riak:list(Env, App),

  true = check_set_equality(Events, ReturnedEvents),

  [ok, ok, ok] = [pivot_event_db_riak:remove(Env, App, Event) || {Event, _} <- Events],

  {ok, []} = pivot_event_db_riak:list(Env, App),

  ok.

check_set_equality(List1, List2) ->
  gb_sets:is_empty(gb_sets:difference(gb_sets:from_list(List1), gb_sets:from_list(List2))).
