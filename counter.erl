%% - Counter module
%% - The counter module creates a counter, its value can be incremented and retrieved.
-module(counter).

-export([start/0, loop/1, increment/1, value/1, stop/1, set/2]).

%% Start counter in 1
start() ->
    spawn(counter, loop, [1]).

loop(Val) ->
    receive
	increment ->
	    loop(Val+ 1);
	{From, value} ->
	    From ! {self(), Val},
	    loop(Val);
	{set, Value} ->
	    loop(Value);
	stop ->
	    true;
	_ ->
	    loop(Val)
    end.

%% Increment counter by one
increment(Counter) ->
    Counter ! increment.

%% Set counter 
set(Counter,Val) ->
    Counter ! {set, Val}.

%% Retrieve value of counter
value(Counter) ->
    Counter ! {self(), value},
    receive
	{Counter, Value} ->
	    Value
    end.

%% Stop counter 
stop(Counter) ->
    Counter ! stop.

