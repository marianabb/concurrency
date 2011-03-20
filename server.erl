%% - Server module
%% - The server module creates a parallel registered process by spawning a process which 
%% evaluates initialize(). 
%% The function initialize() does the following: 
%%      1/ It makes the current process as a system process in order to trap exit.
%%      2/ It creates a process evaluating the store_loop() function.
%%      4/ It executes the server_loop() function.

-module(server).

-export([start/0]).

%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() -> 
    register(transaction_server, spawn(fun() ->
                                               process_flag(trap_exit, true),
                                               Val= (catch initialize()),
                                               io:format("Server terminated with:~p~n",[Val])
                                       end)).

initialize() ->
    process_flag(trap_exit, true),
    Initialvals = [{a,0},{b,0},{c,0},{d,0}], %% All variables are set to 0
    InitialObjects = dict:from_list([{a,{0,0,0,gb_trees:empty()}},{b,{0,0,0,gb_trees:empty()}},
		      {c,{0,0,0,gb_trees:empty()}},{d,{0,0,0,gb_trees:empty()}}]), 
    %% Object: name -> {Values, WTS, RTS, Versions}
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    ObjectsMgrPid = spawn_link(fun() -> object_manager(ServerPid,InitialObjects) end),
    TSGenerator = counter:start(),
    server_loop(dict:new(),StorePid, ObjectsMgrPid, TSGenerator, gb_trees:empty()).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d 
server_loop(Clients,StorePid,ObjectsMgrPid,TSGenerator,Transactions) ->
    receive
	{login, MM, Client} -> 
	    % Client login
	    MM ! {ok, self()},
	    io:format("New client has joined the server: ~p.~n", [Client]),
	    StorePid ! {print, self()},
	    ObjectsMgrPid ! {print, self()},
	    server_loop(dict:store(Client,0,Clients),StorePid,ObjectsMgrPid,TSGenerator,Transactions);
	{close, Client} -> 
	    % Client logout
	    io:format("Client ~p has left the server.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(dict:erase(Client,Clients),StorePid,ObjectsMgrPid,TSGenerator,Transactions);
	{request, Client} -> 
	    % A transaction is started.
	    % The user enters the run command in the client window. 
	    % This is marked by sending a request to the server.
	    TS = counter:value(TSGenerator), 
	    counter:increment(TSGenerator),  
	    ClientsUpdated = dict:store(Client,TS,Clients),
	    TransactionsUpdated = gb_trees:insert(TS,{Client,'going-on',sets:new()},Transactions),
	    io:format("Client ~p has began transaction ~p .~n", [Client, TS]),
	    Client ! {proceed, self()},
	    server_loop(ClientsUpdated,StorePid,ObjectsMgrPid,TSGenerator,TransactionsUpdated);
	{action, Client, Act} ->
	    % The client sends the actions of the list (the transaction) one by one 
	    % in the order they were entered by the user.
	    Tc = dict:fetch(Client,Clients),
	    io:format("Received ~p from client ~p in transacion ~p.~n", [Act, Client, Tc]),
	    TransactionsUpdated = 
		case Act of 
		    {read,Var} -> 
			io:format("\tValidating read rule~n"),
			ObjectsMgrPid ! {getObject,Var},
			{Val, WTS, _, Versions} = receive {object, O} -> O end,
			case Tc > WTS of
			    true ->
				io:format("\t\tValid~n"),
				DSelected = maxLeqList(Tc, gb_trees:keys(Versions)),
				case DSelected =:= WTS of
				    true ->
					io:format("\t\t\tPerform read operation of ~p in version ~p of ~p~n",[Tc, DSelected,Var]),					
					ObjectsMgrPid ! {updateObject, Var, 
							 {Val, WTS, Tc, Versions}}, %update read timestamp
					io:format("\t\t\tClient ~p reads ~p = ~p~n",[Tc, Var, Val]);				    
				    false ->
					io:format("\t\t\tWait until the transaction that made version ~p of '~w' commits or aborts.~n", [DSelected, Var])
				        %Client blocks, but server should not block!
				end,
				Transactions; %no change on transactions
			    false ->
				io:format("\t\tNot valid~n"),
				io:format("\t\t\tRead on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
				gb_trees:delete(Tc,Transactions) %the transaction is over
			end;
		    {write,Var,Value} -> 
			io:format("\tValidating write rule~n"),
			ObjectsMgrPid ! {getObject,Var},
			{Val, WTS, RTS, Versions} = receive {object, O} -> O end,
			case ((Tc >= RTS) and (Tc > WTS)) of
			    true ->
				io:format("\t\t Valid~n"),
				io:format("\t\t\t Perform write operation of ~p in ~p ~n",[Tc, Var]),
				case gb_trees:lookup(Tc,Versions) of
				    none ->
					ObjectsMgrPid ! {updateObject, Var, 
							 {Val, WTS, RTS, gb_trees:enter(Tc, Value, Versions)}};
				    {value, _} ->
					ObjectsMgrPid ! {updateObject, Var, 
							 {Val, WTS, RTS, gb_trees:update(Tc, Value, Versions)}}
				end,
				Transactions; %no change on transactions
			    false ->
				io:format("\t\tNot valid~n"),
				io:format("\t\t\t Write on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
				Client ! {abort, self()},
				gb_trees:delete(Tc,Transactions) %the transaction is over
		    end
	    end,
	    server_loop(Clients,StorePid,ObjectsMgrPid,TSGenerator,TransactionsUpdated);
	{confirm, Client} -> 
	    % Once, all the actions are sent, the client sends a confirm message 
	    % and waits for the server reply.
	    io:format("Client ~p has ended transaction ~p .~n", [Client, dict:fetch(Client,Clients)]),
	    Client ! {abort, self()},
	    server_loop(Clients,StorePid,ObjectsMgrPid,TSGenerator,Transactions)
    after 50000 ->
	case all_gone(Clients) of
	    true -> exit(normal);    
	    false -> server_loop(Clients,StorePid,ObjectsMgrPid,TSGenerator,Transactions)
	end
    end.

%% - The values are maintained here
store_loop(ServerPid, Database) -> 
    receive
	{print, ServerPid} -> 
	    io:format("Database status:~n~p.~n",[Database]),
	    store_loop(ServerPid,Database)
    end.

%% - The coordinator
object_manager(ServerPid, Objects) ->
    receive
	{print, ServerPid} -> 
	    io:format("Objects status:~n~p.~n",[Objects]),
	    object_manager(ServerPid,Objects);
	{getObject, VarName} ->	    
	    ServerPid ! {object, dict:fetch(VarName, Objects)},
	    object_manager(ServerPid,Objects);
	{updateObject, VarName, Object} ->
	    object_manager(ServerPid,dict:store(VarName, Object, Objects))
    end.
    
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Not using this anymore...
%% - Low level function to handle lists
%% add_client(C,T) -> [C|T].

%% remove_client(_,[]) -> [];
%% remove_client(C, [C|T]) -> T;
%% remove_client(C, [H|T]) -> [H|remove_client(C,T)].

% find the maximun value leq than X in an ordered list
maxLeqList(_, []) ->
    %none;
    0; %intial version
maxLeqList(X, [H|T]) ->
%    maxLeqList_aux(X, none, [H|T]).
    maxLeqList_aux(X, 0, [H|T]).

maxLeqList_aux(_, MaxLeqSoFar, []) ->
    MaxLeqSoFar;
maxLeqList_aux(X, MaxLeqSoFar, [H|T]) ->
    case H =< X of
	true ->
	    maxLeqList_aux(X, H, T);
	false ->
	    MaxLeqSoFar
    end.
	    

all_gone([]) -> true;
all_gone(_) -> false.
