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
    WaitMgrPid = spawn_link(fun() -> wait_manager(ServerPid, gb_trees:empty()) end),
    TSGenerator = counter:start(),
    server_loop(dict:new(),StorePid, ObjectsMgrPid, WaitMgrPid, TSGenerator, gb_trees:empty(), queue:new()).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variabe a, b, c and d 
%% - Last_Event: Indicates if a transaction was committed/aborted during last execution.
%%   Contains the timestamp of the transaction. Zero if no transaction was aborted/committed.
server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Events) ->
    case queue:peek(Events) of
	empty ->
	    true
	    %no events, get the next message
	    ;	
	{value, {EventType, Tts}} ->
	    case EventType of
		commited ->
		    io:format("Event: t.~p commited or can commit.~n",[Tts]);
		aborted ->
		    io:format("Event: t.~p aborted.~n",[Tts])
		end,
	    case EventType of
		commited ->
		    case gb_trees:lookup(Tts, Transactions) of
			{value, {_,'finished',SetOfWrites,_}} ->
			    VariablesToCommit = sets:to_list(SetOfWrites),
			    lists:foreach(
			      fun(V) ->
				      %set the values of the tentative versions become the values of objects
				      ObjectsMgrPid ! {getObject,V},
				      {_, _, RTSV, VersionsV} = receive {object, O} -> O end,
				      {value, NewVal} = gb_trees:lookup(Tts,VersionsV),
				      %%% io:format("\t**** Update ~p from version ~p! with value ~p ~n", [V, Tts, NewVal]),
				      ObjectsMgrPid ! {updateObject, V, {NewVal, Tts, RTSV, VersionsV}}, 
				      %delete the version?						
				      StorePid ! {commit, V, NewVal} %also at the store
			      end,
			      VariablesToCommit),
			    io:format("\tt.~p committed.~n",[Tts]),
			    io:format("\t"),
			    StorePid ! {printNow, self()},
			    true = receive {store_loop, print_ok} -> true end,
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
					gb_trees:delete(Tts,Transactions),Events)
				;
			none ->
			    io:format("\t**** Commited already handled ~n"),
			    true
						%ok, this means I already handled the commit
		    end;
		aborted ->
		    true %nothing special	    
	    end,
	    %see if a next committed transaction was waiting to commit
	    io:format("\t**** Can another transaction commit?~n"),
	    TransactionsL = gb_trees:keys(Transactions),
	    case TransactionsL of
		[] ->
		    none
		    ,io:format("\t**** No, no one can~n")
		    ; %no other transactions, nothing to do
		[NextT|_] ->
		    {value, {_,State,_,_}} = gb_trees:lookup(NextT, Transactions),
		    case State of
			'finished' ->
			    io:format("\t.~p can commit now.~n",[NextT]),
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
					gb_trees:delete(NextT,Transactions),queue:in({commited,NextT},Events));
			'going-on' ->
			    true; %cannot commit if not finished
			'waiting' ->
			    true %cannot commit if not finished	    
		    end
	    end,
	    io:format("\t**** Is there anything left?~n"),
	    WaitMgrPid ! {dequeue,Tts},
		receive 
		    no_action ->
			%nothing to do, we are done with the event
			io:format("\tNothing else to do with this event~n."),
			server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,queue:drop(Events));
		    {old, {action, A_Client, A_Act}} ->
			A_Tc = dict:fetch(A_Client,Clients),
			io:format("Action ~p from client ~p in transacion ~p was waiting.~n", [A_Act, A_Client, A_Tc]),
			{A_TransactionsUpdated, A_Status} =
			    case A_Act of 
				{read,_} -> 
				    do_read(Clients, ObjectsMgrPid, WaitMgrPid, {action, A_Client, A_Act}, Transactions);
				{write,_,_} -> 
				    do_write(Clients, ObjectsMgrPid, {action, A_Client, A_Act}, Transactions)
			    end,
			case A_Status of 
			    abort ->
				A_Client ! {abort, self()},
				server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,
					    TSGenerator,A_TransactionsUpdated,queue:in({aborted,A_Tc},Events));			  
			    continue ->
				{value, {A_Tc_Client,_,A_Tc_WriteSets,A_Tc_WaitingFor}} = 
				    gb_trees:lookup(A_Tc, A_TransactionsUpdated),
				server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
					    gb_trees:update(A_Tc,{A_Tc_Client,'going-on',A_Tc_WriteSets,A_Tc_WaitingFor},
							    A_TransactionsUpdated),Events)
			end;			
		    {old, {confirm, A_Client}} ->
			{A_TransactionsUpdated, A_Status, A_Event} = 
			    do_confirm(Clients, {confirm, A_Client}, Transactions),
			case A_Status of
			    continue ->
				A_Client ! {committed, self()}, %a transaction is always able to commit (client needs not to wait)
				case A_Event of
				    none ->
					%transacction has to wait to commit 
					server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,A_TransactionsUpdated,Events);
				    {commited, _} ->
					%transaction has committed, notify
					server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,A_TransactionsUpdated,queue:in(A_Event,Events))
				end;
			    wait ->
				none = A_Event,
				A_Tc = dict:fetch(A_Client,Clients),
				{value,{A_Client,_,_,A_WaitingFor}} = gb_trees:lookup(A_Tc, Transactions),
				io:format("\t~w must wait, transaction ~p is waiting for transacion ~p\~n",
					  [{confirm, A_Client}, A_Tc, A_WaitingFor]),
				WaitMgrPid ! {enqueue, {confirm, A_Client}, A_WaitingFor},
				server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,A_TransactionsUpdated,Events)
			end
		end
    end,

    %% Invariant at this point: the events queue is empty.
    true = queue:is_empty(Events),
    io:format("** server is waiting **~n"),
    receive
	{login, MM, Client} -> 
	    % Client login
	    MM ! {ok, self()},
	    io:format("New client has joined the server: ~p.~n", [Client]),
	    StorePid ! {printNow, self()},
	    true = receive {store_loop, print_ok} -> true end,
	    % ObjectsMgrPid ! {print, self()},
	    server_loop(dict:store(Client,0,Clients),StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Events);
	{close, Client} -> 
	    % Client logout
	    io:format("Client ~p has left the server.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(dict:erase(Client,Clients),StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Events);
	{request, Client} -> 
	    % A transaction is started.
	    % The user enters the run command in the client window. 
	    % This is marked by sending a request to the server.
	    TS = counter:value(TSGenerator), 
	    counter:increment(TSGenerator),  
	    ClientsUpdated = dict:store(Client,TS,Clients),
	    TransactionsUpdated = gb_trees:insert(TS,{Client,'going-on',sets:new(),none},Transactions),
	    % For each transaction we keep {Client,Status,SetOfVariablesItHasToCommit,TransactionItIsWaitingFor}
	    io:format("Client ~p has began transaction ~p .~n", [Client, TS]),
	    Client ! {proceed, self()},
	    server_loop(ClientsUpdated,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Events);
	{action, Client, Act} ->
	    %%% io:format("**ENTER ACTION**"),
	    % The client sends the actions of the list (the transaction) one by one 
	    % in the order they were entered by the user.
	    Tc = dict:fetch(Client,Clients),
	    io:format("Received ~p from client ~p in transacion ~p.~n", [Act, Client, Tc]),
	    {value, {Client,TcStatus,_,WaitingFor}} = gb_trees:lookup(Tc, Transactions),
	    %%% io:format("** Status: ~p.~n", [TcStatus]),
	    case TcStatus of
		'going-on' -> 
		    %perform action
		    %%% io:format("\t**ENTER going-on transaction**~n"),
		    {TransactionsUpdated, Status} =
			case Act of 
			    {read,_} -> 
				do_read(Clients, ObjectsMgrPid, WaitMgrPid, {action, Client, Act}, Transactions);
			    {write,_,_} -> 
				do_write(Clients, ObjectsMgrPid, {action, Client, Act}, Transactions)
			end,
		    case Status of 
			abort ->
			    Client ! {abort, self()},
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,queue:in({aborted,Tc},Events));
			continue ->
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Events)
		    end;
		'waiting' -> 
		    %%% io:format("\t**ENTER waiting transaction**~n"),
		    %enqueue action
		    %(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event)
		    io:format("\t~w must wait, transaction ~p is waiting for transacion ~p\~n",
			     [Act, Tc, WaitingFor]),
		    WaitMgrPid ! {enqueue, {action, Client, Act}, WaitingFor},
		    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Events)
	    end;

        {confirm, Client} -> 
	    %%% io:format("**ENTER CONFIRM**"),
	    % Once, all the actions are sent, the client sends a confirm message 
	    % and waits for the server reply.
	    {TransactionsUpdated, Status, Event} = 
		do_confirm(Clients, {confirm, Client}, Transactions),
	    case Status of
		continue ->
		    Client ! {committed, self()}, %a transaction is always able to commit because all the operations
						  %are checked for consistency with those of earlier transactions
						  %before being carried out
		    case Event of
			none ->
			    %transacction has to wait to commit, there is no event, the client needs not to wait
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Events);
			{commited, _} ->
			    %transaction commited, notify
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,queue:in(Event,Events))
		    end;
		wait ->
		    none = Event,
		    Tc = dict:fetch(Client,Clients),
		    {value, {Client,_,_,WaitingFor}} = gb_trees:lookup(Tc, Transactions),
		    io:format("\t~w must wait, transaction ~p is waiting for transacion ~p\~n",
			      [{confirm, Client}, Tc, WaitingFor]),
		    %transaction is waiting, the confirm MESSAGE has to wait too
		    %enqueue action
		    WaitMgrPid ! {enqueue, {confirm, Client}, WaitingFor},
		    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Events)
	    end
    after 50000 ->
	    case all_gone(Clients) of
		true -> exit(normal);    
		false -> server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Events)
	    end
    end.

%% - Read function
do_read(Clients, ObjectsMgrPid, WaitMgrPid, {action, Client, {read, Var}}, Transactions) ->
    Tc = dict:fetch(Client,Clients),
    io:format("\tValidating read rule~n"),
    ObjectsMgrPid ! {getObject,Var},
    {Val, WTS, _, Versions} = receive {object, O} -> O end,
    case Tc > WTS of
        true ->
            io:format("\t\tValid~n"),
            DSelected = maxLeqList(Tc, gb_trees:keys(Versions)),
	    TransactionsUpdated =
            case ((DSelected =:= WTS) or (DSelected =:= Tc)) of %Is DSelected commited or 
						          %the transaction has already written its own version 
                true ->
		    io:format("\t\t\tPerform read operation of t.~p in version ~p of var.'~p'~n",
                              [Tc, DSelected,Var]),					
		    %update read timestamp
		    ObjectsMgrPid ! {updateObject, Var, {Val, WTS, Tc, Versions}}, 
		    io:format("\t\t\tt.~p reads ~p = ~p~n",[Tc, Var, Val]),
		    Transactions;
                false -> 
		    io:format("\t\t\tWait until the transaction that made version ~p of '~w' commits or aborts.~n", 
                              [DSelected, Var]),
		    %cannot perform read operation, transaction must wait
		    %enqueue operation to be carried out after Dselected finishes
		    WaitMgrPid ! {enqueue, {action, Client, {read, Var}}, DSelected},
		    %set the status of transaction to waiting
		    %%% io:format("HEY1: ~w", [gb_trees:lookup(Tc, Transactions)]),
		    {value, {Client,'going-on',WriteSets,_}} = gb_trees:lookup(Tc, Transactions),
		    gb_trees:update(Tc,{Client,'waiting',WriteSets,DSelected},Transactions)
            end,				
            {TransactionsUpdated, continue}; %no change on transactions
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\tRead on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
            {gb_trees:delete(Tc,Transactions), abort} %the transaction is over
    end.

%% - Write function
do_write(Clients, ObjectsMgrPid, {action, Client, {write,Var,Value}}, Transactions) ->
    Tc = dict:fetch(Client,Clients),
    io:format("\tValidating write rule~n"),
    ObjectsMgrPid ! {getObject,Var},
    {Val, WTS, RTS, Versions} = receive {object, O} -> O end,
    case ((Tc >= RTS) and (Tc > WTS)) of
        true ->
            io:format("\t\t Valid~n"),
            io:format("\t\t\tPerform write operation of t.~p in '~p' ~n",[Tc, Var]),
            case gb_trees:lookup(Tc,Versions) of
                none ->
		    ObjectsMgrPid ! {updateObject, Var, 
                                     {Val, WTS, RTS, gb_trees:enter(Tc, Value, Versions)}};
                {value, _} ->
		    ObjectsMgrPid ! {updateObject, Var, 
                                     {Val, WTS, RTS, gb_trees:update(Tc, Value, Versions)}}
            end,
	    io:format("\t\t\tt.~p writes ~p on v.~p of ~p~n",[Tc, Value, Tc, Var]),
            {value, {Client,State,WriteOps,WatingFor}} = gb_trees:lookup(Tc, Transactions),
            %keep wich variables I have to commit with the transaction
            TransactionsUpdt = gb_trees:update(Tc, {Client,State,sets:add_element(Var,WriteOps),WatingFor}, Transactions), 
            {TransactionsUpdt, continue};
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\t Write on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
            {gb_trees:delete(Tc,Transactions), abort} %the transaction is over
    end.

%% - Confirm function
do_confirm(Clients, {confirm, Client}, Transactions)->
    Tc = dict:fetch(Client,Clients),
    io:format("Client ~p has ended transaction ~p .~n", [Client, Tc]),
    {value, {Client,TcStatus,WriteSets,WaitingFor}} = gb_trees:lookup(Tc, Transactions),
    case TcStatus of
	'going-on' -> 
	    %perform action
	    io:format("\tWrite data of t.~p to permanent storage.~n",[Tc]),
	    %a transaction is always able to commit
	    %nevertheless, committed versions of each object must be created in timestamp order, so we
	    %need to check	    
	    TransactionsL = gb_trees:keys(Transactions),
	    {TransactionsUpdated, Event} = case TransactionsL of					       
					       [Tc|_] -> %no need to wait to commit
						   io:format("\tt.~p finished and can commit.~n",[Tc]),
						   %{gb_trees:delete(Tc,Transactions), {commited, Tc}};
						   {gb_trees:update(Tc,{Client,'finished',WriteSets,WaitingFor},
								    Transactions), {commited, Tc}};
					       [X|_] ->
						   io:format("\tt.~p finished but cannot commit, must wait.~n",[Tc]),
						   io:format("\t(For example, ~p has neither committed nor aborted)~n",[X]),
						   {gb_trees:update(Tc,{Client,'finished',WriteSets,WaitingFor},Transactions), none}
					   end,
	    {TransactionsUpdated, continue, Event};
	'waiting' -> 	    
	    %transaction is waiting, the confirm message has to wait too
	    {Transactions, wait, none}
    end.

%% - The values are maintained here
store_loop(ServerPid, Database) -> 
    receive
	{printNow, ServerPid} -> 
            io:format("Database status: ~w.~n",[Database]),
	    ServerPid ! {store_loop, print_ok},
            store_loop(ServerPid,Database);
        {print, ServerPid} -> 
            io:format("Database status: ~w.~n",[Database]),
            store_loop(ServerPid,Database);
	{commit, Var, Val} ->
	    %%io:format("\tNew value at the store: '~p' = ~p.~n",[Var,Val]),
	    NewDB = updateDB(Database, {Var,Val}),
	    store_loop(ServerPid, NewDB)				   
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

%% - A manager for the waiting transactions
wait_manager(ServerPid, Queue_Tree) ->
    receive
	{enqueue, Action, Transaction} ->
	    ActionQueue = 
	    case gb_trees:lookup(Transaction, Queue_Tree) of
		none ->
		    queue:new();
		{value, Q} ->
		    Q
		end,
	    UpdatedActionQueue = queue:in({old, Action}, ActionQueue),
	    UpdatedQueue_Tree = gb_trees:update(Transaction, UpdatedActionQueue, Queue_Tree),
	    wait_manager(ServerPid, UpdatedQueue_Tree);
	{dequeue, Transaction} ->
	    case gb_trees:lookup(Transaction, Queue_Tree) of
		none ->		    
		    ServerPid ! no_action, %no action is waiting for Transaction
		    wait_manager(ServerPid, Queue_Tree)
		    ;
		{value, Q} ->
		    case queue:out(Q) of
			{{value,FirstAction}, QUpdted} -> 
			    Queue_Tree_Updted = gb_trees:update(Transaction, QUpdted, Queue_Tree),
			    ServerPid ! FirstAction,
			    wait_manager(ServerPid, Queue_Tree_Updted)
			    ;
                         {empty, Q} ->
			    Queue_Tree_Updted = gb_trees:delete(Transaction, Queue_Tree),
			    ServerPid ! no_action,  %no action is waiting for Transaction
			    wait_manager(ServerPid, Queue_Tree_Updted)
		    end		    
	    end	    
    end.



%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Not using this anymore...
%% - Low level function to handle lists
%% add_client(C,T) -> [C|T].

%% remove_client(_,[]) -> [];
%% remove_client(C, [C|T]) -> T;
%% remove_client(C, [H|T]) -> [H|remove_client(C,T)].

%% Find the maximun value leq than X in an ordered list
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

updateDB(Database, {Var,Val}) ->
    updateDB_aux({Var,Val}, Database, []).
updateDB_aux({_,_}, [], NewDB) ->
    NewDB;
updateDB_aux({Var,Val},[{Var,_}|T],NewDB) ->
    updateDB_aux({Var,Val},T,[{Var,Val}|NewDB]);
updateDB_aux({Var,Val},[H|T],NewDB) ->
    updateDB_aux({Var,Val},T,[H|NewDB]).

all_gone([]) -> true;
all_gone(_) -> false.


%% Deletes all element in the list where C is the client
% Why this is not used anymore?
%del_all_client(C, List) ->
%  [{Client, Tc, Act} || {Client, Tc, Act} <- List, Client =/= C].
