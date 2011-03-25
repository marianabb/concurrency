%% - Server module
%% - The server module creates a parallel registered process by spawning a
%%   process which evaluates initialize(). 
%% The function initialize() does the following: 
%%      1/ It makes the current process as a system process in order to trap
%%         exit. 
%%      2/ It creates a process evaluating the store_loop() function.
%%      4/ It executes the server_loop() function.

-module(server).

-export([start/0]).

%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() -> 
    register(transaction_server, spawn(fun() ->
                                               process_flag(trap_exit,
							    true), 
                                               Val= (catch initialize()), 
                                               io:format("Server terminated
                                                          with:~p~n",[Val]) 
                                       end)).

initialize() ->
    process_flag(trap_exit, true),
    Initialvals = [{a,0},{b,0},{c,0},{d,0}], %% All variables are set to 0
    InitialObjects =
	dict:from_list([{a,{0,0,0,gb_trees:empty()}},
			{b,{0,0,0,gb_trees:empty()}}, 
			{c,{0,0,0,gb_trees:empty()}},
			{d,{0,0,0,gb_trees:empty()}}]),
    %% Object: name -> {Values, WTS, RTS, Versions}
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    %% Manager for values and versions
    ObjectsMgrPid = spawn_link(fun() -> object_manager(ServerPid,
						       InitialObjects) end), 
    %% Manager for waiting actions and transactions
    WaitMgrPid = spawn_link(fun() -> wait_manager(ServerPid,
						  gb_trees:empty()) end), 
    %% Timestamp generator
    TSGenerator = counter:start(),
    server_loop(dict:new(),StorePid, ObjectsMgrPid, WaitMgrPid, TSGenerator,
		gb_trees:empty(), queue:new()). 
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store
%%   holding the values of the global variabe a, b, c and d 
%% - Events: Queue that contains the transactions that have
%%   committed/aborted during last execution. 
%%   CONTAINS THE EVENT TYPES and the timestamps of the transactions.
%% - Transactions: Tree that contains the transactions, their status, set of
%%   writes and timestamps. 
server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
	    Transactions, Events)->
    %% Before processing any new messages, check the Events queue
    case queue:peek(Events) of
	empty ->
	    %%no events, get the next message
	    true;	
	{value, {EventType, Tts}} ->
	    case EventType of
		committed -> 
		    io:format("Event: t.~p committed or can
                               commit.~n",[Tts]), 
		    true;
		aborted ->
		    io:format("Event: t.~p aborted.~n",[Tts]),
		    true
	    end,
	    case EventType of
		committed ->
		    case gb_trees:lookup(Tts, Transactions) of
			{value, {_,'finished',SetOfWrites,_}} ->
			    VariablesToCommit = sets:to_list(SetOfWrites),
			    lists:foreach(
			      fun(V) ->
				      %% Let the values of the tentative
				      %% versions become the values of
				      %% objects 
				      ObjectsMgrPid ! {getObject,V},
				      {_, _, RTSV, VersionsV} = 
					  receive {object, O} -> O end,
				      {value, NewVal} =
					  gb_trees:lookup(Tts,VersionsV), 
				      %%%%%%DEBUG2 io:format("\t**** Update ~p from
                                      %%%%%%DEBUG2            version ~p! with value ~p
                                      %%%%%%DEBUG2            ~n", [V, Tts, NewVal]), 
				      ObjectsMgrPid ! {updateObject, V,
						       {NewVal, Tts, RTSV,
							VersionsV}},
				      %% TO-DO Delete the version. 
						
                                      %% Also change the value at the store 
				      StorePid ! {commit, V, NewVal} 
			      end, 
			      VariablesToCommit), 
                            io:format("\tt.~p committed.~n",[Tts]),
			    io:format("\t"),
			    StorePid ! {printNow, self()}, 
			    true = receive {store_loop, print_ok} -> true
			    	   end, %% TO-DO Erase 
			    server_loop(Clients,StorePid,ObjectsMgrPid,
					WaitMgrPid,TSGenerator,
					gb_trees:delete(Tts,Transactions), 
					Events)
				;
			none ->
			    %%%%%%DEBUG2 io:format("\t**** Committed already handled ~n"),
			    true
			    %ok, already handled the commit
		    end;
		aborted ->
		    true %nothing special	    
	    end,

	    %% Check if a next committed transaction is waiting to commit
	    %%%%%%DEBUG2 io:format("\t**** Can another transaction commit?~n"),
	    TransactionsL = gb_trees:keys(Transactions),
	    case TransactionsL of
		[] ->
		    none
                    %%%%%%DEBUG2 ,io:format("\t**** No, no one can~n")
		    ; %% No other transactions, nothing to do
		[NextT|_] ->
		    {value, {_,State,_,_}} = 
			gb_trees:lookup(NextT, Transactions),
		    case State of
			'finished' ->
			    io:format("\t.~p can commit now.~n",[NextT]),
			    server_loop(Clients,StorePid,ObjectsMgrPid,
					WaitMgrPid, TSGenerator,
					gb_trees:delete(NextT,Transactions),
					queue:in({committed,NextT},Events));
			'going-on' ->
			    true; %cannot commit if not finished
			'waiting' ->
			    true %cannot commit if not finished	    
		    end
	    end,
	    %%%%%%DEBUG2 io:format("\t**** Is there anything left?~n"),
	    WaitMgrPid ! {dequeue,Tts},
		receive 
		    %% Nothing to do, we are done with the event
		    no_action ->
			io:format("\tNothing else to do with this event~n."),
			server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,
				    TSGenerator,Transactions,
				    queue:drop(Events));
		    %% There is an action waiting. We can execute it now.
		    {old, {action, A_Client, A_Act}} ->
			%% Get the transaction and execute it.
			A_Tc = dict:fetch(A_Client,Clients),
			io:format("Action ~p from client ~p in transacion ~p
                                   was waiting.~n", [A_Act, A_Client, A_Tc]), 

			case gb_trees:lookup(A_Tc, Transactions) of
			    none ->
				io:format("But transacion ~p aborted, so ~w
                                           is ignored.~n", 
					  [A_Tc,A_Act]),
				server_loop(Clients,StorePid,ObjectsMgrPid,
					    WaitMgrPid,TSGenerator,
					    Transactions, Events);			    	    
			    {value, _} ->				
				{A_TransactionsUpdated, A_Status} =
				    case A_Act of 
					{read,_} -> 
					    do_read(Clients, ObjectsMgrPid,
						    WaitMgrPid, 
						    {action, A_Client, A_Act}, 
						    Transactions);
					{write,_,_} -> 
					    do_write(Clients, ObjectsMgrPid,
						     {action, A_Client, A_Act},
						     Transactions)
				    end,
				case A_Status of 
				    abort ->
					%% Abort the transaction and enqueue the event
					A_Client ! {abort, self()},
					server_loop(Clients,StorePid,ObjectsMgrPid,
						    WaitMgrPid, TSGenerator,
						    A_TransactionsUpdated, 
						    queue:in({aborted,A_Tc},Events));
				    continue ->
					%% Obtain the transaction, save it with
					%% status 'going-on' and continue
					%% execution. 
					{value,
					 {A_Tc_Client,_,A_Tc_WriteSets,A_Tc_WaitingFor}}
					    =  
					    gb_trees:lookup(A_Tc,
							    A_TransactionsUpdated),  
					server_loop(Clients,StorePid,ObjectsMgrPid,
						    WaitMgrPid,TSGenerator,
						    gb_trees:update(A_Tc,
								    {A_Tc_Client,
								     'going-on',
								     A_Tc_WriteSets,
								     A_Tc_WaitingFor},
								    A_TransactionsUpdated)
						    ,Events)
				end
			end;		    		    

		    %% There is a confirm action waiting. We attempt to commit.			    
		    {old, {confirm, A_Client}} ->
			A_Tc = dict:fetch(A_Client,Clients),
			case gb_trees:lookup(A_Tc, Transactions) of
			    none ->
				io:format("But transacion ~p aborted so is ignored.~n", 
					  [A_Tc]),
				server_loop(Clients,StorePid,ObjectsMgrPid,
					    WaitMgrPid,TSGenerator,
					    Transactions, Events);			    	    
			    {value, _} ->							
				{A_TransactionsUpdated, A_Status, A_Event} = 
				    do_confirm(Clients, {confirm, A_Client}, 
					       Transactions),
				case A_Status of
				    continue ->
					A_Client ! {committed, self()}, %a
						%transaction is always able
						%to commit (client needs not
						%to wait) 
					case A_Event of
					    none ->
						%% Transaction has to wait to commit
						server_loop(Clients,StorePid,
							    ObjectsMgrPid,WaitMgrPid, 
							    TSGenerator,
							    A_TransactionsUpdated,Events); 
					    {committed, _} ->
						%% Transaction has committed successfully, notify
						server_loop(Clients,StorePid,ObjectsMgrPid,
							    WaitMgrPid, TSGenerator,
							    A_TransactionsUpdated,
							    queue:in(A_Event,Events))
					end;
				    wait ->
					none = A_Event, %% This must be true
					A_Tc = dict:fetch(A_Client,Clients),
					{value,{A_Client,_,_,A_WaitingFor}} = 
					    gb_trees:lookup(A_Tc, Transactions),
					io:format("\t~w must wait, transaction ~p is
                                           waiting for transaction ~p\~n", 
						  [{confirm, A_Client}, A_Tc,
						   A_WaitingFor]), 
					WaitMgrPid ! {enqueue, {confirm, A_Client},
						      A_WaitingFor}, 
					server_loop(Clients,StorePid,ObjectsMgrPid,
						    WaitMgrPid,TSGenerator, 
						    A_TransactionsUpdated,Events)
				end
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
	    server_loop(dict:store(Client,0,Clients),StorePid,ObjectsMgrPid,
			WaitMgrPid,TSGenerator,Transactions,Events);
	{close, Client} -> 
	    % Client logout
	    io:format("Client ~p has left the server.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(dict:erase(Client,Clients),StorePid,ObjectsMgrPid,
			WaitMgrPid,TSGenerator,Transactions,Events); 
	{request, Client} -> 
	    % A transaction is started.
	    % The user enters the run command in the client window. 
	    % This is marked by sending a request to the server.
	    TS = counter:value(TSGenerator), 
	    counter:increment(TSGenerator),  
	    ClientsUpdated = dict:store(Client,TS,Clients),
	    TransactionsUpdated =
		gb_trees:insert(TS,{Client,'going-on',sets:new(),none}
				,Transactions), 
	    % For each transaction we keep
	    % {Client,Status,SetOfVariablesItHasToCommit,
            % TransactionItIsWaitingFor} 
	    io:format("Client ~p has began transaction ~p .~n", [Client,
								 TS]), 
	    Client ! {proceed, self()},
	    server_loop(ClientsUpdated,StorePid,ObjectsMgrPid,WaitMgrPid,
			TSGenerator,TransactionsUpdated,Events); 
	{action, Client, Act} -> 
	    %%% io:format("**ENTER ACTION**"),
	    % The client sends the actions of the list (the transaction) one by one 
	    % in the order they were entered by the user.
	    Tc = dict:fetch(Client,Clients),	    
	    io:format("Received ~p from client ~p in transacion ~p.~n",
		      [Act, Client, Tc]), 
	    case gb_trees:lookup(Tc, Transactions) of
		{value, {Client,TcStatus,_,WaitingFor}} ->
		    %%% io:format("** Status: ~p.~n", [TcStatus]),
		    case TcStatus of
			'going-on' -> 
			    %perform action
                            %%% io:format("\t**ENTER going-on transaction**~n"),
			    {TransactionsUpdated, Status} =
				case Act of 
				    {read,_} -> 
					do_read(Clients, ObjectsMgrPid, WaitMgrPid,
						{action, Client, Act}, Transactions);
				    {write,_,_} -> 
					do_write(Clients, ObjectsMgrPid, {action,
									  Client,
									  Act},
						 Transactions) 
				end,
			    case Status of 
				abort ->
				    Client ! {abort, self()},
				    server_loop(Clients,StorePid, 
						ObjectsMgrPid,WaitMgrPid, 
						TSGenerator,TransactionsUpdated,
						queue:in({aborted,Tc},Events)); 
				continue ->
				    server_loop(Clients,StorePid,
						ObjectsMgrPid,WaitMgrPid,
						TSGenerator,TransactionsUpdated,
						Events)
			    end;
			'waiting' -> 
                                                %%% io:format("\t**ENTER waiting transaction**~n"),
						%enqueue action
						%(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
						%Transactions,Last_Event)
			    io:format("\t~w must wait, transaction ~p is waiting for
                              transacion ~p\~n",[Act, Tc, WaitingFor]),
			    WaitMgrPid ! {enqueue, {action, Client, Act},
					  WaitingFor}, 
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,
					TSGenerator,Transactions,Events) 
		    end;
		none ->
		    io:format("But transacion ~p aborted, so ~w is ignored.~n",
			      [Tc,Act]),
 		    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
				Transactions, Events)
		    
	    end;

        {confirm, Client} -> 
	    Tc = dict:fetch(Client,Clients),
	    case gb_trees:lookup(Tc, Transactions) of
		{value, {Client,_,_,WaitingFor}}  ->
                    %%% io:format("**ENTER CONFIRM**"),
		    % Once, all the actions are sent, the client sends a confirm message 
	            % and waits for the server reply.
		    {TransactionsUpdated, Status, Event} = 
			do_confirm(Clients, {confirm, Client}, Transactions),
		    case Status of
			continue ->
			    Client ! {committed, self()}, %a transaction is always
						%able to commit because all
						%the operations 
						%are checked for
						%consistency with those of
						%earlier transactions 
						%before being carried out
			    case Event of
				none ->
						%transacction has to wait to commit, there is no
						%event, the client needs not to wait 
				    server_loop(Clients,StorePid,
						ObjectsMgrPid,WaitMgrPid,
						TSGenerator,TransactionsUpdated,Events);
				{committed, _} ->
						%transaction committed, notify
				    server_loop(Clients,StorePid,
						ObjectsMgrPid,WaitMgrPid,
						TSGenerator,TransactionsUpdated,
						queue:in(Event,Events))
			    end;
			wait ->
			    none = Event,
			    io:format("\t~w must wait, transaction ~p is waiting for
                               transacion ~p\~n",[{confirm, Client}, Tc,
						  WaitingFor]), 
						%transaction is waiting, the confirm MESSAGE has to wait
						%too, so enqueue action
			    WaitMgrPid ! {enqueue, {confirm, Client}, WaitingFor}, 
			    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,
					TSGenerator,TransactionsUpdated,Events) 
		    end;
		none ->
		    io:format("But transacion ~p aborted, so confirm is
	                       ignore it .~n", [Tc]),
		    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,
				Transactions, Events)
		    
	    end
    after 50000 ->
	    case all_gone(Clients) of
		true -> 
		    exit(normal);    
		false ->
		    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,
				TSGenerator,Transactions,Events) 
	    end
    end.

%% - Read function
do_read(Clients, ObjectsMgrPid, WaitMgrPid, {action, Client, {read, Var}},
	Transactions) -> 
    Tc = dict:fetch(Client,Clients),
    io:format("\tValidating read rule~n"),
    ObjectsMgrPid ! {getObject,Var},
    {Val, WTS, _, Versions} = receive {object, O} -> O end,
    case Tc > WTS of
        true ->
            io:format("\t\tValid~n"),
            DSelected = maxLeqList(Tc, gb_trees:keys(Versions)),
	    TransactionsUpdated =
	    %Is DSelected committed or 
	    %the transaction has already written its own version 
	    case ((DSelected =:= WTS) or (DSelected =:= Tc)) of 	     
                true ->
		    io:format("\t\t\tPerform read operation of t.~p in
                               version ~p of var.'~p'~n", 
                              [Tc, DSelected,Var]),
		    %update read timestamp
		    ObjectsMgrPid ! {updateObject, Var, {Val, WTS, Tc,
							 Versions}},  
		    RVal = case DSelected of
			       WTS -> Val;
			       Tc -> {value, TcVal} =
					 gb_trees:lookup(DSelected,
							 Versions), 
				     TcVal
			   end,		     		    
		    io:format("\t\t\tt.~p reads ~p = ~p~n",[Tc, Var, RVal]),
		    Transactions; 
                false -> 
		    io:format("\t\t\tWait until the transaction that made
                               version ~p of '~w' commits or aborts.~n",  
                              [DSelected, Var]), 
		    %cannot perform read operation, transaction must wait
		    %enqueue operation to be carried out after Dselected finishes
		    WaitMgrPid ! {enqueue, {action, Client, {read, Var}},
				  DSelected}, 
		    %set the status of transaction to waiting 
		    %%% io:format("HEY1: ~w", [gb_trees:lookup(Tc,
                    %%% Transactions)]), 
		    {value, {Client,'going-on',WriteSets,_}} =
			gb_trees:lookup(Tc, Transactions), 
		    gb_trees:update(Tc,{Client,'waiting',WriteSets,DSelected},
				    Transactions) 
            end,				
            {TransactionsUpdated, continue}; %no change on transactions
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\tRead on ~p is too late! Abort transaction ~p
                      .~n", [Var, Tc]), 
	    %the transaction is over 
            {gb_trees:delete(Tc,Transactions), abort} 
						
    end.

%% - Write function
do_write(Clients, ObjectsMgrPid, {action, Client, {write,Var,Value}},
	 Transactions) -> 
    Tc = dict:fetch(Client,Clients),
    io:format("\tValidating write rule~n"),
    ObjectsMgrPid ! {getObject,Var},
    {Val, WTS, RTS, Versions} = receive {object, O} -> O end,
    case ((Tc >= RTS) and (Tc > WTS)) of
        true ->
            io:format("\t\t Valid~n"),
            io:format("\t\t\tPerform write operation of t.~p in '~p'
                       ~n",[Tc, Var]), 
            case gb_trees:lookup(Tc,Versions) of 
                none ->
		    ObjectsMgrPid ! {updateObject, Var, 
                                     {Val, WTS, RTS, gb_trees:enter(Tc,
								    Value,
								    Versions)}}; 
                {value, _} ->
		    ObjectsMgrPid ! {updateObject, Var, 
                                     {Val, WTS, RTS, gb_trees:update(Tc,
								     Value,
								     Versions)}} 
            end,
	    io:format("\t\t\tt.~p writes ~p on v.~p of ~p~n",[Tc, Value, Tc,
							      Var]), 
            {value, {Client,State,WriteOps,WatingFor}} = 
		gb_trees:lookup(Tc, Transactions), 
            %keep wich variables I have to commit with the transaction 
            TransactionsUpdt = gb_trees:update(Tc,{Client,State,
						   sets:add_element(Var,WriteOps),
						   WatingFor}, 
					       Transactions),  
            {TransactionsUpdt, continue}; 
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\t Write on ~p is too late! Abort transaction ~p
                       .~n", [Var, Tc]), 
	    %the transaction is over  
            {gb_trees:delete(Tc,Transactions), abort}  
    end. 

%% - Confirm function 
do_confirm(Clients, {confirm, Client}, Transactions)-> 
    Tc = dict:fetch(Client,Clients), 
    io:format("Client ~p has ended transaction ~p .~n", [Client, Tc]), 
    {value, {Client,TcStatus,WriteSets,WaitingFor}} = 
	gb_trees:lookup(Tc, Transactions), 
    case TcStatus of 
	'going-on' ->  
	    %% Perform action 
	    io:format("\tWrite data of t.~p to permanent storage.~n",[Tc]),
	    %% A transaction is always able to commit 
	    %% Nevertheless, committed versions of each object must be
	    %% created in timestamp order	     
	    TransactionsL = gb_trees:keys(Transactions), 
	    {TransactionsUpdated, Event} =  
                case TransactionsL of		 			       
                    [Tc|_] -> 
			%% Tc is the smallest transaction. No need to wait
			%% to commit
                        io:format("\tt.~p finished and can commit.~n",[Tc]), 
                        %%{gb_trees:delete(Tc,Transactions),
			%%{committed,Tc}}; 
                        {gb_trees:update(Tc,{Client,'finished',
					     WriteSets,WaitingFor},
					 Transactions),  
                         {committed, Tc}}; 
                    [X|_] -> %% Must wait to commit 
                        io:format("\tt.~p finished but cannot commit, must 
                                   wait.~n",[Tc]),  
                        io:format("\t(For example, ~p has neither committed 
                                   nor aborted)~n",[X]),  
                        {gb_trees:update(Tc,{Client,'finished',WriteSets,
					     WaitingFor},Transactions),
                         none}
                end,
	    {TransactionsUpdated, continue, Event}; 
	'waiting' -> 	    
	    %% Transaction is waiting, the confirm message has to wait too 
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
            % UpdatedQueue_Tree = gb_trees:update(Transaction,
	    % UpdatedActionQueue, Queue_Tree), 
            UpdatedQueue_Tree = gb_trees:enter(Transaction,
					       UpdatedActionQueue,
					       Queue_Tree), 
            wait_manager(ServerPid, UpdatedQueue_Tree);
        {dequeue, Transaction} ->
            case gb_trees:lookup(Transaction, Queue_Tree) of
                none ->		    
                    ServerPid ! no_action, % No action is waiting for
					   % Transaction 
                    wait_manager(ServerPid, Queue_Tree); 
                {value, Q} ->
                    case queue:out(Q) of
                        %% If an action is waiting send it to the server 
                        {{value,FirstAction}, QUpdted} ->  
                            %% Queue_Tree_Updted =
			    %% gb_trees:update(Transaction, QUpdted,
			    %% Queue_Tree), 
                            Queue_Tree_Updted = gb_trees:enter(Transaction,
							       QUpdted,
							       Queue_Tree), 
                            ServerPid ! FirstAction,
                            wait_manager(ServerPid, Queue_Tree_Updted); 
                        {empty, Q} ->
                            Queue_Tree_Updted = gb_trees:delete(Transaction,
								Queue_Tree), 
                            ServerPid ! no_action,  %no action is waiting
						%for Transaction 
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
