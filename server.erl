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
    %% Objects are {name, {value, RTS, WTS}}
    InitialObjects = dict:from_list([{a,{0,0,0}},{b,{0,0,0}},
                                     {c,{0,0,0}},{d,{0,0,0}}]), 
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    %% Manager for values and versions
    ObjectsMgrPid = spawn_link(fun() -> object_manager(ServerPid,InitialObjects) end),
    %% Manager for dependencies between transactions
    DepsMgrPid = spawn_link(fun() -> dependency_manager(ServerPid, gb_trees:empty()) end),
    %% Timestamp generator
    TSGenerator = counter:start(),
    server_loop(dict:new(),StorePid, ObjectsMgrPid, DepsMgrPid, TSGenerator, gb_trees:empty()).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d 
%% - Transactions is a tree that holds all current transactions.
%% each transaction is a tuple: {TS, {Client, Status, Dependencies, OldObjects}}
%% - Dependencies is a dictionary {Timestamp, Status} 
%% - OldObjects is a dictionary {Name, {OldTimestamp, OldValue, NewValue}}
server_loop(ClientList, StorePid, ObjectsMgrPid, DepsMgrPid, TSGenerator, Transactions) ->
    receive
        {login, MM, Client} -> 
            MM ! {ok, self()},
            io:format("Received request to connect from: ~p.~n", [Client]),
            StorePid ! {print, self()},
            server_loop(dict:store(Client,{0,0},ClientList),StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions);
        {close, Client} -> 
            io:format("Client~p has left the server.~n", [Client]),
            StorePid ! {print, self()},
            server_loop(dict:erase(Client, ClientList),StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions);
        {request, Client, ReqNumber} -> 
            %% A transaction is started.
            %% The user enters the run command in the client window. 
            %% This is marked by sending a request to the server.
            
            %% Check if the client has already requested the same transaction
            case dict:find(Client, ClientList) of
                error ->
                    %% Client is not registered
                    io:format("Warning: Received a request from an unregistered client~n");
                {ok, {_, ReqNumber}} ->
                    %% Received a repeated request
                    io:format("Warning: Ignoring repeated request from client ~p~n", [Client]),
                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions);
                {ok, {_, NextRequest}} ->
                    %% The request is new: Proceed with execution
                    TS = counter:value(TSGenerator), 
                    counter:increment(TSGenerator),  
                    ClientLUpdated = dict:store(Client,{TS, NextRequest},ClientList),
                    %% Insert a new transaction in the tree
                    UpdatedTransactions = gb_trees:insert(TS,{Client,'going-on',dict:new(),dict:new(),1,'not_sent'},
                                                          Transactions),
                    io:format("Client ~p has began transaction ~p.~n", [Client, TS]),
                    Client ! {proceed, self()},
                    server_loop(ClientLUpdated,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,UpdatedTransactions)
            end;
        {confirm, Client, ConfNumber} -> 
            {Tc,_} = dict:fetch(Client,ClientList),
            io:format("Received Confirm from client ~p in transaction ~p.~n", [Client, Tc]),
            
            %% The server only executes a confirm when it knows that
            %%no messages have been lost before.
            {value, {Client, Status, Deps, Old_Obj, Next, Resend}} = gb_trees:lookup(Tc, Transactions),
            io:format("ConfNumber = ~p and Next = ~p~n", [ConfNumber, Next]),
            case (ConfNumber =:= Next) of
                false ->
                    %% We have lost messages. Ask for resend only once
                    case (Resend =:= 'sent') of
                        false ->
                            %% Send a message to the client asking for resend
                            io:format("Last message has been lost from t.~p. Asking for resend~n", [Tc]),
                            Client ! {resend, Next, self()},
                            %% Change the flag to 'sent'
                            TransactionsCS = gb_trees:enter(Tc, {Client, Status, Deps, Old_Obj, 
                                                                 Next, 'sent'}, Transactions),
                            server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,TransactionsCS);
                        true -> 
                            %% The message has been sent already
                            server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions)
                    end;                    
                true ->
                    io:format("Confirm arrived in order for t.~p~n", [Tc]),
                    %% Confirm and handle outcoming status
                    {UpdatedTransactions, St} = do_confirm(Tc, ObjectsMgrPid, DepsMgrPid, StorePid, 
                                                               Transactions),
                    io:format("Transaction t.~p finished confirm with status ~p~n", [Tc, St]),
                    StorePid ! {print, self()},
                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,UpdatedTransactions)
            end;
        {action, Client, Act, ActNumber} ->
            %% The client sends the actions of the list (the transaction) one by one 
            %% in the order they were entered by the user.
            {Tc,_} = dict:fetch(Client,ClientList),
            io:format("Received ~p from client ~p in transacion ~p.~n", [Act, Client, Tc]),
            
            %% Check status of Tc. If aborted: do nothing. Otherwise continue execution.
            {value, {Client, Status, Deps, Old_Obj, Next, Resend}} = gb_trees:lookup(Tc, Transactions),
            case Status of
                'aborted' ->
                    %% Set Next to the next action anyway so that the confirm is accepted later
                    TransactionsAb = gb_trees:enter(Tc, {Client, Status, Deps, Old_Obj, Next+1, Resend}, 
                                                    Transactions),
                    %% Everything else was handled previously
                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,TransactionsAb);
                'going-on' ->
                    %% Only execute actions that come in the right order
                    io:format("ActNumber = ~p and Next = ~p~n", [ActNumber, Next]),
                    case (ActNumber =:= Next) of
                        false ->
                            %% If the action is not the one the server expects: Ignore it
                            %%and ask for resend only once.
                            case (Resend =:= 'sent') of
                                false ->
                                    %% Ask for resend of previous message
                                    io:format("Message has been lost from t.~p. Asking for resend~n", [Tc]),
                                    Client ! {resend, Next, self()},
                                    %% Change the flag to 'sent'
                                    TransactionsS = gb_trees:enter(Tc, {Client, Status, Deps, Old_Obj, 
                                                                        Next, 'sent'}, Transactions),
                                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,TransactionsS);
                                true -> 
                                    %% The message has been sent already
                                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions)
                            end;
                        true ->
                            %% Reset the Resend flag and set Next to the next action
                            TransactionsR = gb_trees:enter(Tc, {Client, Status, Deps, Old_Obj, Next+1, 'not_sent'}, 
                                                           Transactions),
                            
                            %% We can proceed with normal execution                            
                            {UpdatedTransactions, Result} = 
                                case Act of
                                    {read, Name} ->
                                        do_read(Tc, Name, TransactionsR, ObjectsMgrPid, DepsMgrPid);
                                    {write, Name, Value} ->
                                        do_write(Tc, Name, Value, TransactionsR, ObjectsMgrPid)
                                end,
                            case Result of
                                abort ->
                                    %% If the action aborted: send abort message to Client
                                    io:format("Action ~p aborted. Sending abort to client~n", [Act]),
                                    
                                    %% Handle abort here
                                    %% Restore original values and timestamps to modified variable
                                    do_restore(Tc, ObjectsMgrPid, UpdatedTransactions),
                                    %% Send the message to the client
                                    Client ! {abort, self()},
                                    %% Propagate the event: Update Dependency Dictionary
                                    TPropagated = propagate_event(Tc,'aborted',ObjectsMgrPid,DepsMgrPid,
                                                                  StorePid,UpdatedTransactions),
                                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,TPropagated);
                                
                                continue ->
                                    %% If action was successful: continue
                                    io:format("Action ~p was successful~n", [Act]),
                                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,
                                                UpdatedTransactions)
                            end
                    end
            end
    after 50000 ->
            case all_gone(ClientList) of
                true -> exit(normal);    
                false -> 
                    server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions)

            %% Confirm messages can not be lost!
                    %% Check if there are still transactions active
%%                     case gb_trees:is_empty(Transactions) of
%%                         false ->
%%                             %% There are transactions with lost confirm messages 
%%                             %% Get the timestamps and ask for resend of confirm messages
%%                             reconfirm(gb_trees:keys(Transactions), Transactions),
%%                             %% Continue execution
%%                             server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions);
%%                         true ->
%%                             %% No transactions left. Continue execution
%%                             server_loop(ClientList,StorePid,ObjectsMgrPid,DepsMgrPid,TSGenerator,Transactions)
%%                     end
            end
    end.

%% Confirm messages can not be lost
%% - Sends a message to every client in the list to resend the confirm
%% message. Assumes they have been waiting long enough so that all the
%% other messages have been executed.
%% reconfirm([], _) ->
%%     ok;
%% reconfirm([Ts|Keys], Transactions) ->
%%     %% Get the owner of the transaction
%%     {value, {Client, _, _, __, _, _}} = gb_trees:lookup(Ts, Transactions),
%%     %% Send the reconfirm message
%%     Client ! {reconfirm, self()},
%%     reconfirm(Keys, Transactions).


%% - The values are maintained here
store_loop(ServerPid, Database) -> 
    receive
        {print, ServerPid} -> 
            io:format("Database status:~n~p.~n",[Database]),
            store_loop(ServerPid,Database);
        {commit, {Var, Val}} ->
            io:format("\tNew value at the store: '~p' = ~p.~n",[Var,Val]),
            NewDB = updateDB(Database, {Var,Val}),
            store_loop(ServerPid, NewDB)
    end.
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% - Read
do_read(Ts, VName, Transactions, ObjectsMgrPid, DepsMgrPid) ->
    %%io:format("\tValidating read rule~n"),
    %% Get information for the variable VName
    ObjectsMgrPid ! {getObject, VName},
    {VValue, RTS, WTS} = receive {object, O} -> O end,
    %% Get information for the Transaction Ts
    {value, {Client, Status, Deps, Old_Obj, Next, Resend}} = gb_trees:lookup(Ts, Transactions),
    
    case WTS > Ts of
        true ->
            %% Abort Transaction. Change status to 'aborted'
            Up_Transactions = gb_trees:enter(Ts, {Client, 'aborted', Deps, Old_Obj, Next, Resend}, Transactions),
            {Up_Transactions, abort};
        false ->
            %% Proceed with read depending on WTS
            %% Look for current status of Transaction WTS and add it to dependencies also
            case gb_trees:lookup(WTS, Transactions) of
                none ->
                    %% No dependency to add
                    %%io:format("Reading ~p. No dependencies to add~n", [VName]),
                    %% Update RTS of the variable to max(RTS, Ts)
                    ObjectsMgrPid ! {updateObject, VName, {VValue, my_max(RTS, Ts), WTS}},
                    {Transactions, continue};
                {value, {_, S, _, _, _, _}} ->
                    case (WTS =:= Ts) of
                        true ->
                            %% A transaction should not depend on itself
                            %% Update RTS of the variable to max(RTS, Ts)
                            ObjectsMgrPid ! {updateObject, VName, {VValue, my_max(RTS, Ts), WTS}},
                            {Transactions, continue};
                        false ->
                            %% Use the current status of the other transaction
                            Up_Deps = dict:store(WTS, S, Deps),
                            Up_Transactions = gb_trees:enter(Ts, {Client, Status, Up_Deps, Old_Obj, Next, Resend}, 
                                                             Transactions),
                            %% Save the dependency to the Dependency Dictionary
                            DepsMgrPid ! {enqueue, WTS, Ts},
                            %% Update RTS of the variable to max(RTS, Ts)
                            ObjectsMgrPid ! {updateObject, VName, {VValue, my_max(RTS, Ts), WTS}},
                            {Up_Transactions, continue}
                    end                        
            end
    end.
      
%% - Returns the maximum value
my_max(A, B) ->
    case A > B of
        true ->
            A;
        false ->
            B
    end.


%% - Write
do_write(Ts, VName, New_Value, Transactions, ObjectsMgrPid) ->
    %%io:format("\tValidating write rule~n"),
    %% Get information for the variable VName
    ObjectsMgrPid ! {getObject, VName},
    {VValue, RTS, WTS} = receive {object, O} -> O end,
    %% Get information for the Transaction Ts
    {value, {Client, Status, Deps, Old_Obj, Next, Resend}} = gb_trees:lookup(Ts, Transactions),
    
    case RTS > Ts of
        true ->
            %% Abort Transaction. Change status to 'aborted'
            Up_Transactions = gb_trees:enter(Ts, {Client, 'aborted', Deps, Old_Obj, Next, Resend}, Transactions),
            {Up_Transactions, abort};
        false ->
            case WTS > Ts of
                true ->
                    %% Write can be skipped
                    io:format("\tWrite of ~p by t.~p was skipped~n", [VName, Ts]),
                    {Transactions, continue};
                false ->
                    io:format("\tWrite of ~p by t.~p can proceed~n", [VName, Ts]),
                    %% Add WTS, OldValue and NewValue of VName to old objects of Ts
                    Up_Old_Obj = dict:store(VName, {WTS, VValue, New_Value}, Old_Obj),
                    Up_Transactions = gb_trees:enter(Ts, {Client, Status, Deps, Up_Old_Obj, Next, Resend}, Transactions),
                    %% Update WTS of VName to Ts and Change its value
                    ObjectsMgrPid ! {updateObject, VName, {New_Value, RTS, Ts}},
                    {Up_Transactions, continue}
            end
    end.
    
%% - Commit changes to store once we know everything went ok
do_commit(Tc, StorePid, Transactions) ->
    io:format("Executing commit of transaction t.~p~n", [Tc]),
    {value, {_, _, _, Old_Obj, _, _}} = gb_trees:lookup(Tc, Transactions),
    %% Commit to store every value that Tc has changed
    do_commit_aux(Tc, dict:to_list(Old_Obj), StorePid).

do_commit_aux(_, [], _) ->
    ok;
do_commit_aux(Tc, [{Name, {_, _, NewValue}} | T], StorePid) ->
    StorePid ! {commit, {Name, NewValue}},
    do_commit_aux(Tc, T, StorePid).


%% - Restore all changes made by Tc 
do_restore(Tc, ObjectsMgrPid, Transactions) ->
    {value, {_, _, _, Old_Obj, _, _}} = gb_trees:lookup(Tc, Transactions),
    do_restore_aux(Tc, dict:to_list(Old_Obj), ObjectsMgrPid).

do_restore_aux(_, [], _) ->
    ok;
do_restore_aux(Tc, [{Name, {OldTs, OldValue, _}}|T], MgrPid) ->
    MgrPid ! {getObject, Name},
    {_, RTS, WTS} = receive {object, O} -> O end,
    case WTS =:= Tc of
        true ->
            %% Tc was the last one to modify the object. Revert changes
            io:format("t.~p aborting. Restoring value of ~p to ~p~n", [Tc, Name, OldValue]),
            MgrPid ! {updateObject, Name, {OldValue, RTS, OldTs}};
        false ->
            %% No need to revert
            ok
    end,
    do_restore_aux(Tc, T, MgrPid).

%% - Check all conditions to see if we can commit/abort Tc
do_confirm(Tc, ObjectsMgrPid, DepsMgrPid, StorePid, Transactions) ->
    %% Check status of Tc. If aborted: Delete Tc from tree. Otherwise try to commit.
    {value, {Client, Status, Deps, Old_Obj, Next, Resend}} = gb_trees:lookup(Tc, Transactions),
  
    case (Status =:= 'aborted') of
        true ->
            %% The abort message has already been sent and the Old timestamps managed
            %% Must delete Tc from Transactions
            io:format("\t\tConfirm: Transaction ~p is done by abortion. Will be deleted~n", [Tc]),
            TransactionsAbort = gb_trees:delete(Tc, Transactions),
            {TransactionsAbort, aborted};
        false ->
            case ((Status =:= 'going-on') or (Status =:= 'waiting')) of
                true ->
                    %%io:format("Confirming with status 'going-on' or 'waiting'~n"),
                    %% No action of Tc has been aborted: Attempt to commit.
                    %% First check dependencies:
                    %% While Tc still depends on other T: Tc must wait
                    case check_deps(Tc, Transactions, 'going-on') of
                        true ->
                            io:format("\t\tConfirm: t.~p must wait~n", [Tc]),
                            %% Change status of Tc to 'waiting'
                            TransactionsWait = gb_trees:enter(Tc, {Client, 'waiting', Deps, Old_Obj, Next, Resend}, 
                                                              Transactions),
                            {TransactionsWait, must_wait};
                        false ->
                            %% If any transaction Tc depended on aborted: Tc must abort
                            case check_deps(Tc, Transactions, 'aborted') of
                                true ->
                                    io:format("\t\tConfirm: t.~p must abort because of dependencies~n", [Tc]),
                                    %% Change status of Tc to 'aborted'
                                    UpdatedTransactions2 = gb_trees:enter(Tc, 
                                                                          {Client,'aborted',Deps,Old_Obj,Next,Resend}, 
                                                                          Transactions),
                                    %% Abort Tc
                                    %% Restore original values and timestamps to modified variable
                                    do_restore(Tc, ObjectsMgrPid, UpdatedTransactions2),
                                    %% Send the message to the client
                                    Client ! {abort, self()},
                                    %% Propagate the event: Update Dependency Dictionary
                                    PropagatedT = propagate_event(Tc,'aborted',ObjectsMgrPid,DepsMgrPid,
                                                    StorePid,UpdatedTransactions2),
                                    %% Its safe to delete Tc here
                                    io:format("\t\tConfirm: Transaction ~p is done by abortion. Will be deleted~n", [Tc]),
                                    UpdatedTransactions3 = gb_trees:delete_any(Tc, PropagatedT),
                                    {UpdatedTransactions3, aborted_deps};
                                false ->
                                    io:format("\t\tConfirm: t.~p can commit~n", [Tc]),
                                    %% Otherwise: COMMIT
                                    do_commit(Tc, StorePid, Transactions), %do_commit does not modify Transactions
                                    %% Send the message to the client
                                    Client ! {committed, self()},
                                    %% Propagate the event: Update Dependency Dictionary
                                    PropagatedCT = propagate_event(Tc,'committed',ObjectsMgrPid,DepsMgrPid,
                                                                   StorePid,Transactions),
                                    %% Delete Tc
                                    io:format("\t\tConfirm: Transaction ~p is done by commitment. Will be deleted~n", 
                                              [Tc]),
                                    TransactionsCommit = gb_trees:delete_any(Tc, PropagatedCT),
                                    {TransactionsCommit, committed}
                            end
                    end;
                false ->
                    %% There is no other state possible. This should not occur.
                    io:format("Warning: Transaction t~p has status other than 'going-on', 'waiting' or 'aborted'", [Tc])
            
            end
    
    end.


%% Manages a dictionary of objects
%% Every object is a tuple: {name, {value, RTS, WTS}}
object_manager(ServerPid, Objects) ->
    receive
        {print, ServerPid} -> 
            io:format("Objects status:~n~p.~n",[Objects]),
            object_manager(ServerPid,Objects);
        {getObject, VarName} ->	 
            %%io:format("Object Manager received getObject ~p~n",[VarName]),
            ServerPid ! {object, dict:fetch(VarName, Objects)},
            object_manager(ServerPid,Objects);
        {updateObject, VarName, Object} ->
            %%io:format("Object Manager received updateObject ~p: ~p~n",[VarName, Object]),
            object_manager(ServerPid, dict:store(VarName, Object, Objects))
    end.

%% - Checks the dependencies of Tc for any transactions
%% with the status 'Status'. If there at least one returns true,
%% false otherwise
check_deps(Tc, Transactions, Status) ->
    {value, {_, _, Deps, _, _, _}} = gb_trees:lookup(Tc, Transactions),
    check_deps_aux(Status, dict:to_list(Deps)).

check_deps_aux(_, []) ->
    false;
check_deps_aux(Status, [{_, DepStatus}|L]) ->
    case Status =:= DepStatus of
        true ->
            %% Found it! Return true
            true;
        false ->
            %% Continue searching
            check_deps_aux(Status, L)
    end.

%% - Propagates abort/commit events on the dependencies
propagate_event(Tc, Status, ObjectsMgrPid, DepsMgrPid, StorePid, Transactions) ->
    io:format("\tPropagate: Event ~p of t.~p will be propagated~n", [Status, Tc]),
    DepsMgrPid ! {dequeue, Tc},
    receive
        no_deps ->
            io:format("\tPropagate: All dependencies have been checked~n"),
            %% No more dependencies. Queue has been deleted.
            %% Return Transactions
            io:format("Transactions after propagation: ~p~n", [Transactions]),
            Transactions;
        {dependency, First_Dep} ->
            io:format("\tPropagate: Checking dependency ~p of t.~p~n", [First_Dep, Tc]),
            %% Update status of Tc in this dependent transaction
            case gb_trees:lookup(First_Dep, Transactions) of
                none ->
                    %% Transaction doesn't exist anymore. Ignore
                    io:format("\tPropagate: Dependency has been deleted. Ignoring~n"),
                    propagate_event(Tc, Status, ObjectsMgrPid, DepsMgrPid, StorePid, Transactions);
                {value, {C, S, Deps, Old_Obj, Next, Resend}} ->
                    %% Update dependencies of First_Dep
                    Up_Deps = dict:store(Tc, Status, Deps),
                    Up_Transactions = gb_trees:enter(First_Dep, {C, S, Up_Deps, Old_Obj, Next, Resend}, Transactions),
                    %% Attempt to commit First_Dep only if its already 'waiting'
                    case (S =:= 'waiting') of
                        true ->
                            io:format("\tPropagate: t.~p was waiting: Attempting commit~n", [First_Dep]),
                            {Up_Transactions2, S2} = do_confirm(First_Dep,ObjectsMgrPid,DepsMgrPid,
                                                                StorePid,Up_Transactions),
                            io:format("\tPropagate: Waiting transaction t.~p ended with status ~p~n", [First_Dep, S2]),
                            %% Continue propagation until the Tc queue is empty
                            propagate_event(Tc, Status, ObjectsMgrPid, DepsMgrPid, StorePid, Up_Transactions2);
                        false ->
                            %% Don't do anything unless the transaction is waiting
                            %% Continue propagation until the Tc queue is empty
                            io:format("\tPropagate: t.~p was not waiting. Continue with queue of ~p~n", 
                                      [First_Dep, Tc]),
                            propagate_event(Tc, Status, ObjectsMgrPid, DepsMgrPid, StorePid, Up_Transactions)
                    end
            
            end
    end.

%% - A manager for dependencies between transactions
dependency_manager(ServerPid, Queue_Dict) ->
    receive
        {enqueue, Transaction, Dependent} ->
            TQueue = 
                case gb_trees:lookup(Transaction, Queue_Dict) of
                    none ->
                        queue:new();
                    {value, Q} ->
                        Q
                end,
            %% Insert the Dependent transaction in the queue of Transaction
            Updated_TQueue = queue:in(Dependent, TQueue),
            UpdatedQueue_Dict = gb_trees:enter(Transaction, Updated_TQueue, Queue_Dict),
            dependency_manager(ServerPid, UpdatedQueue_Dict);
        {dequeue, Transaction} ->
            case gb_trees:lookup(Transaction, Queue_Dict) of
                none ->	
                    ServerPid ! no_deps, % No other T is waiting for Transaction
                    dependency_manager(ServerPid, Queue_Dict);
                {value, Q} ->
                    case queue:out(Q) of 
                       %% Send the first dependent T to the server
                        {{value, First_Dep}, QUpdted} -> 
                            Queue_Dict_Updted = gb_trees:enter(Transaction, QUpdted, Queue_Dict),
                            ServerPid ! {dependency, First_Dep},
                            dependency_manager(ServerPid, Queue_Dict_Updted);
                        {empty, Q} ->
                            %% If the queue is empty we can delete it
                            Queue_Dict_Updted = gb_trees:delete(Transaction, Queue_Dict),
                            ServerPid ! no_deps,  % No other T is waiting for Transaction
                            dependency_manager(ServerPid, Queue_Dict_Updted)
                    end		    
            end	    
    end.


updateDB(Database, {Var,Val}) ->
    updateDB_aux({Var,Val}, Database, []).
updateDB_aux({_,_}, [], NewDB) ->
    NewDB;
updateDB_aux({Var,Val},[{Var,_}|T],NewDB) ->
    updateDB_aux({Var,Val},T,[{Var,Val}|NewDB]);
updateDB_aux({Var,Val},[H|T],NewDB) ->
    updateDB_aux({Var,Val},T,[H|NewDB]).


%% %% - Low level function to handle lists
%% add_client(C,T) -> [C|T].

%% remove_client(_,[]) -> [];
%% remove_client(C, [C|T]) -> T;
%% remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.
