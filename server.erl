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
    WaitMgrPid = spawn_link(fun() -> wait_manager(ServerPid, gb_trees:empty(), gb_trees:empty()) end),
    TSGenerator = counter:start(),
    server_loop(dict:new(),StorePid, ObjectsMgrPid, WaitMgrPid, TSGenerator, gb_trees:empty(), 0).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d 
%% - Last_Event: Indicates if a transaction was committed/aborted during last execution.
%%   Contains the timestamp of the transaction. Zero if no transaction was aborted/committed.
server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event) ->
    
    % Depending on the previous status received in Last_Event we must check the Waiting tree
    case Last_Event > 0 of
        true -> 
            % Request the queue corresponding to Last_Event to the wait manager
            WaitMgrPid ! {getFirst, Last_Event},
            receive 
                % The Transaction has no one waiting for it
                {no_waiting} ->
                    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,0);
                % There are actions waiting for Last_Event in the queue
                {first, {Client_Q, Tc_Q, Act_Q}} -> 
                    % Delete (Tc, Last_Event) from Waiting_Ts
                    WaitMgrPid ! {deleteWait, {Tc_Q, Last_Event}},
                    % Apply the action and continue with the queue
                    {TransactionsUpdated_Q, Status_Q} =
                        case Act_Q of 
                            {read,Var_Q} -> 
                                do_read(ObjectsMgrPid, WaitMgrPid, Tc_Q, Var_Q, Transactions);
                            {write,Var_Q,Value_Q} -> 
                                do_write(ObjectsMgrPid, Tc_Q, Var_Q, Value_Q, Transactions)
                        end,
                    % If the action failed, send abort to the client
                    case Status_Q of
                        abort ->
                            Client_Q ! {abort, self()}
                    end,
                    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated_Q,Last_Event);
                % The queue is now empty, the element has been deleted by the Wait Manager.
                {empty_queue} ->
                    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,0)
            end
    end,
    
    receive
	{login, MM, Client} -> 
	    % Client login
	    MM ! {ok, self()},
	    io:format("New client has joined the server: ~p.~n", [Client]),
	    StorePid ! {print, self()},
	    ObjectsMgrPid ! {print, self()},
	    server_loop(dict:store(Client,0,Clients),StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event);
	{close, Client} -> 
	    % Client logout
	    io:format("Client ~p has left the server.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(dict:erase(Client,Clients),StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event);
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
	    server_loop(ClientsUpdated,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Last_Event);
	{action, Client, Act} ->
	    % The client sends the actions of the list (the transaction) one by one 
	    % in the order they were entered by the user.
	    Tc = dict:fetch(Client,Clients),
	    io:format("Received ~p from client ~p in transacion ~p.~n", [Act, Client, Tc]),

        %Before applying action we must check if Tc is currently waiting
        WaitMgrPid ! {checkT, {Tc, Act}},
        receive
            {must_wait} ->
                % continue loop without changes to Transactions
                server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event);    
            {proceed} ->                            
                {TransactionsUpdated, Status} =
                    case Act of 
                        {read,Var} -> 
                            do_read(ObjectsMgrPid, WaitMgrPid, Tc, Var, Transactions);
                        {write,Var,Value} -> 
                            do_write(ObjectsMgrPid, Tc, Var, Value, Transactions)
                    end,
                % If the action failed, send abort to the client
                case Status of
                    abort ->
                        Client ! {abort, self()}
                end,
                server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,TransactionsUpdated,Last_Event)
        end;
	{confirm, Client} -> 
	    % Once, all the actions are sent, the client sends a confirm message 
	    % and waits for the server reply.
        Tc = dict:fetch(Client,Clients),
	    io:format("Client ~p has ended transaction ~p .~n", [Client, dict:fetch(Client,Clients)]),

        % TO-DO: Before answering confirm we must check if Tc is currently waiting
%%         WaitMgrPid ! {checkT, {Tc, Act}},
%%         receive
%%             {must_wait} ->
%%                 % continue loop without changes to Transactions
%%                 server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event);    
%%             {proceed} ->                            
%%                 % do_confirm() TO-DO
%%                 server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event)%TO-DO TransUpdated
%%         end,

	    Client ! {abort, self()}, %TO-DO erase
	    server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event) %TO-DO erase
    after 50000 ->
            case all_gone(Clients) of
                true -> exit(normal);    
                false -> server_loop(Clients,StorePid,ObjectsMgrPid,WaitMgrPid,TSGenerator,Transactions,Last_Event)
            end
    end.

%% - Read function
do_read(ObjectsMgrPid, WaitMgrPid, Tc, Var, Transactions) ->
    io:format("\tValidating read rule~n"),
    ObjectsMgrPid ! {getObject,Var},
    {Val, WTS, _, Versions} = receive {object, O} -> O end,
    case Tc > WTS of
        true ->
            io:format("\t\tValid~n"),
            DSelected = maxLeqList(Tc, gb_trees:keys(Versions)),
            case DSelected =:= WTS of
                true ->
					io:format("\t\t\tPerform read operation of ~p in version ~p of ~p~n",
                              [Tc, DSelected,Var]),					
                    %update read timestamp
					ObjectsMgrPid ! {updateObject, Var, {Val, WTS, Tc, Versions}}, 
					io:format("\t\t\tClient ~p reads ~p = ~p~n",[Tc, Var, Val]);				    
                false ->
					io:format("\t\t\tWait until the transaction that made version ~p of '~w' commits or aborts.~n", 
                              [DSelected, Var]),
                    % Client blocks, but server should not block!
                    % Wait manager must create a new queue for Dselected
                    WaitMgrPid ! {insertT, {Tc, {read, Val}, DSelected}},
                    receive %TO-DO Perhaps eliminate the receive and just print
                        {inserted} -> 
                            io:format("\t\t\t Transaction ~p was inserted in Queue_Tree waiting for ~p.~n",
                                      [Tc, DSelected])
                    end
            end,				
            {Transactions, continue}; %no change on transactions
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\tRead on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
            {gb_trees:delete(Tc,Transactions), abort} %the transaction is over
    end.

%% - Write function
do_write(ObjectsMgrPid, Tc, Var, Value, Transactions) ->
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
            {value, {Client,State,WriteOps}} = gb_trees:lookup(Tc, Transactions),
            %keep wich variables i have to commit with the transaction
            TransactionsUpdt = gb_trees:update(Tc, {Client,State,sets:add_element(Var,WriteOps)}, Transactions), 
            {TransactionsUpdt, continue};
        false ->
            io:format("\t\tNot valid~n"),
            io:format("\t\t\t Write on ~p is too late! Abort transaction ~p .~n", [Var, Tc]),
            {gb_trees:delete(Tc,Transactions), abort} %the transaction is over
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


%% - Manages waiting transactions with a tree of queues and a tree
% Queue_Tree: Every element is (T_timestamp, {actions queue}) where the actions are
%waiting for the transaccion T.
% Waiting_Ts: Every element is (Tc, Tw) where Tc is waiting for Tw.
wait_manager(ServerPid, Queue_Tree, Waiting_Ts) ->
    receive
        {print, ServerPid} ->
            io:format("Waiting actions:~n~p.~n", [Queue_Tree]); %TO-DO verify if this prints correctly

        % Checks if a Transaction can proceed or must wait for another to commit/abort.
        % If it must wait, it enqueues the trasaction.
        {checkT, {Client, Tc, Act}} ->
            % Look for Tc in Waiting_Ts
            case gb_trees:lookup(Tc, Waiting_Ts) of
                none ->
					ServerPid ! {proceed},
                    wait_manager(ServerPid, Queue_Tree, Waiting_Ts);
                {value, Tw} ->
                    % Enqueue the action in Tw's queue
                    Queue = gb_trees:get(Tw, Queue_Tree),
                    Updated_Q_Tree = gb_trees:enter(Tw, queue:in({Client, Tc, Act}, Queue)),
                    ServerPid ! {must_wait},
                    wait_manager(ServerPid, Updated_Q_Tree, Waiting_Ts)
            end;
        % Inserts a transaction on the corresponding queue in Queue_Tree
        {insertT, {Client, Tc, Act, Tw}} ->
            % Insert Tc in Waiting_Ts waiting for Tw
            % Note: It should not be possible for Tc to be in the tree
            Updated_Waiting_Ts = gb_trees:insert(Tc, Tw, Waiting_Ts), %TO-DO perhaps try-catch just in case
            
            % Look for Tw in Queue_Tree
            case gb_trees:lookup(Tw, Queue_Tree) of
                none ->
                    New_Q = queue:new();
                {value, Tw_Queue} ->
                    New_Q = Tw_Queue
            end,
            Updated_Q_Tree = gb_trees:insert(Tw, queue:in({Client, Tc, Act}, New_Q)),
            ServerPid ! {inserted},
            wait_manager(ServerPid, Updated_Q_Tree, Updated_Waiting_Ts);
        % Returns the first element from the queue of Tw in Queue_Tree or 'empty_queue' if the queue is empty
        % Note: If the queue is empty it deletes the element from the tree
        {getFirst, Tw} ->
            case gb_trees:lookup(Tw, Queue_Tree) of
                {value, Q} ->
                    case queue:out(Q) of
                        {{value,First}, Q2} -> % First contains {Client, Tc, Act}
                            % Update the queue
                            Updated_Q_Tree = gb_trees:enter(Tw, Q2),
                            ServerPid ! {first, First},
                            wait_manager(ServerPid, Updated_Q_Tree, Waiting_Ts);
                        % If the queue is empty, remove the element from the tree
                        {empty, _} ->
                            Updated_Q_Tree = gb_trees:delete(Tw, Queue_Tree),
                            ServerPid ! {empty_queue},
                            wait_manager(ServerPid, Updated_Q_Tree, Waiting_Ts)
                    end;
                none ->
                    ServerPid ! {no_waiting}
            end,
            wait_manager(ServerPid, Queue_Tree, Waiting_Ts);
        % Delete (Tc, Tw) from Waiting_Ts
        {deleteWait, {Tc, Tw}} ->
            case gb_tree:lookup(Tc, Waiting_Ts) of
                % Delete ONLY if value is the same as Tw! TO-DO: Check if this works as expected
                {value, Tw} ->
                    Updated_Waiting_Ts = gb_tree:delete(Tc, Waiting_Ts),
                    wait_manager(ServerPid, Queue_Tree, Updated_Waiting_Ts)
                    %TO-DO Perhaps send a 'success' message?
            end,
            wait_manager(ServerPid, Queue_Tree, Waiting_Ts)
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
