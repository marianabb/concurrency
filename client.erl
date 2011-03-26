%% - Client module
%% - The client module creates a parallel process by spawning handler. 
%% - The handler does the following: 
%%      1/ It makes itself into a system process in order to trap exits.
%%      2/ It creates a window and sets up the prompt and the title.
%%      4/ It waits for connection message (see disconnected).
%%

-module(client).

-import(window, [set_title/2, insert_str/2, set_prompt/2]).

-export([start/1]).

start(Host) ->
    spawn(fun() -> handler(Host) end).

%%%%%%%%%%%%%%%%%%%%%%% INACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The handler starts a window and a connector
handler(Host) ->
    process_flag(trap_exit, true),
    Window = window:start(self()),
    set_title(Window, "Connecting..."),
    set_prompt(Window, "action > "),
    start_connector(Host),
    disconnected(Window).

%% - The window is disconnected until it received a connected meassage from 
%% the connector
disconnected(Window) ->
    receive
	{connected, ServerPid} -> 
	    insert_str(Window, "Connected to the transaction server\n"),
	    set_title(Window, "Connected"),
	    TrNrGenerator = counter:start(),	    
	    connected(Window, ServerPid,TrNrGenerator);
	{'Exit', _, _} -> exit(died);
	Other -> io:format("client disconnected unexpected:~p~n",[Other]),
		 disconnected(Window)
    end.
%%%%%%%%%%%%%%%%%%%%%%% INACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% CONNECTOR %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connector(Host) ->
    S = self(),
    spawn_link(fun() -> try_to_connect(S,Host) end).

try_to_connect(Parent, Host) ->
    %% Parent is the Pid of the process (handler) that spawned this process
    {transaction_server, Host} ! {login, self(), Parent},
    receive 
	{ok, ServerPid} -> Parent ! {connected, ServerPid},
			   exit(connectorFinished);
	Any -> io:format("Unexpected message~p.~n",[Any])
    after 5000 ->
	io:format("Unable to connect to the transaction server at node~p. Restart the client application later.~n",[Host])
    end,
    exit(serverBusy).
%%%%%%%%%%%%%%%%%%%%%%% CONNECTOR %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%%%%%%%%%%%%% ACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connected(Window, ServerPid, TrNrGenerator) ->
    receive
	%% - The user has requested a transaction
	{request, Window, Transaction} ->
	    TrNr = counter:value(TrNrGenerator), 
            counter:increment(TrNrGenerator),  
	    io:format("Client requested the transaction ~p.~n",[Transaction]),
	    insert_str(Window, "Processing request...\n"),
	    process(Window, ServerPid, Transaction, TrNr, TrNrGenerator);
	{'EXIT', Window, windowDestroyed} -> end_client(ServerPid);
	{close, ServerPid} -> exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other]),
	    connected(Window,ServerPid, TrNrGenerator)
    end.

%% - Asking to process a request
process(Window, ServerPid, Transaction, TrNr, TrNrGenerator) ->
    ServerPid ! {request, self(), TrNr}, %% Send a request to server and wait for proceed message  
    receive
	{proceed, ServerPid} -> 
	    MsgNrGenerator = counter:start(),
	    send(Window, ServerPid, Transaction, MsgNrGenerator, Transaction, TrNrGenerator); %% received green light send the transaction.
	{close, ServerPid} -> exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    after 5000 ->
	    io:format("request lost?, resend request to the server"),
	    process(Window, ServerPid, Transaction, TrNr, TrNrGenerator)
    end.

%% Not needed anymore
%% flush() ->
%%     receive
%% 	{proceed, _} ->si
%% 	    flush();
%% 	{close, _} ->
%% 	    flush();
%% 	Other ->
%% 	    io:format("client active unexpected: ~p~n",[Other]),
%% 	    flush()
%%     after 0 ->
%% 	    true
%%     end.

%% - Sending the transaction and waiting for confirmation
send(Window, ServerPid, [], MsgNrGenerator, Transaction, TrNrGenerator) ->
    ServerPid ! {confirm, self()}, %% Once all the list (transaction) items sent, send confirmation
    receive	
	{reconfirm, ServerPid} ->
	    send(Window, ServerPid, [], MsgNrGenerator, Transaction, TrNrGenerator);
	{resend, MsgNr, ServerPid} ->
	    send(Window, ServerPid, lists:nthtail(MsgNr-1, Transaction), counter:set(MsgNrGenerator,MsgNr), Transaction, TrNrGenerator);
	{abort, ServerPid} -> 
	    insert_str(Window, "Aborted... type run if you want to try again!\n"),
	    connected(Window, ServerPid, TrNrGenerator);
	{committed, ServerPid} -> 
	    insert_str(Window, "Transaction succeeded!\n"),
	    connected(Window, ServerPid, TrNrGenerator);
	{'EXIT', Window, windowDestroyed} -> 
	    end_client(ServerPid);
	{close, ServerPid} -> 
	    exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    end;
send(Window, ServerPid, [H|T], MsgNrGenerator, Transaction, TrNrGenerator) -> 
    sleep(3), 
    case loose(6) of
	%% In order to handle losses, think about adding an extra field to the message sent
	false -> 
	    MsgNr = counter:value(MsgNrGenerator), 
            counter:increment(MsgNrGenerator),  
	    ServerPid ! {action, self(), H, MsgNr}; 
        true -> ok
    end,
    send(Window, ServerPid, T, MsgNrGenerator, Transaction, TrNrGenerator).
%%%%%%%%%%%%%%%%%%%%%%% Active Window %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% - Clean end
end_client(ServerPid) ->
    io:format("Client ended communication.~n",[]),
    ServerPid ! {close, self()},
    exit(died).

%% - Blocks a random amount of seconds between 1 and 5.
%% - This simulates latency in the network.
%% - Latency is an integer parameter which can be interpreted as a worst case 
%% waiting time in seconds
sleep(Latency) ->
    receive
    after 1000*random:uniform(Latency) ->
	  true
    end.

%% - Loses messages randomly
%% - This simulates the fact that the communication media is unreliable
%% - Lossyness is an integer parameter:
%%        - if Lossyness =0 the function will always return true
%%        - if Lossyness >10 the function will always return false
%%        - if lossyness =6 the function will return either values with 
%%        probability 1/2    
loose(Lossyness) -> 
    Val=random:uniform(10),
    if  
	Val >= Lossyness -> false;
	true -> true
    end.
    
	    

		  
