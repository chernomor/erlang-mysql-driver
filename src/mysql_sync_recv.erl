-module(mysql_sync_recv).

-export([start_link/3, read/1
	]).

-record(connect, {
	  socket,
	  log_fun,
	  reader,
	  buf = <<>>,
	  seqnum = 0
	 }).

%%%	Packet :== {PacketData, Num} 
%%%


start_link(Host, Port, LogFun)
	when is_list(Host), is_integer(Port); is_tuple(Host), is_integer(Port) ->

   init(Host, Port, LogFun)
.

%% return: {ok, #connect}
init(Host, Port, LogFun) ->
	case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false} ]) of
	{ok, Sock} ->
		{ok, read(#connect{socket  = Sock, log_fun = LogFun })};
	E ->
		LogFun(?MODULE, ?LINE, error,
		   fun() ->
			   {"mysql_recv: Failed connecting to ~p:~p : ~p",
				[Host, Port, E]}
		   end),
		Msg = lists:flatten(io_lib:format("connect failed : ~p", [E])),
		{error, Msg}
	end
.


%% return: #connect | {error, Reason}
read(State) ->
	ReadIter = fun() ->
			Sock = State#connect.socket,
			case gen_tcp:recv(Sock, 0, 9000) of
				{ok, InData} ->
					read(State#connect{ buf = <<State#connect.buf/bytes, InData/bytes>>} );

				{error, Reason} ->
					LogFun = State#connect.log_fun,
					LogFun(?MODULE, ?LINE, error,
					   fun() ->
						   { "mysql_recv: Socket ~p closed : ~p", [Sock, Reason] }
					   end),
					{error, Reason}
			end
		end,
	State#connect{ reader = ReadIter }
.

%% return: {#connect, Packet} | {error, Reason}
get_packet(State) ->
	#connect { buf=Data, reader = ReadIter, seqnum = SeqNum } = State,
	case extract_packet(Data, SeqNum, State#connect.log_fun ) of
		{Packet, Rest} ->
			{ State#connect{ buf = Rest, seqnum = SeqNum+1 }, Packet };
		{not_enough_data} ->
			case ReadIter() of
				{error, Reason} ->
					{error, Reason};
				{State} ->
					get_packet( State )
			end;
		E ->
			{error, E}
	end
.

%% return: {{PacketData, Num}, Rest} | {not_enough_data} | {unexpected_seqnum}
extract_packet(Data, ExpectedSeqNum, LogFun) ->
	case Data of
	<<Length:24/little, ExpectedSeqNum:8, D/binary>> ->
		if
		Length =< size(D) ->
			{PacketData, Rest} = split_binary(D, Length),
			{{PacketData, Num}, Rest};
		true ->
			{not_enough_data}
		end;
	<<Length:24/little, Num:8, D/binary>> ->
		LogFun(?MODULE, ?LINE, error,
		   fun() ->
			   { "mysql_recv: unexpected seqnum ~w received, expected: ~w",
				[Num, ExpectedSeqNum] }
		   end),
		{unexpected_seqnum};
	_ ->
		{not_enough_data}
	end
.


