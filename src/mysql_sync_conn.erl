-module(mysql_sync_conn).

-record(state, {
	  mysql_version,
	  log_fun,
	  socket,
	  data
	 }).

-define(SECURE_CONNECTION, 32768).
-define(MYSQL_QUIT_OP, 1).
-define(MYSQL_QUERY_OP, 3).
-define(DEFAULT_STANDALONE_TIMEOUT, 5000).
-define(MYSQL_4_0, 40). %% Support for MySQL 4.0.x
-define(MYSQL_4_1, 41). %% Support for MySQL 4.1.x et 5.0.x

%% Used by transactions to get the state variable for this connection
%% when bypassing the dispatcher.
-define(STATE_VAR, mysql_connection_state).

-define(Log(LogFun,Level,Msg),
	LogFun(?MODULE, ?LINE,Level,fun()-> {Msg,[]} end)).
-define(Log2(LogFun,Level,Msg,Params),
	LogFun(?MODULE, ?LINE,Level,fun()-> {Msg,Params} end)).
-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).


%%%	Conn :== mysql_sync_recv:#state

init(Host, Port, User, Password, Database, LogFun, Encoding) ->
	case mysql_sync_recv:start_link(Host, Port, LogFun) of
	{ok, Conn} ->
		case mysql_init(Conn, User, Password, LogFun) of
		{ok, Version} ->
			Db = iolist_to_binary(Database),
			case do_query(Sock, LogFun,
				  <<"use ", Db/binary>>,
				  Version) of
			{error, MySQLRes} ->
				?Log2(LogFun, error,
				 "mysql_conn: Failed changing to database "
				 "~p : ~p",
				 [Database,
				  mysql:get_result_reason(MySQLRes)]),
				{error, failed_changing_database};

			%% ResultType: data | updated
			{_ResultType, _MySQLRes} ->
				case Encoding of
				undefined -> undefined;
				_ ->
					EncodingBinary = list_to_binary(atom_to_list(Encoding)),
					do_query(Sock, RecvPid, LogFun,
						 <<"set names '", EncodingBinary/binary, "'">>,
						 Version)
				end,
				State = #state{mysql_version=Version,
					   socket   = Sock,
					   log_fun  = LogFun,
					   data	 = <<>>
					  },
				State
			end;
		{error, Reason} ->
			{error, Reason}
		end;
	E ->
		?Log2(LogFun, error,
		 "failed connecting to ~p:~p : ~p",
		 [Host, Port, E]),
		{error, connect_failed}
	end.

%% return:	{ok,Version, Conn}
mysql_init(Conn, User, Password, LogFun) ->
	case do_recv(Conn) of
	{Conn2, {Packet, InitSeqNum}} ->
		case greeting(Packet, LogFun) of
			{error, Reason} ->
				{error, Reason} ;
		{ok, Version, Salt1, Salt2, Caps} ->
			AuthRes =
			case Caps band ?SECURE_CONNECTION of
				?SECURE_CONNECTION ->
					mysql_sync_auth:do_new_auth(
					  Conn, User, Password, Salt1, Salt2, LogFun);
				_ ->
					mysql_sync_auth:do_old_auth(
					  Conn, User, Password, Salt1, LogFun)
			end,
			case AuthRes of
				{ok, <<0:8, _Rest/binary>>, _RecvNum} ->
					{ok,Version, Conn};
				{ok, <<255:8, Code:16/little, Message/binary>>, _RecvNum} ->
					?Log2(LogFun, error, "init error ~p: ~p",
					 [Code, binary_to_list(Message)]),
					{error, binary_to_list(Message)};
				{ok, RecvPacket, _RecvNum} ->
					?Log2(LogFun, error,
					  "init unknown error ~p",
					  [binary_to_list(RecvPacket)]),
					{error, binary_to_list(RecvPacket)};
				{error, Reason} ->
					?Log2(LogFun, error,
					  "init failed receiving data : ~p", [Reason]),
					{error, Reason}
					end
			end;
	{error, Reason} ->
		{error, Reason}
	end.

do_recv(Conn) ->
	mysql_sync_recv:get_packet(Conn).


