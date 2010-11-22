-record(connect, {
	  socket,
	  seqnum = 0,
	  mysql_version,
	  log_fun,
	  reader,
	  buf = <<>>
	 }).


