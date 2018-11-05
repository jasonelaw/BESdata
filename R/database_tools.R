dbConnect <- function(database = c('DSN', 'JANUS', 'NEPTUNE', 'WATERSHED'), dsn = NULL){
  database = match.arg(database)
  if(identical(database, 'DSN') & is.null(dsn)){
    stop('Must provide a dsn if no database name provided.')
  }
  conn.string <- switch(database,
                        DSN       = sprintf('{DSN=%s}', dsn),
                        JANUS     = 'driver={SQL Server};server=BESREPORTS;database=JANUS;trusted_connection=true',
                        NEPTUNE   = 'driver={SQL Server};server=BESDBPROD1;database=NEPTUNE;trusted_connection=true',
                        WATERSHED = 'driver={SQL Server};server=BESREPORTS;database=WATERSHED;trusted_connection=true')
  RODBC:::odbcDriverConnect(conn.string)
}

dbDisconnect <- function(conn){
  RODBC::odbcClose(conn)
}

dbGetQuery <- function(conn, statement){
  ret <- RODBC::sqlQuery(conn, statement, as.is = TRUE, errors = FALSE)
  if (identical(ret, -1L)){
    stop("The server returned an error:\n", RODBC::odbcGetErrMsg(conn))
  } else if(identical(ret, -2L) || (is.data.frame(ret) && nrow(ret) < 1)){
    warning("There was no data in the query.")
    return(ret)
  } else {
    return(ret)
  }
}

dbReadTable <- function(conn, name){
  ret <- RODBC::sqlFetch(conn, name, rownames = FALSE, as.is = TRUE, errors = FALSE)
  if (identical(ret, 1L)){
    stop("The server returned an error:\n", RODBC::odbcGetErrMsg(conn))
  } else if(identical(ret, 2L) || nrow(ret) < 1){
    warning("There was no data in the query.")
  } else {
    return(ret)
  }
}
