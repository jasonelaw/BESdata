dbConnect <- function(dsn){
  RODBC::odbcConnect(dsn)
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
